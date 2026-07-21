package runtime

import (
	"bytes"
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// providerConfigChangedChannel is the LISTEN/NOTIFY channel the
// providerconfigs_changed trigger fires (gated, via notify_providerconfig_changed
// → notify_gated) whenever a DEFAULT config row (is_default=TRUE) is created,
// edited, or deleted. The ProviderConfigCache LISTENs on it to live-push the new
// document to the affected providers, instead of waiting out the failsafe.
const providerConfigChangedChannel = "providerconfig_changed"

// configReloadInterval is the failsafe cadence: the ProviderConfigCache re-reads every
// tracked (kind, kindVersion)'s default config into `current` this often regardless of
// LISTEN/NOTIFY, so a dropped notification still converges within this bound.
// Workers read `current` live, so the refresh is the delivery.
const configReloadInterval = 60 * time.Second

// configRepo is the narrow store surface the ProviderConfigCache needs.
type configRepo interface {
	// GetDefaultProviderConfig reads ONE (kind, kindVersion)'s default document + bundle.
	// Defaults are per-(kind, kindVersion): a vpc/v2 worker gets vpc/v2's default, a v1
	// worker vpc/v1's — so the kindVersion is part of the key, not a kind-wide lookup.
	GetDefaultProviderConfig(ctx context.Context, kind model.Kind, kindVersion int) (store.DefaultProviderConfig, bool, error)
	// ConfiguredKindVersions lists every (kind, kindVersion) that has an applied manifest (a
	// kind_config row), so a DYNAMIC plane (one built with no explicit kind list)
	// tracks exactly the cluster's live (kind, kindVersion) set — picking up a CRD applied
	// after boot (at any kindVersion) with no restart. converge carries NO provider code,
	// so its broker learns which (kind, kindVersion) pairs to serve worker configs for from
	// here, not a compiled-in list.
	ConfiguredKindVersions(ctx context.Context) ([]model.KindVersion, error)
}

// ProviderConfigCache is the runtime half of RUNTIME-EDITABLE CONFIG. It owns the ONE
// authoritative copy of each (kind, kindVersion)'s DEFAULT config — the providerconfigs
// row with kind=K AND kindVersion=M AND is_default — loaded once at boot (so a worker's
// boot GetProviderConfig sees it) and kept current thereafter: LISTENing on
// providerconfig_changed (plus a failsafe re-read) it refreshes its in-memory map
// whenever an operator edits a default. No restart, no env var per cloud — the
// Crossplane ProviderConfig model, now VERSIONED per web-API kindVersion.
//
// Delivery is PULL, not push: the broker serves cp.Config(kind, kindVersion) /
// cp.Bundle(kind, kindVersion) over GetProviderConfig, and re-reads through a single
// RLock, so a dumb worker always observes the plane's current per-kindVersion document.
// The plane's only job is to keep `current` fresh and fire the per-(kind, kindVersion)
// change callbacks the broker wires to its live worker pushes.
//
// Bootstrap vs non-bootstrap is a per-PROVIDER distinction, not a plane one:
// the document carries both tiers (same structure), and Setup decides which
// fields are boot-critical (a missing broker URL can fail Setup → the pod
// won't start) vs work-time-only (a missing topic boots fine and only a task
// fails). The plane just owns the document.
//
// PER-KIND-VERSION keying: `current` is keyed by (kind, kindVersion) because a default is now
// per-(kind, kindVersion). A single kind that publishes v1 AND v2 holds two entries, so
// a v2 worker's GetProviderConfig(vpc, 2) never sees v1's document and vice-versa.
type ProviderConfigCache struct {
	repo      configRepo
	refresher *NotifyRefresher // providerconfig_changed LISTEN + failsafe reload
	// kinds is the FIXED set to track when non-empty (a kind-scoped pod / test);
	// each is tracked at kindVersion 1 (v1) — a static plane is a single-version test
	// seam, and the runtime-critical per-kindVersion set is discovered dynamically (see
	// dynamic below). Empty ⇒ DYNAMIC.
	kinds   []model.Kind
	dynamic bool

	// providerConfigSnapshot is the READ side (the served current-config map + the
	// Config/Bundle getters). Embedded so callers use cp.Config(...)/cp.Bundle(...)
	// directly; the refresh loop below swaps its contents. ProviderConfigCache owns the
	// refresh ORCHESTRATION; the snapshot owns the serving.
	providerConfigSnapshot

	// OnDefaultChange, if set, is invoked (once) after a reload in which at least
	// one default config CHANGED (spec OR data). Called from the plane's loop
	// goroutine, so it must not block.
	OnDefaultChange func()

	// OnKindConfigChange, if set, is invoked ONCE PER (kind, kindVersion) whose default
	// providerconfig changed in a reload — on EITHER axis (spec or bundle) — with the
	// (kind, kindVersion) and its NEW spec + data (nil = that half absent/deleted). A
	// providerconfig is a MONOLITH of {spec, data}, so it changes and pushes as one: the
	// broker wires this to push the whole thing down every connected worker serving that
	// exact (kind, kindVersion) (the PUSH half of worker config delivery), so an
	// operator's edit reaches workers immediately rather than on their poll — and a v2
	// edit never lands on a v1 worker. Called from the plane's loop goroutine, so it must
	// not block.
	OnKindConfigChange func(kind model.Kind, kindVersion int, spec, data []byte)
}

// NewProviderConfigCache builds the plane and SYNCHRONOUSLY loads each tracked
// (kind, kindVersion)'s boot config, so a reader observes it immediately. Call Start
// afterwards for live refresh. A non-empty kinds list tracks exactly those at
// kindVersion 1 (a kind-scoped pod / test); an EMPTY/nil list is DYNAMIC — every reload
// discovers the live (kind, kindVersion) set from the DB (the broker's mode: it carries
// no providers, so it serves worker configs for whatever pairs have an applied
// manifest, including a post-boot v2). A per-(kind, kindVersion) load error at boot is
// logged and leaves that pair with a nil document.
func NewProviderConfigCache(ctx context.Context, pool *pgxpool.Pool, kinds []model.Kind) *ProviderConfigCache {
	cp := &ProviderConfigCache{
		repo:                   store.New(pool),
		kinds:                  kinds,
		dynamic:                len(kinds) == 0,
		providerConfigSnapshot: providerConfigSnapshot{current: make(map[model.KindVersion]store.DefaultProviderConfig, len(kinds))},
	}
	cp.reload(ctx) // populate boot snapshots
	// providerconfig_changed LISTEN + 60s failsafe reload (Start launches it).
	// reloadAndSignal is the AUTHORITATIVE refresher: it re-reads every tracked
	// (kind, kindVersion)'s default config into `current` (which the broker's
	// GetProviderConfig serves live) and fires the once-per-edit side effects. The
	// NOTIFY makes an operator's edit prompt; the interval reconciles a lost one.
	cp.refresher = NewNotifyRefresher(NewPgxListener(pool), "configplane providerconfig_changed",
		providerConfigChangedChannel, configReloadInterval, cp.reloadAndSignal)
	return cp
}

// Start launches the providerconfig_changed listener + failsafe reload loop (the
// shared NotifyRefresher): the listener handles prompt edits, the interval
// catches a missed NOTIFY, and an immediate reload observes a default created
// between the boot snapshot (NewProviderConfigCache) and Start.
func (cp *ProviderConfigCache) Start(ctx context.Context) { cp.refresher.Start(ctx) }

// Stop cancels the listener + loop and joins them, releasing the held LISTEN
// connection before the pool closes.
func (cp *ProviderConfigCache) Stop() { cp.refresher.Stop() }

// trackedPairs returns the (kind, kindVersion) set this reload must load: the DYNAMICALLY
// discovered live set (dynamic mode), or the fixed kind list projected to v1 (static
// mode). A discovery error in dynamic mode returns ok=false so the caller keeps the
// prior snapshot (a DB blip never narrows what we serve).
func (cp *ProviderConfigCache) trackedPairs(ctx context.Context) (pairs []model.KindVersion, ok bool) {
	if cp.dynamic {
		discovered, err := cp.repo.ConfiguredKindVersions(ctx)
		if err != nil {
			slog.Warn("configplane: discover kind kind versions", "err", err)
			return nil, false
		}
		return discovered, true
	}
	// Static mode: a fixed kind list is a single-version test seam, so each kind is
	// tracked at v1. (The per-kindVersion fan-out that matters in prod runs DYNAMIC.)
	pairs = make([]model.KindVersion, len(cp.kinds))
	for i, k := range cp.kinds {
		pairs[i] = model.KindVersion{Kind: k, Version: 1}
	}
	return pairs, true
}

// reload re-reads every tracked (kind, kindVersion)'s default config into `current`,
// returning whether any pair's bytes CHANGED. The broker serves `current` live
// through GetProviderConfig, so a refresh here IS the delivery. On a per-pair load
// error it keeps the prior snapshot (a transient DB blip never wipes a worker's
// config). The byte compare is tiny and the docs rare, so this stays off any hot
// path; it exists only to drive reloadAndSignal's once-per-edit side effects.
func (cp *ProviderConfigCache) reload(ctx context.Context) (anyChanged bool) {
	pairs, ok := cp.trackedPairs(ctx)
	if !ok {
		return false
	}
	for _, km := range pairs {
		next, _, err := cp.repo.GetDefaultProviderConfig(ctx, km.Kind, km.Version)
		if err != nil {
			slog.Warn("configplane: load config", "kind", km.Kind, "kind_version", km.Version, "err", err)
			continue
		}

		cp.mu.Lock()
		prev := cp.current[km]
		specChanged := !bytes.Equal(prev.Spec, next.Spec)
		bundleChanged := !bytes.Equal(prev.Data, next.Data)
		if specChanged || bundleChanged {
			cp.current[km] = next
		}
		cp.mu.Unlock()

		// A providerconfig is a MONOLITH of {spec, data}; when EITHER axis changed, push
		// the WHOLE current thing once (the broker ships it as one ProviderConfigUpdate).
		// Re-shipping an unchanged (possibly large) bundle on a spec-only edit is the
		// accepted cost of a single, tear-free push — pull already returns both together.
		if specChanged || bundleChanged {
			anyChanged = true
			slog.Info("configplane: kind providerconfig changed", "kind", km.Kind, "kind_version", km.Version,
				"spec_present", next.Spec != nil, "bundle_bytes", len(next.Data))
			if cp.OnKindConfigChange != nil {
				cp.OnKindConfigChange(km.Kind, km.Version, next.Spec, next.Data)
			}
		}
	}

	// DYNAMIC mode: a (kind, kindVersion) whose CRD/providerconfig was DELETED drops out of
	// the re-discovered set, so the loop above never revisits it — without this,
	// cp.current would keep serving its STALE config/bundle forever (GetProviderConfig
	// reads cp.current), and no deletion would ever reach connected workers. Reconcile
	// removals: for each tracked pair no longer discovered, fire the deletion push
	// (empty doc/bundle → workers clear it) and drop it from cp.current. Skipped in
	// static mode (cp.kinds is fixed, so nothing is ever removed).
	if cp.dynamic {
		live := make(map[model.KindVersion]struct{}, len(pairs))
		for _, km := range pairs {
			live[km] = struct{}{}
		}
		cp.mu.Lock()
		var removed []model.KindVersion
		for km := range cp.current {
			if _, ok := live[km]; !ok {
				removed = append(removed, km)
			}
		}
		for _, km := range removed {
			delete(cp.current, km)
		}
		cp.mu.Unlock()
		for _, km := range removed {
			anyChanged = true
			slog.Info("configplane: kind default removed", "kind", km.Kind, "kind_version", km.Version)
			if cp.OnKindConfigChange != nil {
				cp.OnKindConfigChange(km.Kind, km.Version, nil, nil) // both empty → worker clears the pair
			}
		}
	}
	return anyChanged
}

// reloadAndSignal is reload plus the once-per-edit retrier nudge: when at least
// one default changed, it fires OnDefaultChange so a provider still pending on a
// missing/invalid default re-attempts Setup now (instead of waiting out the 30s
// retry tick). Only the live loop calls this; the boot reload (NewProviderConfigCache)
// calls reload directly, before the retrier — and thus OnDefaultChange — exists.
func (cp *ProviderConfigCache) reloadAndSignal(ctx context.Context) {
	if cp.reload(ctx) && cp.OnDefaultChange != nil {
		cp.OnDefaultChange()
	}
}
