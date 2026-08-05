// Package classicbom implements the compose + rollup reactions for kind=classicbom.
//
// A ClassicBOM root composes the per-FD/per-team resource set in one
// shot — accounts, TGWs, VPCs, routes — plus the dep edges between
// them. The compose reaction expands the BOM; the rollup reaction is
// gated until every descendant has synced (synced_gen >= generation).
// Until then the reconcile finishes with advance_synced_gen=false and the
// reactive cascade re-fires it as descendants catch up. Once they have,
// the rollup reaction reads the subtree and writes a denormalized
// ClassicBOMStatus the API can serve to UI clients without recursing
// the tree.
//
// Every composed child name is prefixed by the deployment_instance name
// (diName) because resource names are GLOBALLY unique (one UNIQUE (kind,
// name) per instance). Two BOMs sharing an FD/team thus emit distinct
// names and never collide.
//
// Composed resource set (diName = spec.deployment_instance.name):
//
//   - Per FD with TGW (AWSTransitGateway.Enabled=true AND not
//     AccountsOnly):
//     1 account ("<diName>/<fdName>-tgw", spec.TeamName="tgw") to host the TGW
//     1 TGW resource ("<diName>/<fdName>"), depends on → TGW host account
//
//   - Per team in any FD:
//     1 team account ("<diName>/<fdName>/<teamName>"), spec.TeamName=teamName
//
//   - Per team in an FD that has a TGW (i.e. not AccountsOnly):
//     1 VPC ("<diName>/<fdName>/<teamName>"), depends on → team account
//
//   - Per team in an FD that has TGW AND AWSTransitGateway.Enabled:
//     1 route ("<diName>/<fdName>/<teamName>"), depends on → team VPC + FD TGW
//
// EXAMPLE COUPLING: classicbom imports the sibling providers/account and
// providers/networking packages to reference their Kind/Spec/Status types as
// the children it composes. This is example-only domain coupling between two
// reference providers, NOT a constraint the SDK imposes — sdk-go/converge has
// no notion of one kind depending on another's Go package. A third party
// composing their own children would import their own kinds here instead; if
// you need to swap account/networking out, define the child contract you want
// and compose against that rather than these example packages.
package classicbom

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/kelseyhightower/envconfig"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/sdk-go/converge"
)

// The three reaction names classicbom's manifest (CRD) declares, mirrored from
// testfixtures/classicbom.kind.json — the core selects one per StageTask and ships
// it in req.Reaction, and Work switches on these to route to the matching body. Named
// consts (not bare literals at the switch) so a typo can't silently miss a case and a
// rename stays in one place; kept in sync with the manifest's ReactionDecl.Name.
const (
	// reactionCompose (trigger=specChange) expands the BOM into children + dep edges.
	reactionCompose = "compose"
	// reactionRollup (trigger=children settled) writes the denormalized per-team status.
	reactionRollup = "rollup"
	// reactionTeardown (trigger=deleteRequested) runs the root's finalizer LAST, after
	// its whole child subtree has torn down.
	reactionTeardown = "teardown"
)

// Provider is the worker-side logic for kind=classicbom: it implements
// converge.Provider (the ONE SDK contract — Kind/Work/OnConfig/Ready). It composes +
// rolls up and, on delete, runs the ROOT's simulated teardown (finalizer) last — after
// its subtree. PURE — no config and no downstream to dial, so OnConfig is a no-op and
// Ready is always true. deleteDelay paces the simulated teardown (0 = instant, for
// tests); it defaults to zero on Provider{}, and Runtime sets it from the env-derived
// knob. Operational limits (max_inflight, task_deadline) live in the applied manifest
// (CRD) and seed kind_config via the kind_manifest trigger; they are operator-editable
// via the kinds API.
type Provider struct {
	// deleteDelay paces the root's simulated teardown finalizer (FAKE_DELETE_DELAY,
	// default 5s in the demo; 0 = instant for tests). Set via Runtime; the zero value
	// is instant.
	deleteDelay time.Duration
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = Provider{}

// Work runs one classicbom task, routing on req.Reaction to the matching body since
// this kind declares THREE reactions (compose/rollup/teardown). The reaction name is
// selected by the core and shipped in the StageTask. compose expands the BOM into
// children + edges; rollup writes the per-team status once every descendant has
// synced; teardown runs the root's finalizer LAST (paced by deleteDelay), after its
// subtree is gone. An unknown reaction is a terminal miss — a manifest/handler drift
// bug, never silently succeeded.
func (p Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	switch req.Reaction {
	case reactionCompose:
		return composer{}.React(ctx, req)
	case reactionRollup:
		return rollup{}.React(ctx, req)
	case reactionTeardown:
		return fault.Teardown{Delay: p.deleteDelay}.React(ctx, req)
	default:
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("classicbom: unknown reaction %q", req.Reaction))
	}
}

// OnConfig is a no-op: classicbom composes from the resource spec alone and carries no
// default providerconfig.
func (Provider) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure composer with no downstream to dial can always do work.
func (Provider) Ready() bool { return true }

// New builds a configured Provider: deleteDelay paces the root's simulated teardown
// (0 = instant, for tests; the demo worker derives it from FAKE_DELETE_DELAY). PURE —
// no I/O. The in-process test harness builds the kind with it (via the test-only
// demoruntime bridge); the dumb worker runs the same Provider via converge.Serve. The
// concurrency cap lives in kind_config.
func New(deleteDelay time.Duration) Provider {
	return Provider{deleteDelay: deleteDelay}
}

// NewFromEnv builds a Provider from the classic-demo env knobs: FAKE_DELETE_DELAY
// (default 5s) paces the root's simulated teardown finalizer. The composer's compose +
// rollup reactions carry no simulated latency (they expand/roll up in one shot), so the
// only env knob is the teardown pacing — the sibling of account.NewFromEnv, so the demo
// worker wires every provider from the same environment.
func NewFromEnv() Provider {
	var cfg struct {
		DeleteDelay string `envconfig:"FAKE_DELETE_DELAY"`
	}
	// The struct has one optional field with no validation, so Process only errors on a
	// malformed value; treat an unset/blank knob as the default (0 → fault's own default).
	_ = envconfig.Process("", &cfg)
	return Provider{deleteDelay: fault.DeleteDelayFromEnv(cfg.DeleteDelay)}
}

// ─────────────────────────────────────────────────────────────────────────
// Composer
// ─────────────────────────────────────────────────────────────────────────

type composer struct{}

// React expands the ClassicBOM spec into the desired child set + dep edges. It
// reads req.Resource and returns the children + edges on the Outcome; it composes
// from the spec alone.
func (composer) React(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	var spec ClassicBOMSpec
	if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
		// A spec that doesn't even parse will never compose without an
		// edit → terminal (CompositionFailed). Stop-retrying until respec.
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("CompositionFailed: decode classicbom spec: %w", err))
	}
	var out converge.Outcome
	if spec.DeploymentInstance == nil {
		// "I expected a DeploymentInstance and got none" — a malformed spec that
		// composes zero children. Terminal CompositionFailed so the root surfaces
		// as phase=Failed with a real reason instead of a hollow "ready".
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("CompositionFailed: spec has no deployment_instance"))
	}

	diName := spec.DeploymentInstance.Name
	for _, fd := range spec.DeploymentInstance.FunctionalDomains {
		if fd == nil {
			continue
		}
		// FD-level TGW host account + TGW.
		if fdHasTGW(fd) {
			tgwName := fdTGWName(diName, fd.Name)
			tgwHostName := fdTGWAccountName(diName, fd.Name)
			out.Children = append(out.Children,
				converge.ChildSpec{
					Kind:        account.Kind,
					KindVersion: 1, // explicit web-API version (no implicit default)
					Name:        tgwHostName,
					Spec:        account.AccountSpec{TeamName: "tgw", Email: ""},
					Labels: map[string]string{
						"composed_by":         "classicbom",
						"deployment_instance": diName,
						"functional_domain":   fd.Name,
						"role":                "fd-tgw-host",
					},
				},
				converge.ChildSpec{
					Kind:        networking.KindTGW,
					KindVersion: 1,
					Name:        tgwName,
					Spec:        networking.TGWSpec{Name: fd.Name},
					Labels: map[string]string{
						"composed_by":         "classicbom",
						"deployment_instance": diName,
						"functional_domain":   fd.Name,
					},
				},
			)
			out.Edges = append(out.Edges, converge.DepEdge{
				From: converge.ResourceRef{Kind: networking.KindTGW, Name: tgwName},
				To:   converge.ResourceRef{Kind: account.Kind, Name: tgwHostName},
			})
		}

		for _, team := range fd.ServiceTeams {
			if team == nil {
				continue
			}
			teamRefName := teamAccountName(diName, fd.Name, team.Name)
			out.Children = append(out.Children, converge.ChildSpec{
				Kind:        account.Kind,
				KindVersion: 1,
				Name:        teamRefName,
				Spec:        account.AccountSpec{TeamName: team.Name},
				Labels: map[string]string{
					"composed_by":         "classicbom",
					"deployment_instance": diName,
					"functional_domain":   fd.Name,
					"team":                team.Name,
				},
			})

			if fdHasVPC(fd) {
				vpcRef := converge.ResourceRef{Kind: networking.KindVPC, Name: teamRefName}
				accountRef := converge.ResourceRef{Kind: account.Kind, Name: teamRefName}
				out.Children = append(out.Children, converge.ChildSpec{
					Kind:        networking.KindVPC,
					KindVersion: 1, // classicbom composes vpc/v1 children
					Name:        teamRefName,
					Spec:        networking.VPCSpec{CIDR: "10.0.0.0/16"},
					Labels: map[string]string{
						"composed_by":         "classicbom",
						"deployment_instance": diName,
						"functional_domain":   fd.Name,
						"team":                team.Name,
					},
				})
				out.Edges = append(out.Edges, converge.DepEdge{
					From: vpcRef, To: accountRef,
					Values: []converge.ValueFlow{
						{DependentField: "/account_id", SourceField: "/account_id"},
					},
				})
			}

			if fdHasTGW(fd) {
				routeRef := converge.ResourceRef{Kind: networking.KindRoute, Name: teamRefName}
				vpcRef := converge.ResourceRef{Kind: networking.KindVPC, Name: teamRefName}
				tgwRef := converge.ResourceRef{Kind: networking.KindTGW, Name: fdTGWName(diName, fd.Name)}
				out.Children = append(out.Children, converge.ChildSpec{
					Kind:        networking.KindRoute,
					KindVersion: 1,
					Name:        teamRefName,
					Spec:        networking.RouteSpec{},
					Labels: map[string]string{
						"composed_by":         "classicbom",
						"deployment_instance": diName,
						"functional_domain":   fd.Name,
						"team":                team.Name,
					},
				})
				out.Edges = append(out.Edges,
					converge.DepEdge{
						From: routeRef, To: vpcRef,
						Values: []converge.ValueFlow{
							{DependentField: "/vpc_id", SourceField: "/vpc_id"},
						},
					},
					converge.DepEdge{
						From: routeRef, To: tgwRef,
						Values: []converge.ValueFlow{
							{DependentField: "/tgw_id", SourceField: "/tgw_id"},
						},
					},
				)
			}
		}
	}

	return out, nil
}

// ─────────────────────────────────────────────────────────────────────────
// StatusRollup — runs once every descendant has synced. Reads the subtree
// and writes a per-team rollup.
// ─────────────────────────────────────────────────────────────────────────

type rollup struct{}

// React builds a per-team summary of the classicbom subtree. It reads
// req.Descendants and returns the rollup status + conditions on the Outcome.
//
// Two passes over req.Descendants:
//
//  1. Collect each FD's TGW id into tgwByFD. The TGW is an
//     FD-level resource (one per FD, label team=""), but its id
//     applies to every team in that FD. Doing this in pass 1
//     means it's known before any team rollup is built.
//
//  2. Build one RolledUpTeam per (fd, team) descendant, copying
//     fields from the descendant's status. The team's TGWID is
//     filled from tgwByFD[fd] at emit time.
//
// Two-pass shape is required: a single-pass version that stamps
// TGWID onto already-seen teams when it encounters the TGW row
// silently drops TGWs for teams whose first descendant happens
// to be processed AFTER the TGW. req.Descendants order is not
// guaranteed (no ORDER BY in ListDescendants), so single-pass
// produced rollups whose tgw_id presence — and therefore JSON
// byte size — varied run-to-run for identical inputs.
//
// Output teams are sorted by (FD, Team) so the encoded status
// is byte-stable across runs. The drainer's
// `r.spec #> dep_path IS DISTINCT FROM src_value` filter relies
// on stable encoding to skip redundant downstream patches; an
// unstable rollup would defeat that filter on every reconcile.
func (rollup) React(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	// Pass 1: per-FD TGW ids.
	tgwByFD := map[string]string{}
	for _, d := range req.Descendants {
		if d.IsFrozen() {
			continue // orphaned or quarantined child — ignored: set aside, not part of the rollup
		}
		if d.Kind != networking.KindTGW {
			continue
		}
		fd := d.Labels["functional_domain"]
		if fd == "" {
			continue
		}
		var tgw networking.TGWStatus
		if err := json.Unmarshal(d.Status, &tgw); err != nil {
			continue
		}
		tgwByFD[fd] = tgw.TGWID
	}

	// Pass 2: build team rollups. Skip FD-level resources here —
	// their TGW ids are already in tgwByFD; we don't need a
	// synthetic team="" entry to carry them.
	teams := map[string]*RolledUpTeam{} // keyed by the "fd/team" pair
	for _, d := range req.Descendants {
		if d.IsFrozen() {
			continue // orphaned or quarantined child — ignored: no phantom team, no Ready demotion
		}
		fd := d.Labels["functional_domain"]
		team := d.Labels["team"]
		if fd == "" || team == "" {
			continue
		}
		key := fd + "/" + team
		ht, ok := teams[key]
		if !ok {
			ht = &RolledUpTeam{FD: fd, Team: team, TGWID: tgwByFD[fd], Ready: true}
			teams[key] = ht
		}
		if !d.IsReady {
			ht.Ready = false
		}
		switch d.Kind {
		case account.Kind:
			var out account.AccountStatus
			if err := json.Unmarshal(d.Status, &out); err == nil {
				ht.AccountID = out.AccountID
			}
		case networking.KindVPC:
			var out networking.VPCStatus
			if err := json.Unmarshal(d.Status, &out); err == nil {
				ht.VPCID = out.VPCID
			}
		case networking.KindRoute:
			var out networking.RouteStatus
			if err := json.Unmarshal(d.Status, &out); err == nil {
				ht.RouteID = out.RouteID
			}
		}
	}

	out := ClassicBOMStatus{Teams: make([]RolledUpTeam, 0, len(teams))}
	for _, t := range teams {
		out.Teams = append(out.Teams, *t)
	}
	sort.Slice(out.Teams, func(i, j int) bool {
		if out.Teams[i].FD != out.Teams[j].FD {
			return out.Teams[i].FD < out.Teams[j].FD
		}
		return out.Teams[i].Team < out.Teams[j].Team
	})
	b, err := json.Marshal(out)
	if err != nil {
		return converge.Outcome{}, fmt.Errorf("marshal classicbom status: %w", err)
	}

	// Aggregate Ready: a composite is healthy iff every descendant is
	// is_ready (synced AND healthy). The rollup runs once no descendant is
	// still PROGRESSING, so a FAILED child (is_ready=false) is counted here
	// and flips the composite to Ready=False/ChildrenNotReady → health_ok
	// false → the root surfaces as Degraded instead of sitting in
	// conditionless limbo. A later resync (or the reactive demotion
	// cascade, when a child's health/failure flips) re-runs this rollup so
	// the composite tracks subtree health without a spec change. This is
	// composite demotion: Crossplane propagating a composed resource going
	// Unavailable up to the claim.
	var unready int
	for _, d := range req.Descendants {
		if d.OwnerID == nil { // skip the root itself if present
			continue
		}
		// Ignore children leaving/set aside: a deleting child (DeletionRequestedAt)
		// or a frozen child (IsFrozen = orphaned grace OR quarantined) must not
		// count toward unready — they're on their way out or deliberately set
		// aside, not a health problem. This is the whole point of quarantine: a
		// failed child set aside does not degrade the composite.
		if !d.IsReady && d.DeletionRequestedAt == nil && !d.IsFrozen() {
			unready++
		}
	}
	ready := converge.Condition{Type: converge.TypeReady, Status: converge.ConditionTrue, Reason: "Available"}
	if unready > 0 {
		ready = converge.Condition{
			Type:    converge.TypeReady,
			Status:  converge.ConditionFalse,
			Reason:  "ChildrenNotReady",
			Message: fmt.Sprintf("%d descendant(s) not ready", unready),
		}
	}
	return converge.Outcome{Status: b, Conditions: []converge.Condition{ready}}, nil
}

// ─────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────

func fdHasTGW(fd *FunctionalDomain) bool {
	if fd == nil || fd.AccountsOnly {
		return false
	}
	if fd.AWSTransitGateway == nil || !fd.AWSTransitGateway.Enabled {
		return false
	}
	return true
}

func fdHasVPC(fd *FunctionalDomain) bool {
	return fd != nil && !fd.AccountsOnly
}

// Resource names are globally unique per kind (UNIQUE(kind,name) on
// resource_meta), so every composed child name is prefixed with the
// deployment_instance name (diName): two BOMs that both declare FD "core" /
// team "platform" emit distinct names ("<fiA>-core-platform" vs
// "<fiB>-core-platform") and never collide. The segment separator is a hyphen,
// never a slash — a slash is the public (kind/name) addressing delimiter used by
// the API, conctl, and the UI, so a slash in the name itself would be ambiguous
// to parse back.
func fdTGWName(diName, fdName string) string {
	return diName + "-" + fdName
}

func teamAccountName(diName, fdName, teamName string) string {
	return diName + "-" + fdName + "-" + teamName
}

// fdTGWAccountName is the FD-level TGW host account. Its suffix is "-tgw-host"
// (not "-tgw") so it can never collide with a team account teamAccountName emits
// as "<di>-<fd>-<team>": since both use a hyphen separator, only a distinct
// suffix keeps them apart — a team would have to be literally named "tgw-host"
// to clash. Matches the row's role:fd-tgw-host label.
func fdTGWAccountName(diName, fdName string) string {
	return diName + "-" + fdName + "-tgw-host"
}
