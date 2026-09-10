package model_test

import (
	"testing"

	"github.com/salesforce/converge/internal/model"
)

// TestOutcomeMaskHas covers the bit-set membership used on the dispatch path.
func TestOutcomeMaskHas(t *testing.T) {
	m := model.OutcomeMask{model.OutcomeChildren, model.OutcomeStatus}
	if !m.Has(model.OutcomeChildren) || !m.Has(model.OutcomeStatus) {
		t.Fatal("Has should report declared bits")
	}
	if m.Has(model.OutcomeFinalizer) {
		t.Fatal("Has should not report an undeclared bit")
	}
	if (model.OutcomeMask(nil)).Has(model.OutcomeStatus) {
		t.Fatal("nil mask Has anything = false")
	}
}

// TestValidateManifest mirrors the DB validate_kind_manifest() CHECK: the same
// inputs accepted/rejected on both sides keeps the SDK and DB lattices in lock-step.
func TestValidateManifest(t *testing.T) {
	mask := func(bits ...model.OutcomeBit) model.OutcomeMask { return bits }

	cases := []struct {
		name    string
		m       model.KindManifest
		wantErr bool
	}{
		{
			name: "classicbom: compose+work+rollup",
			m: model.KindManifest{Kind: "classicbom",
				KindVersion: 1, FinalizerName: "converge.io/classicbom", Reactions: []model.ReactionDecl{
					{Name: "compose", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeChildren, model.OutcomeEdges, model.OutcomeConfigs, model.OutcomeStatus)},
					{Name: "rollup", Trigger: model.TriggerChildrenSettled, Emits: mask(model.OutcomeStatus)},
				}},
		},
		{
			name: "account: plain worker",
			m: model.KindManifest{Kind: "account",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "work", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeStatus)},
				}},
		},
		{
			name: "statussink: reactor sideeffect (no transition — it's on the binding)",
			m: model.KindManifest{Kind: "statussink",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "react", Trigger: model.TriggerReactor, Emits: mask(model.OutcomeSideEffect)},
				}},
		},
		{
			name:    "reject: reactor without sideEffect",
			wantErr: true,
			m: model.KindManifest{Kind: "bad",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "react", Trigger: model.TriggerReactor, Emits: mask(model.OutcomeStatus)},
				}},
		},
		{
			name:    "reject: reactor emitting more than sideEffect",
			wantErr: true,
			m: model.KindManifest{Kind: "bad",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "react", Trigger: model.TriggerReactor, Emits: mask(model.OutcomeSideEffect, model.OutcomeChildren)},
				}},
		},
		{
			name:    "reject: two reactor reactions on one kind (ambiguous resolution)",
			wantErr: true,
			m: model.KindManifest{Kind: "bad",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "r1", Trigger: model.TriggerReactor, Emits: mask(model.OutcomeSideEffect)},
					{Name: "r2", Trigger: model.TriggerReactor, Emits: mask(model.OutcomeSideEffect)},
				}},
		},
		{
			name:    "reject: children without status",
			wantErr: true,
			m: model.KindManifest{Kind: "bad",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "c", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeChildren)},
				}},
		},
		{
			name:    "reject: children + sideEffect",
			wantErr: true,
			m: model.KindManifest{Kind: "bad",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "c", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeChildren, model.OutcomeStatus, model.OutcomeSideEffect)},
				}},
		},
		{
			name:    "reject: two rollups",
			wantErr: true,
			m: model.KindManifest{Kind: "bad",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "r1", Trigger: model.TriggerChildrenSettled, Emits: mask(model.OutcomeStatus)},
					{Name: "r2", Trigger: model.TriggerChildrenSettled, Emits: mask(model.OutcomeStatus)},
				}},
		},
		{
			name:    "reject: two composers",
			wantErr: true,
			m: model.KindManifest{Kind: "bad",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "c1", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeChildren, model.OutcomeStatus)},
					{Name: "c2", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeChildren, model.OutcomeStatus)},
				}},
		},
		{
			name:    "reject: operation without verb",
			wantErr: true,
			m: model.KindManifest{Kind: "bad",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "op", Trigger: model.TriggerOperation, Emits: mask(model.OutcomeOperationOutput)},
				}},
		},
		{
			name:    "reject: unknown trigger",
			wantErr: true,
			m: model.KindManifest{Kind: "bad",
				KindVersion: 1, Reactions: []model.ReactionDecl{
					{Name: "x", Trigger: "bogus", Emits: mask(model.OutcomeStatus)},
				}},
		},
		{
			name:    "reject: missing kind",
			wantErr: true,
			m:       model.KindManifest{Reactions: nil},
		},
		{
			// kind_version is REQUIRED and explicit (>= 1); a 0/unset must be
			// rejected here, never normalized to v1 — matching the DB CHECK and
			// the store guard. Guards against the exported gate silently passing a
			// version the write path then rejects.
			name:    "reject: kind_version 0 (no implicit v1 default)",
			wantErr: true,
			m: model.KindManifest{
				Kind: "vpc", KindVersion: 0, Reactions: []model.ReactionDecl{
					{Name: "work", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeStatus)},
				},
			},
		},
		{
			name:    "reject: kind_version negative",
			wantErr: true,
			m: model.KindManifest{
				Kind: "vpc", KindVersion: -1, Reactions: []model.ReactionDecl{
					{Name: "work", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeStatus)},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := model.ValidateManifest(tc.m)
			if tc.wantErr && err == nil {
				t.Fatalf("ValidateManifest(%s) = nil, want error", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateManifest(%s) = %v, want nil", tc.name, err)
			}
		})
	}
}

// TestReactionSelectors covers the kind-blind selectors the core uses to find
// the composer / rollup / worker reaction off the manifest masks.
func TestReactionSelectors(t *testing.T) {
	mask := func(bits ...model.OutcomeBit) model.OutcomeMask { return bits }
	m := model.KindManifest{Kind: "classicbom",
		KindVersion: 1, Reactions: []model.ReactionDecl{
			{Name: "compose", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeChildren, model.OutcomeStatus)},
			{Name: "rollup", Trigger: model.TriggerChildrenSettled, Emits: mask(model.OutcomeStatus)},
		}}
	if rx, ok := m.ComposerReaction(); !ok || rx.Name != "compose" {
		t.Fatalf("ComposerReaction = %+v, %v", rx, ok)
	}
	if rx, ok := m.RollupReaction(); !ok || rx.Name != "rollup" {
		t.Fatalf("RollupReaction = %+v, %v", rx, ok)
	}
	if _, ok := m.WorkerReaction(); ok {
		t.Fatal("classicbom has no plain-worker reaction (its specChange emits children)")
	}

	leaf := model.KindManifest{Kind: "account",
		KindVersion: 1, Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: mask(model.OutcomeStatus)},
		}}
	if rx, ok := leaf.WorkerReaction(); !ok || rx.Name != "work" {
		t.Fatalf("WorkerReaction = %+v, %v", rx, ok)
	}
	if _, ok := leaf.ComposerReaction(); ok {
		t.Fatal("account is not a composer")
	}
}
