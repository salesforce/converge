package model_test

import (
	"testing"

	"github.com/salesforce/converge/internal/model"
)

func TestIsReactorClassification(t *testing.T) {
	// reactor: only a reactor reaction (no transition — that lives on a binding)
	reactor := model.KindManifest{Kind: "sink", Reactions: []model.ReactionDecl{
		{Name: "react", Trigger: model.TriggerReactor, Emits: model.OutcomeMask{model.OutcomeSideEffect}},
	}}
	if !reactor.IsReactor() {
		t.Errorf("reactor-only manifest must be IsReactor()=true")
	}
	if rx, ok := reactor.ReactorReaction(); !ok || rx.Name != "react" {
		t.Errorf("ReactorReaction() must return the single reactor reaction; got %+v ok=%v", rx, ok)
	}
	// work kind: a specChange worker
	work := model.KindManifest{Kind: "account", Reactions: []model.ReactionDecl{
		{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
	}}
	if work.IsReactor() {
		t.Errorf("specChange manifest must be IsReactor()=false")
	}
	// mixed (work + reactor) → NOT a reactor (it's creatable)
	mixed := model.KindManifest{Kind: "x", Reactions: []model.ReactionDecl{
		{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		{Name: "react", Trigger: model.TriggerReactor, Emits: model.OutcomeMask{model.OutcomeSideEffect}},
	}}
	if mixed.IsReactor() {
		t.Errorf("manifest with a work reaction must be IsReactor()=false even if it also has a reactor reaction")
	}
	// empty manifest → not a reactor
	if (model.KindManifest{Kind: "empty"}).IsReactor() {
		t.Errorf("no-reaction manifest must be IsReactor()=false")
	}
}
