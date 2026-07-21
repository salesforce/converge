package account

import "github.com/salesforce/converge/sdk-go/converge"

// Kind is the kind name registered by this provider.
const Kind converge.Kind = "account"

// AccountSpec is the account kind's desired state. No upstream
// dependencies — accounts are leaves of the dep graph.
type AccountSpec struct {
	TeamName string `json:"team_name" minLength:"1" doc:"Owner team name; appears in tags."`
	Email    string `json:"email" doc:"Root-account contact email."`
}

// AccountStatus is what an account's Worker writes after reconcile. The
// account_id is the value VPCs and other dependents pick up via
// composer-emitted ValueFlows.
type AccountStatus struct {
	AccountID string `json:"account_id" doc:"Provider-issued 12-digit account id."`
}
