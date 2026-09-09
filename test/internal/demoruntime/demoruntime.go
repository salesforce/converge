// Package demoruntime is the TEST-ONLY constructor bridge that returns each demo
// provider as a converge.Provider for the in-process engine harness
// (test/internal/inproc). It exists so a test can build the demo kinds with one import
// instead of reaching into every provider package, while the demo provider packages
// themselves stay buildable against the PUBLIC SDK alone (the archtest
// TestControllersHaveNoInternalDeps guard enforces they import no internal/*).
//
// Each function mirrors the provider's own pure constructor (account.New,
// classicbom.New, celbom.New, statussink.NewForTest, or a bare Provider{} for the
// no-field leaves) and returns the converge.Provider the in-process executor runs
// directly. A worker binary builds the SAME providers via converge.Serve — this bridge
// is only for the zero-broker, in-process reaction executor the integration suite drives.
package demoruntime

import (
	"encoding/json"
	"time"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/examples/demos/classic/statussink"
	"github.com/salesforce/converge/examples/demos/datadriven/celbom"
	"github.com/salesforce/converge/examples/demos/datadriven/fakeapp"
	"github.com/salesforce/converge/examples/demos/datadriven/fakedb"
	"github.com/salesforce/converge/examples/demos/datadriven/fakevpc"
	"github.com/salesforce/converge/sdk-go/converge"
)

// Account returns a configured account provider (delay/deleteDelay/faults) for the
// in-process harness.
func Account(delay, deleteDelay time.Duration, faults fault.Injector) converge.Provider {
	return account.New(delay, deleteDelay, faults)
}

// ClassicBOM returns a configured classicbom composer provider (deleteDelay).
func ClassicBOM(deleteDelay time.Duration) converge.Provider {
	return classicbom.New(deleteDelay)
}

// CELBOM returns a configured celbom composer provider (defaultSpec = the rule set). In
// process the harness passes the rules JSON directly (there is no OnConfig push without a
// broker).
func CELBOM(defaultSpec json.RawMessage) converge.Provider {
	return celbom.New(defaultSpec)
}

// FakeApp returns the fakeapp leaf provider.
func FakeApp() converge.Provider { return fakeapp.Provider{} }

// FakeDB returns the fakedb leaf provider.
func FakeDB() converge.Provider { return fakedb.Provider{} }

// FakeVPC returns the fakevpc leaf provider.
func FakeVPC() converge.Provider { return fakevpc.Provider{} }

// StatusSink returns a pre-dialed statussink reactor provider (endpoint + default config
// doc) alongside the getObject hook so a test can assert what was uploaded. Construction
// stays in statussink.NewForTest (it sets unexported fields).
func StatusSink(endpoint, defaultDoc string) (converge.Provider, func(key string) ([]byte, bool)) {
	return statussink.NewForTest(endpoint, defaultDoc)
}
