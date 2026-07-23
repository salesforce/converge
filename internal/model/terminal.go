package model

import "errors"

// terminalError marks a reaction failure as TERMINAL: it will not succeed
// on retry without a spec change, so the reaper/scheduler should stop
// re-queuing it (the resource sits in phase=Failed until the user edits
// the spec, which bumps generation and clears failure_gen). Use for
// non-retryable errors — a malformed spec, a permanent provider rejection.
// A plain error returned by a reaction is treated as TRANSIENT (retried).
type terminalError struct{ err error }

func (e *terminalError) Error() string { return e.err.Error() }
func (e *terminalError) Unwrap() error { return e.err }

// Terminal wraps err to signal a non-retryable (terminal) failure. The
// dispatcher records phase=Failed with failure_terminal=true so the
// scheduler stops re-queuing it. nil in → nil out.
func Terminal(err error) error {
	if err == nil {
		return nil
	}
	return &terminalError{err: err}
}

// IsTerminal reports whether err (or anything it wraps) was marked
// terminal via Terminal(). Used by the dispatcher's failure path.
func IsTerminal(err error) bool {
	var te *terminalError
	return errors.As(err, &te)
}
