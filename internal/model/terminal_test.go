package model

import (
	"errors"
	"fmt"
	"testing"
)

// TestTerminal pins the Terminal/IsTerminal contract: Terminal wraps an
// error so IsTerminal reports true through arbitrary wrapping, while a
// plain error is transient (IsTerminal=false), and nil stays nil.
func TestTerminal(t *testing.T) {
	if Terminal(nil) != nil {
		t.Fatal("Terminal(nil) must be nil")
	}

	plain := errors.New("boom")
	if IsTerminal(plain) {
		t.Error("a plain error must be transient (IsTerminal=false)")
	}

	term := Terminal(plain)
	if !IsTerminal(term) {
		t.Error("Terminal(err) must report IsTerminal=true")
	}
	if term.Error() != "boom" {
		t.Errorf("Terminal must preserve the message, got %q", term.Error())
	}
	if !errors.Is(term, plain) {
		t.Error("Terminal must Unwrap to the original error")
	}

	// Terminal survives further wrapping (providers often add context).
	wrapped := fmt.Errorf("compose stage 0: %w", term)
	if !IsTerminal(wrapped) {
		t.Error("IsTerminal must see through fmt.Errorf %w wrapping")
	}
	if IsTerminal(nil) {
		t.Error("IsTerminal(nil) must be false")
	}
}
