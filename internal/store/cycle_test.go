package store

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

// edges builds a dependent→dependency adjacency from (from,to) pairs.
func edges(pairs ...[2]uuid.UUID) map[uuid.UUID][]uuid.UUID {
	adj := map[uuid.UUID][]uuid.UUID{}
	for _, p := range pairs {
		adj[p[0]] = append(adj[p[0]], p[1])
	}
	return adj
}

func TestDetectDepCycle(t *testing.T) {
	a, b, c, d := uuid.New(), uuid.New(), uuid.New(), uuid.New()

	t.Run("acyclic chain A->B->C passes", func(t *testing.T) {
		if cyc := detectDepCycle(edges([2]uuid.UUID{a, b}, [2]uuid.UUID{b, c})); cyc != nil {
			t.Fatalf("DAG must have no cycle, got %v", cyc)
		}
	})

	t.Run("diamond A->B,A->C,B->D,C->D passes", func(t *testing.T) {
		// A converging diamond is a DAG (shared dependency D), NOT a cycle.
		adj := edges([2]uuid.UUID{a, b}, [2]uuid.UUID{a, c}, [2]uuid.UUID{b, d}, [2]uuid.UUID{c, d})
		if cyc := detectDepCycle(adj); cyc != nil {
			t.Fatalf("diamond must have no cycle, got %v", cyc)
		}
	})

	t.Run("self-edge A->A detected", func(t *testing.T) {
		cyc := detectDepCycle(edges([2]uuid.UUID{a, a}))
		if cyc == nil {
			t.Fatal("self-edge must be detected as a cycle")
		}
		if cyc[0] != a || cyc[len(cyc)-1] != a {
			t.Fatalf("self cycle should start and end at A, got %v", cyc)
		}
	})

	t.Run("two-cycle A->B->A detected", func(t *testing.T) {
		cyc := detectDepCycle(edges([2]uuid.UUID{a, b}, [2]uuid.UUID{b, a}))
		if cyc == nil {
			t.Fatal("A->B->A must be detected")
		}
		// The cycle closes on itself: first == last.
		if cyc[0] != cyc[len(cyc)-1] {
			t.Fatalf("cycle must close (first==last), got %v", cyc)
		}
		if !containsAll(cyc, a, b) {
			t.Fatalf("cycle must include both A and B, got %v", cyc)
		}
	})

	t.Run("three-cycle A->B->C->A detected", func(t *testing.T) {
		cyc := detectDepCycle(edges([2]uuid.UUID{a, b}, [2]uuid.UUID{b, c}, [2]uuid.UUID{c, a}))
		if cyc == nil {
			t.Fatal("A->B->C->A must be detected")
		}
		if !containsAll(cyc, a, b, c) {
			t.Fatalf("cycle must include A,B,C, got %v", cyc)
		}
	})

	t.Run("cycle in a larger graph with an acyclic prefix", func(t *testing.T) {
		// D->A is an acyclic feeder into a B<->C cycle; the cycle must still be found.
		adj := edges([2]uuid.UUID{d, a}, [2]uuid.UUID{a, b}, [2]uuid.UUID{b, c}, [2]uuid.UUID{c, b})
		if cyc := detectDepCycle(adj); cyc == nil {
			t.Fatal("a cycle reachable from an acyclic prefix must be detected")
		}
	})

	t.Run("empty graph passes", func(t *testing.T) {
		if cyc := detectDepCycle(map[uuid.UUID][]uuid.UUID{}); cyc != nil {
			t.Fatalf("empty graph has no cycle, got %v", cyc)
		}
	})
}

func TestFormatCycle(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	idToRef := map[uuid.UUID]refKey{
		a: {Kind: "composite", Name: "group/res-1"},
		b: {Kind: "policy", Name: "group/res-2"},
	}
	got := formatCycle([]uuid.UUID{a, b, a}, idToRef)
	want := "composite/group/res-1 → policy/group/res-2 → composite/group/res-1"
	if got != want {
		t.Fatalf("formatCycle = %q, want %q", got, want)
	}
	// An id missing from the map falls back to its uuid (defensive).
	c := uuid.New()
	if out := formatCycle([]uuid.UUID{c}, idToRef); !strings.Contains(out, c.String()) {
		t.Fatalf("unmapped id should fall back to uuid, got %q", out)
	}
}

func containsAll(cyc []uuid.UUID, ids ...uuid.UUID) bool {
	have := map[uuid.UUID]bool{}
	for _, id := range cyc {
		have[id] = true
	}
	for _, id := range ids {
		if !have[id] {
			return false
		}
	}
	return true
}
