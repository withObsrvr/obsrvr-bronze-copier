package era

import (
	"testing"
)

func TestRouterRoute(t *testing.T) {
	eras := []Config{
		{EraID: "era1", LedgerStart: 1, LedgerEnd: 100, VersionLabel: "v1"},
		{EraID: "era2", LedgerStart: 101, LedgerEnd: 200, VersionLabel: "v2"},
		{EraID: "era3", LedgerStart: 201, LedgerEnd: 0, VersionLabel: "v3"}, // unbounded
	}

	router, err := NewRouter(eras, nil)
	if err != nil {
		t.Fatalf("NewRouter failed: %v", err)
	}

	tests := []struct {
		ledger   uint32
		expected string
	}{
		{1, "era1"},
		{50, "era1"},
		{100, "era1"},
		{101, "era2"},
		{150, "era2"},
		{200, "era2"},
		{201, "era3"},
		{1000000, "era3"}, // unbounded era
	}

	for _, tt := range tests {
		era, err := router.Route(tt.ledger)
		if err != nil {
			t.Errorf("Route(%d) failed: %v", tt.ledger, err)
			continue
		}
		if era.EraID != tt.expected {
			t.Errorf("Route(%d) = %s, want %s", tt.ledger, era.EraID, tt.expected)
		}
	}
}

func TestRouterActiveEras(t *testing.T) {
	eras := []Config{
		{EraID: "era1", LedgerStart: 1, LedgerEnd: 100},
		{EraID: "era2", LedgerStart: 101, LedgerEnd: 200},
		{EraID: "era3", LedgerStart: 201, LedgerEnd: 0},
	}

	// Only era2 is active
	router, err := NewRouter(eras, []string{"era2"})
	if err != nil {
		t.Fatalf("NewRouter failed: %v", err)
	}

	// Should fail for ledgers in inactive eras
	_, err = router.Route(50)
	if err == nil {
		t.Error("Route(50) should fail for inactive era1")
	}

	// Should succeed for ledgers in active era
	era, err := router.Route(150)
	if err != nil {
		t.Errorf("Route(150) failed: %v", err)
	} else if era.EraID != "era2" {
		t.Errorf("Route(150) = %s, want era2", era.EraID)
	}

	// RouteAny should work regardless of active status
	era, err = router.RouteAny(50)
	if err != nil {
		t.Errorf("RouteAny(50) failed: %v", err)
	} else if era.EraID != "era1" {
		t.Errorf("RouteAny(50) = %s, want era1", era.EraID)
	}
}

func TestRouterOverlapDetection(t *testing.T) {
	// Test overlapping ranges
	eras := []Config{
		{EraID: "era1", LedgerStart: 1, LedgerEnd: 100},
		{EraID: "era2", LedgerStart: 90, LedgerEnd: 200}, // overlaps with era1
	}

	_, err := NewRouter(eras, nil)
	if err == nil {
		t.Error("NewRouter should fail for overlapping eras")
	}
}

func TestRouterUnboundedMiddle(t *testing.T) {
	// Test unbounded era in the middle (should fail)
	eras := []Config{
		{EraID: "era1", LedgerStart: 1, LedgerEnd: 0},   // unbounded
		{EraID: "era2", LedgerStart: 101, LedgerEnd: 200}, // after unbounded
	}

	_, err := NewRouter(eras, nil)
	if err == nil {
		t.Error("NewRouter should fail for unbounded era followed by another era")
	}
}

func TestRouterSplitRange(t *testing.T) {
	eras := []Config{
		{EraID: "era1", LedgerStart: 1, LedgerEnd: 100},
		{EraID: "era2", LedgerStart: 101, LedgerEnd: 200},
	}

	router, err := NewRouter(eras, nil)
	if err != nil {
		t.Fatalf("NewRouter failed: %v", err)
	}

	// Test range within single era
	splits, err := router.SplitRangeByEra(50, 75)
	if err != nil {
		t.Fatalf("SplitRangeByEra failed: %v", err)
	}
	if len(splits) != 1 {
		t.Errorf("Expected 1 split, got %d", len(splits))
	}
	if splits[0].Era.EraID != "era1" || splits[0].Start != 50 || splits[0].End != 75 {
		t.Errorf("Unexpected split: %+v", splits[0])
	}

	// Test range spanning multiple eras
	splits, err = router.SplitRangeByEra(50, 150)
	if err != nil {
		t.Fatalf("SplitRangeByEra failed: %v", err)
	}
	if len(splits) != 2 {
		t.Errorf("Expected 2 splits, got %d", len(splits))
	}
	if splits[0].Era.EraID != "era1" || splits[0].Start != 50 || splits[0].End != 100 {
		t.Errorf("Unexpected split[0]: %+v", splits[0])
	}
	if splits[1].Era.EraID != "era2" || splits[1].Start != 101 || splits[1].End != 150 {
		t.Errorf("Unexpected split[1]: %+v", splits[1])
	}
}

func TestEraContains(t *testing.T) {
	era := Config{LedgerStart: 100, LedgerEnd: 200}

	tests := []struct {
		ledger   uint32
		expected bool
	}{
		{99, false},
		{100, true},
		{150, true},
		{200, true},
		{201, false},
	}

	for _, tt := range tests {
		if got := era.Contains(tt.ledger); got != tt.expected {
			t.Errorf("Contains(%d) = %v, want %v", tt.ledger, got, tt.expected)
		}
	}

	// Test unbounded era
	unbounded := Config{LedgerStart: 100, LedgerEnd: 0}
	if !unbounded.Contains(100) {
		t.Error("Unbounded era should contain LedgerStart")
	}
	if !unbounded.Contains(1000000) {
		t.Error("Unbounded era should contain large ledger")
	}
}
