// Package era provides era boundary routing for the Bronze Copier.
package era

import (
	"errors"
	"fmt"
	"sort"
)

// ErrNoMatchingEra is returned when a ledger doesn't match any configured era.
var ErrNoMatchingEra = errors.New("no matching era for ledger sequence")

// ErrOverlappingEras is returned when era boundaries overlap.
var ErrOverlappingEras = errors.New("era boundaries overlap")

// Config defines configuration for a single era.
type Config struct {
	EraID         string `yaml:"era_id"`
	VersionLabel  string `yaml:"version_label"`
	Network       string `yaml:"network"`
	LedgerStart   uint32 `yaml:"ledger_start"`
	LedgerEnd     uint32 `yaml:"ledger_end"` // 0 = unbounded
	PartitionSize uint32 `yaml:"partition_size"`
}

// Contains returns true if the ledger sequence is within this era's bounds.
func (c Config) Contains(ledgerSeq uint32) bool {
	if ledgerSeq < c.LedgerStart {
		return false
	}
	// LedgerEnd == 0 means unbounded (for live tail)
	if c.LedgerEnd == 0 {
		return true
	}
	return ledgerSeq <= c.LedgerEnd
}

// Router routes ledgers to their corresponding eras.
type Router struct {
	eras       []Config
	activeEras map[string]bool
}

// NewRouter creates a new era router with the given era configurations.
// Eras are sorted by LedgerStart for efficient lookup.
func NewRouter(eras []Config, activeEraIDs []string) (*Router, error) {
	if len(eras) == 0 {
		return nil, errors.New("at least one era must be configured")
	}

	// Sort eras by LedgerStart
	sorted := make([]Config, len(eras))
	copy(sorted, eras)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LedgerStart < sorted[j].LedgerStart
	})

	// Validate no overlaps
	for i := 0; i < len(sorted)-1; i++ {
		current := sorted[i]
		next := sorted[i+1]

		// If current era is unbounded, it can't be followed by another era
		if current.LedgerEnd == 0 {
			return nil, fmt.Errorf("%w: era %q is unbounded but followed by %q",
				ErrOverlappingEras, current.EraID, next.EraID)
		}

		// Check for overlap: current's end must be before next's start
		if current.LedgerEnd >= next.LedgerStart {
			return nil, fmt.Errorf("%w: era %q ends at %d but %q starts at %d",
				ErrOverlappingEras, current.EraID, current.LedgerEnd, next.EraID, next.LedgerStart)
		}
	}

	// Build active eras map
	active := make(map[string]bool)
	if len(activeEraIDs) == 0 {
		// If no active eras specified, all eras are active
		for _, era := range sorted {
			active[era.EraID] = true
		}
	} else {
		for _, id := range activeEraIDs {
			active[id] = true
		}
	}

	return &Router{
		eras:       sorted,
		activeEras: active,
	}, nil
}

// Route returns the era configuration for a given ledger sequence.
// Only returns active eras.
func (r *Router) Route(ledgerSeq uint32) (*Config, error) {
	for i := range r.eras {
		era := &r.eras[i]
		if era.Contains(ledgerSeq) {
			if r.activeEras[era.EraID] {
				return era, nil
			}
			// Era matches but is not active
			return nil, fmt.Errorf("ledger %d matches era %q but era is not active", ledgerSeq, era.EraID)
		}
	}
	return nil, fmt.Errorf("%w: ledger sequence %d", ErrNoMatchingEra, ledgerSeq)
}

// RouteAny returns the era configuration for a given ledger sequence,
// regardless of whether the era is active.
func (r *Router) RouteAny(ledgerSeq uint32) (*Config, error) {
	for i := range r.eras {
		era := &r.eras[i]
		if era.Contains(ledgerSeq) {
			return era, nil
		}
	}
	return nil, fmt.Errorf("%w: ledger sequence %d", ErrNoMatchingEra, ledgerSeq)
}

// ActiveEras returns the list of active era configurations.
func (r *Router) ActiveEras() []Config {
	var active []Config
	for _, era := range r.eras {
		if r.activeEras[era.EraID] {
			active = append(active, era)
		}
	}
	return active
}

// AllEras returns all configured eras.
func (r *Router) AllEras() []Config {
	return r.eras
}

// IsActive returns true if the given era ID is active.
func (r *Router) IsActive(eraID string) bool {
	return r.activeEras[eraID]
}

// EraForRange returns the era that should be used for a ledger range.
// Returns an error if the range spans multiple eras.
func (r *Router) EraForRange(start, end uint32) (*Config, error) {
	startEra, err := r.RouteAny(start)
	if err != nil {
		return nil, err
	}

	endEra, err := r.RouteAny(end)
	if err != nil {
		return nil, err
	}

	if startEra.EraID != endEra.EraID {
		return nil, fmt.Errorf("ledger range [%d-%d] spans multiple eras: %q and %q",
			start, end, startEra.EraID, endEra.EraID)
	}

	return startEra, nil
}

// SplitRangeByEra splits a ledger range into sub-ranges by era.
// Returns a slice of (era, start, end) tuples.
func (r *Router) SplitRangeByEra(start, end uint32) ([]RangeSplit, error) {
	var splits []RangeSplit

	current := start
	for current <= end {
		era, err := r.RouteAny(current)
		if err != nil {
			return nil, err
		}

		// Determine the end of this era's portion
		rangeEnd := end
		if era.LedgerEnd != 0 && era.LedgerEnd < end {
			rangeEnd = era.LedgerEnd
		}

		splits = append(splits, RangeSplit{
			Era:   *era,
			Start: current,
			End:   rangeEnd,
		})

		current = rangeEnd + 1
	}

	return splits, nil
}

// RangeSplit represents a portion of a ledger range within a single era.
type RangeSplit struct {
	Era   Config
	Start uint32
	End   uint32
}
