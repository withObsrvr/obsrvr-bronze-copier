package pas

import (
	"testing"
	"time"
)

func TestComputeEventHash(t *testing.T) {
	event := PASEvent{
		Version:   "1.1",
		EventType: "bronze_partition",
		Timestamp: time.Now(),
		Partition: PartitionInfo{
			Network:      "testnet",
			EraID:        "pre_p23",
			VersionLabel: "v1",
			LedgerStart:  1000,
			LedgerEnd:    1009,
		},
		Tables: map[string]TableInfo{
			"ledgers_lcm_raw": {
				Checksum:    "sha256:abc123",
				RowCount:    10,
				ByteSize:    1234,
				StoragePath: "testnet/pre_p23/v1/ledgers_lcm_raw/range=1000-1009",
			},
		},
		Producer: ProducerInfo{
			Name:    "bronze-copier",
			Version: "v0.1.0",
			GitSHA:  "abcdef",
		},
	}

	// Compute hash with empty prev_event_hash (first in chain)
	event.SetChainHashes("")

	// Hash should be computed and non-empty
	if event.Chain.EventHash == "" {
		t.Error("EventHash should be computed")
	}

	// Hash should start with sha256:
	if len(event.Chain.EventHash) < 7 || event.Chain.EventHash[:7] != "sha256:" {
		t.Errorf("EventHash should start with 'sha256:', got: %s", event.Chain.EventHash)
	}

	// PrevEventHash should be empty for first in chain
	if event.Chain.PrevEventHash != "" {
		t.Errorf("PrevEventHash should be empty for first in chain, got: %s", event.Chain.PrevEventHash)
	}
}

func TestHashChainDeterminism(t *testing.T) {
	// Create two identical events
	createEvent := func() PASEvent {
		return PASEvent{
			Version:   "1.1",
			EventType: "bronze_partition",
			Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			Partition: PartitionInfo{
				Network:      "testnet",
				EraID:        "pre_p23",
				VersionLabel: "v1",
				LedgerStart:  2000,
				LedgerEnd:    2009,
			},
			Tables: map[string]TableInfo{
				"table_a": {Checksum: "sha256:aaa"},
				"table_b": {Checksum: "sha256:bbb"},
			},
			Producer: ProducerInfo{Name: "test"},
		}
	}

	event1 := createEvent()
	event1.SetChainHashes("prev_hash_123")

	event2 := createEvent()
	event2.SetChainHashes("prev_hash_123")

	// Same content + same prev_hash = same event_hash (deterministic)
	if event1.Chain.EventHash != event2.Chain.EventHash {
		t.Errorf("Identical events should produce identical hashes.\n  Event1: %s\n  Event2: %s",
			event1.Chain.EventHash, event2.Chain.EventHash)
	}
}

func TestHashChainDifferentPrevHash(t *testing.T) {
	createEvent := func() PASEvent {
		return PASEvent{
			Version:   "1.1",
			EventType: "bronze_partition",
			Partition: PartitionInfo{
				Network:      "testnet",
				EraID:        "pre_p23",
				VersionLabel: "v1",
				LedgerStart:  3000,
				LedgerEnd:    3009,
			},
			Tables: map[string]TableInfo{
				"ledgers": {Checksum: "sha256:xyz"},
			},
		}
	}

	event1 := createEvent()
	event1.SetChainHashes("prev_hash_A")

	event2 := createEvent()
	event2.SetChainHashes("prev_hash_B")

	// Different prev_hash = different event_hash (chain integrity)
	if event1.Chain.EventHash == event2.Chain.EventHash {
		t.Error("Different prev_hash should produce different event_hash")
	}
}

func TestHashChainDifferentContent(t *testing.T) {
	event1 := PASEvent{
		Version:   "1.1",
		EventType: "bronze_partition",
		Partition: PartitionInfo{
			Network:      "testnet",
			EraID:        "pre_p23",
			VersionLabel: "v1",
			LedgerStart:  4000,
			LedgerEnd:    4009,
		},
		Tables: map[string]TableInfo{
			"ledgers": {Checksum: "sha256:checksum_A"},
		},
	}
	event1.SetChainHashes("")

	event2 := PASEvent{
		Version:   "1.1",
		EventType: "bronze_partition",
		Partition: PartitionInfo{
			Network:      "testnet",
			EraID:        "pre_p23",
			VersionLabel: "v1",
			LedgerStart:  4000,
			LedgerEnd:    4009,
		},
		Tables: map[string]TableInfo{
			"ledgers": {Checksum: "sha256:checksum_B"}, // Different checksum!
		},
	}
	event2.SetChainHashes("")

	// Different content = different event_hash (tamper evident)
	if event1.Chain.EventHash == event2.Chain.EventHash {
		t.Error("Different content should produce different event_hash")
	}
}

func TestTableOrderingDeterminism(t *testing.T) {
	// Tables should be sorted by name for deterministic hashing
	event1 := PASEvent{
		Partition: PartitionInfo{
			Network:      "testnet",
			EraID:        "pre_p23",
			VersionLabel: "v1",
			LedgerStart:  5000,
			LedgerEnd:    5009,
		},
		Tables: map[string]TableInfo{
			"zebra":    {Checksum: "sha256:z"},
			"alpha":    {Checksum: "sha256:a"},
			"middle":   {Checksum: "sha256:m"},
		},
	}
	event1.SetChainHashes("")

	// Create same event with tables added in different order
	event2 := PASEvent{
		Partition: PartitionInfo{
			Network:      "testnet",
			EraID:        "pre_p23",
			VersionLabel: "v1",
			LedgerStart:  5000,
			LedgerEnd:    5009,
		},
		Tables: map[string]TableInfo{
			"alpha":    {Checksum: "sha256:a"},
			"zebra":    {Checksum: "sha256:z"},
			"middle":   {Checksum: "sha256:m"},
		},
	}
	event2.SetChainHashes("")

	// Order shouldn't matter - hashes should be identical
	if event1.Chain.EventHash != event2.Chain.EventHash {
		t.Errorf("Table order should not affect hash.\n  Event1: %s\n  Event2: %s",
			event1.Chain.EventHash, event2.Chain.EventHash)
	}
}

func TestChainKey(t *testing.T) {
	p := PartitionInfo{
		Network:      "pubnet",
		EraID:        "pre_p23",
		VersionLabel: "v1",
		LedgerStart:  1000,
		LedgerEnd:    1999,
	}

	expected := "pubnet/pre_p23/v1"
	if p.ChainKey() != expected {
		t.Errorf("ChainKey() = %s, want %s", p.ChainKey(), expected)
	}
}
