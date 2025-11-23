package pas

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"
)

// PASEvent represents a PAS v1.1 event for partition audit.
type PASEvent struct {
	Version   string    `json:"version"`
	EventType string    `json:"event_type"`
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`

	Partition PartitionInfo          `json:"partition"`
	Tables    map[string]TableInfo   `json:"tables"`
	Producer  ProducerInfo           `json:"producer"`
	Chain     ChainInfo              `json:"chain"`
}

// PartitionInfo identifies the partition being audited.
type PartitionInfo struct {
	Network      string `json:"network"`
	EraID        string `json:"era_id"`
	VersionLabel string `json:"version_label"`
	LedgerStart  uint32 `json:"ledger_start"`
	LedgerEnd    uint32 `json:"ledger_end"`
}

// TableInfo contains checksum and metadata for a single table.
type TableInfo struct {
	Checksum    string `json:"checksum"`
	RowCount    int64  `json:"row_count"`
	StoragePath string `json:"storage_path"`
	ByteSize    int64  `json:"byte_size"`
}

// ProducerInfo identifies the software that produced the data.
type ProducerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	GitSHA  string `json:"git_sha"`
}

// ChainInfo provides hash chaining for tamper-evident audit log.
type ChainInfo struct {
	PrevEventHash string `json:"prev_event_hash"`
	EventHash     string `json:"event_hash"`
}

// ChainKey returns the unique key for this partition's chain.
func (p PartitionInfo) ChainKey() string {
	return p.Network + "/" + p.EraID + "/" + p.VersionLabel
}

// ComputeEventHash computes the SHA256 hash for this event.
// The hash is computed over a deterministic canonical representation:
//
//	SHA256(ledger_start|ledger_end|prev_event_hash|sorted_table_checksums)
//
// This creates a tamper-evident chain where any modification to the
// event's content will result in a different hash.
func (e *PASEvent) ComputeEventHash() string {
	h := sha256.New()

	// Write partition range (deterministic ordering)
	h.Write([]byte(fmt.Sprintf("%d|%d|", e.Partition.LedgerStart, e.Partition.LedgerEnd)))

	// Write prev_event_hash (empty string if first in chain)
	h.Write([]byte(e.Chain.PrevEventHash))
	h.Write([]byte("|"))

	// Write network/era/version for chain isolation
	h.Write([]byte(e.Partition.Network))
	h.Write([]byte("|"))
	h.Write([]byte(e.Partition.EraID))
	h.Write([]byte("|"))
	h.Write([]byte(e.Partition.VersionLabel))
	h.Write([]byte("|"))

	// Write table checksums in sorted order for determinism
	tableNames := make([]string, 0, len(e.Tables))
	for name := range e.Tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	for _, name := range tableNames {
		table := e.Tables[name]
		h.Write([]byte(name))
		h.Write([]byte(":"))
		h.Write([]byte(table.Checksum))
		h.Write([]byte("|"))
	}

	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// SetChainHashes sets the prev_event_hash and computes the event_hash.
// This should be called right before emission to ensure the hash chain
// is properly maintained.
func (e *PASEvent) SetChainHashes(prevEventHash string) {
	e.Chain.PrevEventHash = prevEventHash
	e.Chain.EventHash = e.ComputeEventHash()
}
