package pas

import (
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
