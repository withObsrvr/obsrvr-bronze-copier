package metadata

import (
	"context"
	"fmt"
	"log"
)

// CatalogConfig configures the metadata catalog connection.
type CatalogConfig struct {
	PostgresDSN string // postgres://user:pass@host:5432/db
	Namespace   string // e.g., "mainnet"
	Strict      bool   // If true, catalog failures are hard errors
}

// Writer records partition metadata to the catalog.
type Writer interface {
	// EnsureDataset registers or retrieves a dataset entry.
	// Returns the dataset ID for use in lineage records.
	EnsureDataset(ctx context.Context, info DatasetInfo) (int64, error)

	// RecordPartition writes a lineage record for a committed partition.
	RecordPartition(ctx context.Context, rec PartitionRecord) error

	// PartitionExists checks if a partition has already been committed.
	// Used for idempotency - skip partitions that are already done.
	PartitionExists(ctx context.Context, datasetID int64, start, end uint32) (bool, error)

	// GetLastCommittedLedger returns the highest ledger_end for a dataset.
	// Used to determine where to resume from.
	GetLastCommittedLedger(ctx context.Context, datasetID int64) (uint32, error)

	// GetLastChecksum returns the checksum of the most recent partition.
	// Used for PAS hash chaining (prev_hash).
	GetLastChecksum(ctx context.Context, datasetID int64) (string, error)

	// GetLastLineage returns the most recent lineage record for hash chaining.
	// Returns nil if no previous lineage exists.
	GetLastLineage(ctx context.Context, datasetID int64) (*LineageRecord, error)

	// InsertLineage records a lineage entry with prev_hash chain.
	InsertLineage(ctx context.Context, rec LineageRecord) error

	// InsertQuality records a quality validation result.
	InsertQuality(ctx context.Context, rec QualityRecord) error

	// Close releases database connections.
	Close() error
}

// DatasetInfo describes a Bronze dataset (table x version).
type DatasetInfo struct {
	Domain      string // "bronze"
	Dataset     string // "ledgers_lcm_raw"
	Version     string // "v1"
	EraID       string // "pre_p23"
	Network     string // "pubnet", "testnet"
	SchemaHash  string // hash of parquet schema
	Description string
}

// PartitionRecord describes a committed partition (legacy interface).
type PartitionRecord struct {
	// Dataset identification (either DatasetID or DatasetInfo must be set)
	DatasetID int64
	EraID     string
	VersionLabel string

	// Partition range
	Start uint32
	End   uint32

	// Per-table checksums and row counts
	Checksums    map[string]string
	RowCounts    map[string]int64
	PrimaryTable string // Primary table for checksum (default: first in Checksums)

	// Metadata
	ByteSize        int64
	StoragePath     string
	ProducerVersion string
	ProducerGitSHA  string
	SourceType      string
	SourceLocation  string
}

// LineageRecord is a complete lineage entry with hash chaining.
type LineageRecord struct {
	DatasetID       int64
	LedgerStart     uint32
	LedgerEnd       uint32
	RowCount        int64
	ByteSize        int64
	Checksum        string // sha256 of this partition
	PrevHash        string // checksum of previous partition (for chain verification)
	StoragePath     string
	StorageURI      string
	ProducerVersion string
	ProducerGitSHA  string
	SourceType      string
	SourceLocation  string
}

// QualityRecord tracks validation results per partition.
type QualityRecord struct {
	DatasetID    int64
	LedgerStart  uint32
	LedgerEnd    uint32
	Passed       bool
	ErrorMessage string // non-empty if validation failed
}

// NewWriter creates a Writer based on configuration.
// If PostgresDSN is empty, returns a no-op writer.
func NewWriter(cfg CatalogConfig) Writer {
	if cfg.PostgresDSN == "" {
		log.Println("[metadata] no PostgresDSN configured, using no-op writer")
		return &noopWriter{cfg: cfg}
	}

	// Create PostgreSQL writer
	writer, err := NewPostgresWriter(cfg)
	if err != nil {
		log.Printf("[metadata] failed to create PostgreSQL writer: %v, falling back to no-op", err)
		return &noopWriter{cfg: cfg}
	}

	return writer
}

// noopWriter discards all metadata (used when catalog is not configured).
type noopWriter struct {
	cfg CatalogConfig
}

func (n *noopWriter) EnsureDataset(_ context.Context, _ DatasetInfo) (int64, error) {
	return 0, nil
}

func (n *noopWriter) RecordPartition(_ context.Context, _ PartitionRecord) error {
	return nil
}

func (n *noopWriter) PartitionExists(_ context.Context, _ int64, _, _ uint32) (bool, error) {
	return false, nil // Always process when no catalog
}

func (n *noopWriter) GetLastCommittedLedger(_ context.Context, _ int64) (uint32, error) {
	return 0, nil // No catalog means no history
}

func (n *noopWriter) GetLastChecksum(_ context.Context, _ int64) (string, error) {
	return "", nil // No previous checksum
}

func (n *noopWriter) GetLastLineage(_ context.Context, _ int64) (*LineageRecord, error) {
	return nil, nil // No previous lineage
}

func (n *noopWriter) InsertLineage(_ context.Context, _ LineageRecord) error {
	return nil
}

func (n *noopWriter) InsertQuality(_ context.Context, _ QualityRecord) error {
	return nil
}

func (n *noopWriter) Close() error {
	return nil
}

// String returns a string representation of PartitionRecord for logging.
func (r PartitionRecord) String() string {
	return fmt.Sprintf("partition[%d-%d] era=%s version=%s", r.Start, r.End, r.EraID, r.VersionLabel)
}
