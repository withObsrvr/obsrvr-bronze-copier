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
}

// Writer records partition metadata to the catalog.
type Writer interface {
	// EnsureDataset registers or retrieves a dataset entry.
	// Returns the dataset ID for use in lineage records.
	EnsureDataset(ctx context.Context, info DatasetInfo) (int64, error)

	// RecordPartition writes a lineage record for a committed partition.
	RecordPartition(ctx context.Context, rec PartitionRecord) error

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

// PartitionRecord describes a committed partition.
type PartitionRecord struct {
	// Dataset identification (either DatasetID or DatasetInfo must be set)
	DatasetID int64
	EraID     string
	VersionLabel string

	// Partition range
	Start uint32
	End   uint32

	// Per-table checksums and row counts
	Checksums map[string]string
	RowCounts map[string]int64

	// Metadata
	ByteSize        int64
	StoragePath     string
	ProducerVersion string
	ProducerGitSHA  string
	SourceType      string
	SourceLocation  string
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

func (n *noopWriter) Close() error {
	return nil
}

// String returns a string representation of PartitionRecord for logging.
func (r PartitionRecord) String() string {
	return fmt.Sprintf("partition[%d-%d] era=%s version=%s", r.Start, r.End, r.EraID, r.VersionLabel)
}
