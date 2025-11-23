package metadata

import (
	"context"
)

type CatalogConfig struct {
	PostgresDSN string
	Namespace   string
}

type Writer interface {
	RecordPartition(ctx context.Context, rec PartitionRecord) error
}

type PartitionRecord struct {
	EraID           string
	VersionLabel    string
	Start           uint32
	End             uint32
	Checksums       map[string]string
	RowCounts       map[string]int64
	ProducerVersion string
}

// NewWriter returns a no-op writer placeholder. The real implementation should
// persist lineage and dataset metadata to the DuckLake catalog.
func NewWriter(cfg CatalogConfig) Writer {
	return noopWriter{cfg: cfg}
}

type noopWriter struct {
	cfg CatalogConfig
}

func (n noopWriter) RecordPartition(_ context.Context, _ PartitionRecord) error { return nil }
