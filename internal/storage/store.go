package storage

import "context"

// PartitionRef describes a versioned bronze partition location.
type PartitionRef struct {
	EraID        string
	VersionLabel string
	Table        string
	LedgerStart  uint32
	LedgerEnd    uint32
	Path         string
}

// BronzeStore abstracts writing parquet payloads to storage.
type BronzeStore interface {
	WriteParquet(ctx context.Context, ref PartitionRef, parquetBytes []byte) error
	Exists(ctx context.Context, ref PartitionRef) (bool, error)
}

type StorageConfig struct {
	Backend  string
	Bucket   string
	Prefix   string
	LocalDir string
}

// NewBronzeStore returns a placeholder store implementation that satisfies the
// interface. Storage backends can be implemented later.
func NewBronzeStore(cfg StorageConfig) (BronzeStore, error) {
	return &noopStore{cfg: cfg}, nil
}

type noopStore struct {
	cfg StorageConfig
}

func (n *noopStore) WriteParquet(_ context.Context, _ PartitionRef, _ []byte) error { return nil }

func (n *noopStore) Exists(_ context.Context, _ PartitionRef) (bool, error) { return false, nil }
