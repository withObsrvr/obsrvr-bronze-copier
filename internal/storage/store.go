package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// PartitionRef describes a versioned bronze partition location.
type PartitionRef struct {
	Network      string // "pubnet" | "testnet"
	EraID        string // "pre_p23" | "p23_plus"
	VersionLabel string // "v1" | "v2"
	Table        string // "ledgers_lcm_raw"
	LedgerStart  uint32
	LedgerEnd    uint32
}

// Path returns the storage path for this partition's parquet file.
func (r PartitionRef) Path(prefix string) string {
	return fmt.Sprintf("%s%s/%s/%s/%s/range=%d-%d/part-%d-%d.parquet",
		prefix, r.Network, r.EraID, r.VersionLabel, r.Table,
		r.LedgerStart, r.LedgerEnd, r.LedgerStart, r.LedgerEnd)
}

// ManifestPath returns the storage path for this partition's manifest.
func (r PartitionRef) ManifestPath(prefix string) string {
	return fmt.Sprintf("%s%s/%s/%s/%s/range=%d-%d/_manifest.json",
		prefix, r.Network, r.EraID, r.VersionLabel, r.Table,
		r.LedgerStart, r.LedgerEnd)
}

// DirPath returns the directory path for this partition.
func (r PartitionRef) DirPath(prefix string) string {
	return fmt.Sprintf("%s%s/%s/%s/%s/range=%d-%d",
		prefix, r.Network, r.EraID, r.VersionLabel, r.Table,
		r.LedgerStart, r.LedgerEnd)
}

// Manifest describes the contents of a partition directory.
type Manifest struct {
	Partition PartitionInfo        `json:"partition"`
	Tables    map[string]TableInfo `json:"tables"`
	Producer  ProducerInfo         `json:"producer"`
	CreatedAt time.Time            `json:"created_at"`
}

// PartitionInfo describes the partition boundaries.
type PartitionInfo struct {
	Start        uint32 `json:"start"`
	End          uint32 `json:"end"`
	EraID        string `json:"era_id"`
	VersionLabel string `json:"version"`
	Network      string `json:"network"`
}

// TableInfo describes a single table in the partition.
type TableInfo struct {
	File     string `json:"file"`
	Checksum string `json:"checksum"`
	RowCount int64  `json:"row_count"`
	ByteSize int64  `json:"byte_size"`
}

// ProducerInfo describes the software that produced the partition.
type ProducerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	GitSHA  string `json:"git_sha,omitempty"`
}

// MarshalJSON returns the manifest as JSON bytes.
func (m *Manifest) MarshalJSON() ([]byte, error) {
	type Alias Manifest
	return json.MarshalIndent((*Alias)(m), "", "  ")
}

// BronzeStore abstracts writing parquet payloads to storage.
type BronzeStore interface {
	// WriteParquet writes parquet bytes to storage.
	WriteParquet(ctx context.Context, ref PartitionRef, parquetBytes []byte) error

	// WriteManifest writes a manifest file to storage.
	WriteManifest(ctx context.Context, ref PartitionRef, manifest *Manifest) error

	// Exists checks if a partition already exists.
	Exists(ctx context.Context, ref PartitionRef) (bool, error)

	// URI returns the canonical URI for the given key.
	// For local: file:///path, GCS: gs://bucket/path, S3: s3://bucket/path
	URI(key string) string

	// Close releases any resources.
	Close() error
}

// AtomicStore extends BronzeStore with atomic publish capabilities.
// This is the preferred interface for production use.
type AtomicStore interface {
	BronzeStore

	// WriteParquetTemp writes parquet bytes to a temporary location.
	// Returns the temp key that can be passed to Finalize.
	WriteParquetTemp(ctx context.Context, ref PartitionRef, parquetBytes []byte) (tempKey string, err error)

	// WriteManifestTemp writes a manifest to a temporary location.
	WriteManifestTemp(ctx context.Context, ref PartitionRef, manifest *Manifest) (tempKey string, err error)

	// Finalize atomically moves temp files to their canonical location.
	// For object stores this is copy+delete; for local filesystem it's rename.
	// If any file fails to finalize, all should be rolled back.
	Finalize(ctx context.Context, ref PartitionRef, tempKeys []string) error

	// Abort removes temporary files without publishing.
	Abort(ctx context.Context, tempKeys []string) error

	// Head returns metadata about a stored object (size, checksum if available).
	Head(ctx context.Context, key string) (*ObjectInfo, error)

	// List returns all keys with the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)
}

// ObjectInfo contains metadata about a stored object.
type ObjectInfo struct {
	Key      string
	Size     int64
	ETag     string // MD5 for S3/GCS, empty for local
	ModTime  time.Time
}

// PublishResult contains the result of an atomic publish operation.
type PublishResult struct {
	ParquetKey  string
	ManifestKey string
	Checksum    string
	ByteSize    int64
}

// StorageConfig configures the storage backend.
type StorageConfig struct {
	Backend string // "local" | "gcs" | "s3"

	// Local filesystem
	LocalDir string // /path/to/bronze/

	// GCS
	GCSBucket string

	// S3 (also works for B2, R2, MinIO)
	S3Bucket   string
	S3Endpoint string // custom endpoint for B2/MinIO/R2
	S3Region   string

	// Common
	Prefix string // "bronze/" (path prefix within bucket or local dir)
}

// NewBronzeStore creates a storage backend based on configuration.
func NewBronzeStore(cfg StorageConfig) (BronzeStore, error) {
	switch cfg.Backend {
	case "local":
		if cfg.LocalDir == "" {
			return nil, fmt.Errorf("LocalDir required for local backend")
		}
		return NewLocalStore(cfg.LocalDir, cfg.Prefix)
	case "gcs":
		if cfg.GCSBucket == "" {
			return nil, fmt.Errorf("GCSBucket required for gcs backend")
		}
		return NewGCSStore(cfg.GCSBucket, cfg.Prefix)
	case "s3":
		if cfg.S3Bucket == "" {
			return nil, fmt.Errorf("S3Bucket required for s3 backend")
		}
		return NewS3Store(cfg.S3Bucket, cfg.Prefix, cfg.S3Endpoint, cfg.S3Region)
	default:
		return nil, fmt.Errorf("unknown storage backend: %s", cfg.Backend)
	}
}

// NewAtomicStore creates an atomic storage backend based on configuration.
// All supported backends (local, gcs, s3) implement AtomicStore.
func NewAtomicStore(cfg StorageConfig) (AtomicStore, error) {
	switch cfg.Backend {
	case "local":
		if cfg.LocalDir == "" {
			return nil, fmt.Errorf("LocalDir required for local backend")
		}
		return NewLocalStore(cfg.LocalDir, cfg.Prefix)
	case "gcs":
		if cfg.GCSBucket == "" {
			return nil, fmt.Errorf("GCSBucket required for gcs backend")
		}
		return NewGCSStore(cfg.GCSBucket, cfg.Prefix)
	case "s3":
		if cfg.S3Bucket == "" {
			return nil, fmt.Errorf("S3Bucket required for s3 backend")
		}
		return NewS3Store(cfg.S3Bucket, cfg.Prefix, cfg.S3Endpoint, cfg.S3Region)
	default:
		return nil, fmt.Errorf("unknown storage backend: %s", cfg.Backend)
	}
}

// AsAtomic attempts to cast a BronzeStore to AtomicStore.
// Returns nil if the store doesn't support atomic operations.
func AsAtomic(store BronzeStore) AtomicStore {
	if atomic, ok := store.(AtomicStore); ok {
		return atomic
	}
	return nil
}
