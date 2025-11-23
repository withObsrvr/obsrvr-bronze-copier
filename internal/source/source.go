package source

import (
	"context"
	"errors"
	"fmt"
)

// LedgerCloseMeta represents the raw ledger close metadata payload.
type LedgerCloseMeta struct {
	LedgerSeq          uint32
	Hash               string // hex-encoded ledger hash
	PreviousLedgerHash string // hex-encoded previous ledger hash
	CloseTime          int64  // unix timestamp
	ProtocolVersion    uint32
	RawXDR             []byte // preserved exactly as read
}

// LedgerSource streams ledger close meta from a live or archive source.
type LedgerSource interface {
	// Stream returns channels for ledger data and errors.
	// If end is 0, stream continues indefinitely (tailing mode).
	Stream(ctx context.Context, start uint32, end uint32) (<-chan LedgerCloseMeta, <-chan error)
	Close() error
}

// SourceConfig configures the ledger source.
type SourceConfig struct {
	Mode string // "local" | "gcs" | "s3" | "datastore"

	// Local filesystem
	LocalPath string // /path/to/archive/

	// GCS (legacy direct GCS access - consider using datastore mode instead)
	GCSBucket string // bucket name (without gs://)
	GCSPrefix string // e.g., "landing/ledgers/pubnet/"

	// S3 (also works for B2, R2, MinIO)
	S3Bucket   string
	S3Prefix   string
	S3Endpoint string // custom endpoint for B2/MinIO/R2
	S3Region   string

	// Datastore mode - uses official Stellar datastore package (recommended for Galexie archives)
	DatastoreType string // "GCS" or "S3"
	DatastorePath string // Full bucket path (e.g., "bucket-name/landing/ledgers/testnet")
}

var (
	ErrInvalidSourceMode = errors.New("invalid source mode: must be 'local', 'gcs', or 's3'")
	ErrMissingConfig     = errors.New("missing required configuration")
	ErrLedgerNotFound    = errors.New("ledger not found in archive")
	ErrLedgerGap         = errors.New("gap detected in ledger sequence")
	ErrInvalidXDR        = errors.New("invalid XDR data")
)

// NewLedgerSource constructs a ledger source based on the configured mode.
func NewLedgerSource(cfg SourceConfig) (LedgerSource, error) {
	switch cfg.Mode {
	case "local":
		if cfg.LocalPath == "" {
			return nil, fmt.Errorf("%w: LocalPath required for local mode", ErrMissingConfig)
		}
		return NewLocalSource(cfg.LocalPath)
	case "gcs":
		if cfg.GCSBucket == "" {
			return nil, fmt.Errorf("%w: GCSBucket required for gcs mode", ErrMissingConfig)
		}
		return NewGCSSource(cfg.GCSBucket, cfg.GCSPrefix)
	case "s3":
		if cfg.S3Bucket == "" {
			return nil, fmt.Errorf("%w: S3Bucket required for s3 mode", ErrMissingConfig)
		}
		return NewS3Source(cfg.S3Bucket, cfg.S3Prefix, cfg.S3Endpoint, cfg.S3Region)
	case "datastore":
		if cfg.DatastoreType == "" || cfg.DatastorePath == "" {
			return nil, fmt.Errorf("%w: DatastoreType and DatastorePath required for datastore mode", ErrMissingConfig)
		}
		dsCfg := DefaultDatastoreConfig()
		dsCfg.StorageType = cfg.DatastoreType
		dsCfg.BucketPath = cfg.DatastorePath
		dsCfg.S3Region = cfg.S3Region
		dsCfg.S3Endpoint = cfg.S3Endpoint
		return NewDatastoreSource(dsCfg)
	default:
		return nil, ErrInvalidSourceMode
	}
}
