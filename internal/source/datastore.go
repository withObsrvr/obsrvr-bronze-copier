package source

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
)

// DatastoreSource uses the official Stellar datastore package to read ledgers.
// This handles Galexie format archives properly with buffered reading.
type DatastoreSource struct {
	backend ledgerbackend.LedgerBackend
	store   datastore.DataStore
}

// DatastoreConfig holds configuration for the datastore source.
type DatastoreConfig struct {
	// StorageType is either "GCS" or "S3"
	StorageType string

	// BucketPath is the full bucket path (bucket/prefix for GCS, bucket name for S3)
	BucketPath string

	// S3-specific settings
	S3Region        string
	S3Endpoint      string
	S3ForcePathStyle bool

	// Schema settings - these define how ledgers are organized in the archive
	LedgersPerFile    uint32
	FilesPerPartition uint32

	// BufferedStorageBackend settings
	BufferSize uint32
	NumWorkers uint32
	RetryLimit uint32
	RetryWait  time.Duration
}

// DefaultDatastoreConfig returns sensible defaults for Galexie archives.
// The schema values match Galexie's output format:
// - 1 ledger per file
// - 64000 files per partition (range directory)
func DefaultDatastoreConfig() DatastoreConfig {
	return DatastoreConfig{
		LedgersPerFile:    1,     // Galexie uses 1 ledger per file
		FilesPerPartition: 64000, // 64000 files per range directory
		BufferSize:        100,
		NumWorkers:        5,
		RetryLimit:        3,
		RetryWait:         time.Second,
	}
}

// NewDatastoreSource creates a new source using the official Stellar datastore.
func NewDatastoreSource(cfg DatastoreConfig) (*DatastoreSource, error) {
	ctx := context.Background()

	// Create schema - this defines how ledgers are organized
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    cfg.LedgersPerFile,
		FilesPerPartition: cfg.FilesPerPartition,
	}

	// Build datastore config based on storage type
	dsParams := make(map[string]string)
	var dsConfig datastore.DataStoreConfig

	switch cfg.StorageType {
	case "GCS":
		dsParams["destination_bucket_path"] = cfg.BucketPath
		dsConfig = datastore.DataStoreConfig{
			Type:   "GCS",
			Schema: schema,
			Params: dsParams,
		}
	case "S3":
		dsParams["bucket_name"] = cfg.BucketPath
		dsParams["region"] = cfg.S3Region
		if cfg.S3Endpoint != "" {
			dsParams["endpoint"] = cfg.S3Endpoint
		}
		if cfg.S3ForcePathStyle {
			dsParams["force_path_style"] = "true"
		}
		dsConfig = datastore.DataStoreConfig{
			Type:   "S3",
			Schema: schema,
			Params: dsParams,
		}
	default:
		return nil, fmt.Errorf("unsupported storage type: %s (use GCS or S3)", cfg.StorageType)
	}

	// Create the datastore
	log.Printf("[source:datastore] creating datastore type=%s bucket=%s", cfg.StorageType, cfg.BucketPath)

	store, err := datastore.NewDataStore(ctx, dsConfig)
	if err != nil {
		return nil, fmt.Errorf("create datastore: %w", err)
	}

	// Create BufferedStorageBackend configuration
	bufferedConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: cfg.BufferSize,
		NumWorkers: cfg.NumWorkers,
		RetryLimit: cfg.RetryLimit,
		RetryWait:  cfg.RetryWait,
	}

	// Create the BufferedStorageBackend
	backend, err := ledgerbackend.NewBufferedStorageBackend(bufferedConfig, store, schema)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("create buffered storage backend: %w", err)
	}

	log.Printf("[source:datastore] created datastore backend successfully")

	return &DatastoreSource{
		backend: backend,
		store:   store,
	}, nil
}

// Stream implements LedgerSource.Stream using the official backend.
func (s *DatastoreSource) Stream(ctx context.Context, start, end uint32) (<-chan LedgerCloseMeta, <-chan error) {
	ledgerCh := make(chan LedgerCloseMeta, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(ledgerCh)
		defer close(errCh)

		// Prepare the range
		var ledgerRange ledgerbackend.Range
		if end == 0 {
			ledgerRange = ledgerbackend.UnboundedRange(start)
		} else {
			ledgerRange = ledgerbackend.BoundedRange(start, end)
		}

		log.Printf("[source:datastore] preparing range start=%d end=%d", start, end)

		if err := s.backend.PrepareRange(ctx, ledgerRange); err != nil {
			errCh <- fmt.Errorf("prepare range: %w", err)
			return
		}

		log.Printf("[source:datastore] streaming ledgers from %d", start)

		// Stream ledgers
		for seq := start; end == 0 || seq <= end; seq++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Get ledger from backend
			lcm, err := s.backend.GetLedger(ctx, seq)
			if err != nil {
				errCh <- fmt.Errorf("get ledger %d: %w", seq, err)
				return
			}

			// Marshal to bytes for our internal format
			rawXDR, err := lcm.MarshalBinary()
			if err != nil {
				errCh <- fmt.Errorf("marshal ledger %d: %w", seq, err)
				return
			}

			// Extract header info
			ledgerSeq := lcm.LedgerSequence()
			hash := lcm.LedgerHash().HexString()

			var prevHash string
			var closeTime int64
			var protocolVersion uint32

			switch lcm.V {
			case 0:
				header := lcm.MustV0().LedgerHeader.Header
				prevHash = header.PreviousLedgerHash.HexString()
				closeTime = int64(header.ScpValue.CloseTime)
				protocolVersion = uint32(header.LedgerVersion)
			case 1:
				header := lcm.MustV1().LedgerHeader.Header
				prevHash = header.PreviousLedgerHash.HexString()
				closeTime = int64(header.ScpValue.CloseTime)
				protocolVersion = uint32(header.LedgerVersion)
			case 2:
				header := lcm.MustV2().LedgerHeader.Header
				prevHash = header.PreviousLedgerHash.HexString()
				closeTime = int64(header.ScpValue.CloseTime)
				protocolVersion = uint32(header.LedgerVersion)
			}

			result := LedgerCloseMeta{
				LedgerSeq:          ledgerSeq,
				Hash:               hash,
				PreviousLedgerHash: prevHash,
				CloseTime:          closeTime,
				ProtocolVersion:    protocolVersion,
				RawXDR:             rawXDR,
			}

			select {
			case ledgerCh <- result:
			case <-ctx.Done():
				return
			}
		}

		log.Printf("[source:datastore] stream complete")
	}()

	return ledgerCh, errCh
}

// Close releases resources.
func (s *DatastoreSource) Close() error {
	if s.backend != nil {
		s.backend.Close()
	}
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}
