package copier

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/checkpoint"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/metadata"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/pas"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/source"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/storage"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/tables"
)

// Version information (set via ldflags)
var (
	Version = "v0.1.0"
	GitSHA  = "unknown"
)

// Copier orchestrates the Bronze copy process.
type Copier struct {
	cfg        config.Config
	src        source.LedgerSource
	store      storage.BronzeStore
	meta       metadata.Writer
	pas        pas.Emitter
	checkpoint checkpoint.Manager
	builder    *tables.PartitionBuilder
	datasetID  int64 // cached dataset ID from catalog
}

// New creates a new Bronze Copier.
func New(cfg config.Config, src source.LedgerSource, store storage.BronzeStore) *Copier {
	// Create checkpoint manager
	cpMgr, err := checkpoint.NewManager(checkpoint.Config{
		Enabled: cfg.Checkpoint.Enabled,
		Dir:     cfg.Checkpoint.Dir,
	})
	if err != nil {
		log.Printf("[copier] warning: failed to create checkpoint manager: %v", err)
		cpMgr = nil
	}

	return &Copier{
		cfg:        cfg,
		src:        src,
		store:      store,
		meta:       metadata.NewWriter(metadata.CatalogConfig(cfg.Catalog)),
		pas:        pas.NewEmitter(cfg.PAS),
		checkpoint: cpMgr,
		builder:    tables.NewPartitionBuilder(cfg.Era.PartitionSize),
	}
}

// Run starts the Bronze copy process.
func (c *Copier) Run(ctx context.Context) error {
	// Register dataset with catalog and get ID for lineage records
	if err := c.ensureDataset(ctx); err != nil {
		log.Printf("[copier] warning: failed to ensure dataset in catalog: %v", err)
		// Continue without catalog - it's optional
	}

	// Determine start ledger (from checkpoint or config)
	startLedger := c.cfg.Era.LedgerStart
	if c.checkpoint != nil {
		cp, err := c.checkpoint.Load(ctx)
		if err != nil && !errors.Is(err, checkpoint.ErrNoCheckpoint) {
			return fmt.Errorf("load checkpoint: %w", err)
		}
		if cp != nil && cp.LastCommittedLedger > 0 {
			// Verify checkpoint matches current config
			if cp.Network == c.cfg.Era.Network &&
				cp.EraID == c.cfg.Era.EraID &&
				cp.VersionLabel == c.cfg.Era.VersionLabel {
				startLedger = cp.LastCommittedLedger + 1
				log.Printf("[copier] resuming from checkpoint: ledger %d", startLedger)
			} else {
				log.Printf("[copier] checkpoint doesn't match config, starting fresh")
			}
		}
	}

	// Override with explicit start if it's higher than checkpoint
	if c.cfg.Era.LedgerStart > startLedger {
		startLedger = c.cfg.Era.LedgerStart
	}

	log.Printf("[copier] starting era=%s version=%s network=%s range=%d-%d partition_size=%d",
		c.cfg.Era.EraID, c.cfg.Era.VersionLabel, c.cfg.Era.Network,
		startLedger, c.cfg.Era.LedgerEnd, c.cfg.Era.PartitionSize)

	// Start streaming ledgers
	ledgersCh, errCh := c.src.Stream(ctx, startLedger, c.cfg.Era.LedgerEnd)

	ledgerCount := 0
	partitionCount := 0
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			// Flush any remaining ledgers
			if remaining := c.builder.FlushRemaining(); remaining != nil {
				log.Printf("[copier] flushing remaining %d ledgers on shutdown", len(remaining.Ledgers))
				if err := c.commitPartition(ctx, *remaining); err != nil {
					log.Printf("[copier] error flushing remaining: %v", err)
				}
			}
			return c.src.Close()

		case err := <-errCh:
			if err != nil {
				return fmt.Errorf("source error: %w", err)
			}
			// Error channel closed, continue

		case lcm, ok := <-ledgersCh:
			if !ok {
				// Stream complete - flush remaining
				if remaining := c.builder.FlushRemaining(); remaining != nil {
					log.Printf("[copier] flushing final partition with %d ledgers", len(remaining.Ledgers))
					if err := c.commitPartition(ctx, *remaining); err != nil {
						return fmt.Errorf("commit final partition: %w", err)
					}
					partitionCount++
				}

				elapsed := time.Since(startTime)
				rate := float64(ledgerCount) / elapsed.Seconds()
				log.Printf("[copier] complete: %d ledgers, %d partitions, %.2f ledgers/sec",
					ledgerCount, partitionCount, rate)
				return nil
			}

			ledgerCount++

			if err := c.builder.Add(lcm); err != nil {
				return fmt.Errorf("add ledger %d: %w", lcm.LedgerSeq, err)
			}

			if c.builder.Ready() {
				part := c.builder.Flush()

				// Validate contiguity
				if err := part.ValidateContiguity(); err != nil {
					return fmt.Errorf("partition validation failed: %w", err)
				}

				if err := c.commitPartition(ctx, part); err != nil {
					return fmt.Errorf("commit partition %d-%d: %w", part.Start, part.End, err)
				}
				partitionCount++

				// Log progress
				elapsed := time.Since(startTime)
				rate := float64(ledgerCount) / elapsed.Seconds()
				log.Printf("[copier] progress: %d ledgers, %d partitions, %.2f ledgers/sec",
					ledgerCount, partitionCount, rate)
			}
		}
	}
}

// sourceLocation returns a string describing the data source for lineage tracking.
func (c *Copier) sourceLocation() string {
	switch c.cfg.Source.Mode {
	case "gcs":
		return fmt.Sprintf("gs://%s/%s", c.cfg.Source.GCSBucket, c.cfg.Source.GCSPrefix)
	case "s3":
		return fmt.Sprintf("s3://%s/%s", c.cfg.Source.S3Bucket, c.cfg.Source.S3Prefix)
	case "local":
		return c.cfg.Source.LocalPath
	case "datastore":
		return c.cfg.Source.DatastorePath
	default:
		return c.cfg.Source.Mode
	}
}

// ensureDataset registers the dataset in the catalog and caches the ID.
func (c *Copier) ensureDataset(ctx context.Context) error {
	datasetID, err := c.meta.EnsureDataset(ctx, metadata.DatasetInfo{
		Domain:      "bronze",
		Dataset:     "ledgers_lcm_raw",
		Version:     c.cfg.Era.VersionLabel,
		EraID:       c.cfg.Era.EraID,
		Network:     c.cfg.Era.Network,
		Description: "Raw Ledger Close Meta (LCM) data from Stellar network",
	})
	if err != nil {
		return err
	}
	c.datasetID = datasetID
	if datasetID > 0 {
		log.Printf("[copier] registered dataset in catalog: id=%d", datasetID)
	}
	return nil
}

// commitPartition writes a partition to storage with all metadata.
func (c *Copier) commitPartition(ctx context.Context, part tables.Partition) error {
	log.Printf("[partition] processing era=%s v=%s range=%d-%d (%d ledgers)",
		c.cfg.Era.EraID, c.cfg.Era.VersionLabel, part.Start, part.End, len(part.Ledgers))

	// Generate parquet
	parquetCfg := tables.ParquetConfig{
		BronzeVersion: c.cfg.Era.VersionLabel,
		EraID:         c.cfg.Era.EraID,
		Network:       c.cfg.Era.Network,
		Compression:   "snappy",
	}

	output, err := part.ToParquet(parquetCfg)
	if err != nil {
		return fmt.Errorf("generate parquet: %w", err)
	}

	// Write each table to storage
	for tableName, parquetBytes := range output.Parquets {
		ref := storage.PartitionRef{
			Network:      c.cfg.Era.Network,
			EraID:        c.cfg.Era.EraID,
			VersionLabel: c.cfg.Era.VersionLabel,
			Table:        tableName,
			LedgerStart:  part.Start,
			LedgerEnd:    part.End,
		}

		// Check if partition already exists
		if exists, _ := c.store.Exists(ctx, ref); exists && !c.cfg.Era.AllowOverwrite {
			return fmt.Errorf("partition already exists: %s range=%d-%d", tableName, part.Start, part.End)
		}

		// Write parquet
		if err := c.store.WriteParquet(ctx, ref, parquetBytes); err != nil {
			return fmt.Errorf("write parquet %s: %w", tableName, err)
		}

		// Write manifest
		manifest := &storage.Manifest{
			Partition: storage.PartitionInfo{
				Start:        part.Start,
				End:          part.End,
				EraID:        c.cfg.Era.EraID,
				VersionLabel: c.cfg.Era.VersionLabel,
				Network:      c.cfg.Era.Network,
			},
			Tables: map[string]storage.TableInfo{
				tableName: {
					File:     fmt.Sprintf("part-%d-%d.parquet", part.Start, part.End),
					Checksum: output.Checksums[tableName],
					RowCount: output.RowCounts[tableName],
					ByteSize: int64(len(parquetBytes)),
				},
			},
			Producer: storage.ProducerInfo{
				Name:    "bronze-copier",
				Version: Version,
				GitSHA:  GitSHA,
			},
			CreatedAt: time.Now().UTC(),
		}

		if err := c.store.WriteManifest(ctx, ref, manifest); err != nil {
			return fmt.Errorf("write manifest %s: %w", tableName, err)
		}

		log.Printf("[partition] wrote %s: %d rows, %d bytes, checksum=%s",
			tableName, output.RowCounts[tableName], len(parquetBytes), output.Checksums[tableName])
	}

	// Record in metadata catalog
	if c.datasetID > 0 {
		// Calculate total byte size
		var totalBytes int64
		for _, parquetBytes := range output.Parquets {
			totalBytes += int64(len(parquetBytes))
		}

		// Build storage path (first table as reference)
		storagePath := fmt.Sprintf("%s/%s/%s/ledgers_lcm_raw/range=%d-%d",
			c.cfg.Era.Network, c.cfg.Era.EraID, c.cfg.Era.VersionLabel, part.Start, part.End)

		if err := c.meta.RecordPartition(ctx, metadata.PartitionRecord{
			DatasetID:       c.datasetID,
			EraID:           c.cfg.Era.EraID,
			VersionLabel:    c.cfg.Era.VersionLabel,
			Start:           part.Start,
			End:             part.End,
			Checksums:       output.Checksums,
			RowCounts:       output.RowCounts,
			ByteSize:        totalBytes,
			StoragePath:     storagePath,
			ProducerVersion: fmt.Sprintf("bronze-copier@%s", Version),
			ProducerGitSHA:  GitSHA,
			SourceType:      c.cfg.Source.Mode,
			SourceLocation:  c.sourceLocation(),
		}); err != nil {
			log.Printf("[partition] warning: failed to record metadata: %v", err)
			// Don't fail the partition for metadata errors
		}
	}

	// Emit PAS event
	if c.cfg.PAS.Enabled {
		// Build storage paths and byte sizes for each table
		storagePaths := make(map[string]string)
		byteSizes := make(map[string]int64)
		for tableName, parquetBytes := range output.Parquets {
			storagePaths[tableName] = fmt.Sprintf("%s/%s/%s/%s/range=%d-%d",
				c.cfg.Era.Network, c.cfg.Era.EraID, c.cfg.Era.VersionLabel, tableName, part.Start, part.End)
			byteSizes[tableName] = int64(len(parquetBytes))
		}

		if err := c.pas.EmitPartition(ctx, pas.Event{
			EraID:        c.cfg.Era.EraID,
			VersionLabel: c.cfg.Era.VersionLabel,
			Network:      c.cfg.Era.Network,
			Start:        part.Start,
			End:          part.End,
			Checksums:    output.Checksums,
			RowCounts:    output.RowCounts,
			ByteSizes:    byteSizes,
			StoragePaths: storagePaths,
			Producer: pas.ProducerInfo{
				Name:    "bronze-copier",
				Version: Version,
				GitSHA:  GitSHA,
			},
		}); err != nil {
			log.Printf("[partition] warning: failed to emit PAS event: %v", err)
			// Don't fail the partition for PAS errors (for now)
		}
	}

	// Update checkpoint
	if c.checkpoint != nil {
		cp := &checkpoint.Checkpoint{
			CopierID:            c.cfg.CopierID,
			Network:             c.cfg.Era.Network,
			EraID:               c.cfg.Era.EraID,
			VersionLabel:        c.cfg.Era.VersionLabel,
			LastCommittedLedger: part.End,
			LastPartition: &checkpoint.PartitionInfo{
				Start:    part.Start,
				End:      part.End,
				Checksum: output.Checksums["ledgers_lcm_raw"],
			},
			UpdatedAt: time.Now().UTC(),
		}
		if err := c.checkpoint.Save(ctx, cp); err != nil {
			log.Printf("[partition] warning: failed to save checkpoint: %v", err)
		}
	}

	return nil
}
