package copier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	datasetID  int64       // cached dataset ID from catalog
	log        *slog.Logger // structured logger
}

// New creates a new Bronze Copier.
func New(cfg config.Config, src source.LedgerSource, store storage.BronzeStore) *Copier {
	log := slog.With("component", "copier")

	// Create checkpoint manager
	cpMgr, err := checkpoint.NewManager(checkpoint.Config{
		Enabled: cfg.Checkpoint.Enabled,
		Dir:     cfg.Checkpoint.Dir,
	})
	if err != nil {
		log.Warn("failed to create checkpoint manager", "error", err)
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
		log:        log,
	}
}

// Run starts the Bronze copy process.
func (c *Copier) Run(ctx context.Context) error {
	// Register dataset with catalog and get ID for lineage records
	if err := c.ensureDataset(ctx); err != nil {
		c.log.Warn("failed to ensure dataset in catalog", "error", err)
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
				c.log.Info("resuming from checkpoint", "start_ledger", startLedger)
			} else {
				c.log.Info("checkpoint doesn't match config, starting fresh")
			}
		}
	}

	// Override with explicit start if it's higher than checkpoint
	if c.cfg.Era.LedgerStart > startLedger {
		startLedger = c.cfg.Era.LedgerStart
	}

	// Use parallel mode if configured
	maxInFlight := c.cfg.Perf.MaxInFlightPartitions
	if maxInFlight > 1 {
		return c.runParallel(ctx, startLedger, maxInFlight)
	}

	return c.runSequential(ctx, startLedger)
}

// runSequential runs the copier in sequential mode (one partition at a time).
func (c *Copier) runSequential(ctx context.Context, startLedger uint32) error {
	c.log.Info("starting sequential mode",
		"era_id", c.cfg.Era.EraID,
		"version", c.cfg.Era.VersionLabel,
		"network", c.cfg.Era.Network,
		"ledger_start", startLedger,
		"ledger_end", c.cfg.Era.LedgerEnd,
		"partition_size", c.cfg.Era.PartitionSize,
	)

	// Start streaming ledgers
	ledgersCh, errCh := c.src.Stream(ctx, startLedger, c.cfg.Era.LedgerEnd)

	ledgerCount := 0
	partitionCount := 0
	skippedCount := 0
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			// Flush any remaining ledgers
			if remaining := c.builder.FlushRemaining(); remaining != nil {
				c.log.Info("flushing remaining ledgers on shutdown", "count", len(remaining.Ledgers))
				if _, err := c.publishPartition(ctx, *remaining); err != nil && !errors.Is(err, ErrPartitionExists) {
					c.log.Error("error flushing remaining", "error", err)
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
					c.log.Info("flushing final partition", "ledger_count", len(remaining.Ledgers))
					if _, err := c.publishPartition(ctx, *remaining); err != nil {
						if errors.Is(err, ErrPartitionExists) {
							skippedCount++
						} else {
							return fmt.Errorf("publish final partition: %w", err)
						}
					} else {
						partitionCount++
					}
				}

				elapsed := time.Since(startTime)
				rate := float64(ledgerCount) / elapsed.Seconds()
				c.log.Info("sequential run complete",
					"ledgers", ledgerCount,
					"partitions", partitionCount,
					"skipped", skippedCount,
					"rate_per_sec", fmt.Sprintf("%.2f", rate),
					"duration", elapsed.String(),
				)
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

				// Use publishPartition for transactional lifecycle
				if _, err := c.publishPartition(ctx, part); err != nil {
					if errors.Is(err, ErrPartitionExists) {
						// Partition already exists - skip (idempotent)
						skippedCount++
					} else {
						return fmt.Errorf("publish partition %d-%d: %w", part.Start, part.End, err)
					}
				} else {
					partitionCount++
				}

				// Log progress
				elapsed := time.Since(startTime)
				rate := float64(ledgerCount) / elapsed.Seconds()
				c.log.Info("progress",
					"ledgers", ledgerCount,
					"partitions", partitionCount,
					"skipped", skippedCount,
					"rate_per_sec", fmt.Sprintf("%.2f", rate),
				)
			}
		}
	}
}

// runParallel runs the copier with the worker pool pipeline.
// Uses dispatcher → workers → sequencer pattern for parallel processing
// with ordered commits.
func (c *Copier) runParallel(ctx context.Context, startLedger uint32, maxInFlight int) error {
	c.log.Info("starting pipeline mode",
		"workers", c.cfg.Perf.Workers,
		"era_id", c.cfg.Era.EraID,
		"version", c.cfg.Era.VersionLabel,
		"network", c.cfg.Era.Network,
		"ledger_start", startLedger,
		"ledger_end", c.cfg.Era.LedgerEnd,
		"partition_size", c.cfg.Era.PartitionSize,
	)

	// Use the new pipeline architecture
	pipeline := NewPipeline(
		c,
		c.cfg.Perf.Workers,
		c.cfg.Perf.QueueSize,
		c.cfg.Perf.RetryAttempts,
		c.cfg.Perf.RetryBackoffMs,
	)

	return pipeline.RunBackfill(ctx, startLedger, c.cfg.Era.LedgerEnd)
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
	primaryTable := c.cfg.Bronze.PrimaryTable
	datasetID, err := c.meta.EnsureDataset(ctx, metadata.DatasetInfo{
		Domain:      "bronze",
		Dataset:     primaryTable,
		Version:     c.cfg.Era.VersionLabel,
		EraID:       c.cfg.Era.EraID,
		Network:     c.cfg.Era.Network,
		Description: fmt.Sprintf("Raw %s data from Stellar network", primaryTable),
	})
	if err != nil {
		return err
	}
	c.datasetID = datasetID
	if datasetID > 0 {
		c.log.Info("registered dataset in catalog", "dataset_id", datasetID, "table", primaryTable)
	}
	return nil
}

// commitPartition writes a partition to storage with all metadata.
func (c *Copier) commitPartition(ctx context.Context, part tables.Partition) error {
	c.log.Debug("processing partition",
		"era_id", c.cfg.Era.EraID,
		"version", c.cfg.Era.VersionLabel,
		"ledger_start", part.Start,
		"ledger_end", part.End,
		"ledger_count", len(part.Ledgers),
	)

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

		c.log.Debug("wrote table",
			"table", tableName,
			"rows", output.RowCounts[tableName],
			"bytes", len(parquetBytes),
			"checksum", output.Checksums[tableName],
		)
	}

	// Record in metadata catalog
	if c.datasetID > 0 {
		// Calculate total byte size
		var totalBytes int64
		for _, parquetBytes := range output.Parquets {
			totalBytes += int64(len(parquetBytes))
		}

		// Build storage path (primary table as reference)
		primaryTable := c.cfg.Bronze.PrimaryTable
		storagePath := fmt.Sprintf("%s/%s/%s/%s/range=%d-%d",
			c.cfg.Era.Network, c.cfg.Era.EraID, c.cfg.Era.VersionLabel, primaryTable, part.Start, part.End)

		if err := c.meta.RecordPartition(ctx, metadata.PartitionRecord{
			DatasetID:       c.datasetID,
			EraID:           c.cfg.Era.EraID,
			VersionLabel:    c.cfg.Era.VersionLabel,
			Start:           part.Start,
			End:             part.End,
			Checksums:       output.Checksums,
			RowCounts:       output.RowCounts,
			PrimaryTable:    primaryTable,
			ByteSize:        totalBytes,
			StoragePath:     storagePath,
			ProducerVersion: fmt.Sprintf("bronze-copier@%s", Version),
			ProducerGitSHA:  GitSHA,
			SourceType:      c.cfg.Source.Mode,
			SourceLocation:  c.sourceLocation(),
		}); err != nil {
			c.log.Warn("failed to record metadata", "error", err)
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
			c.log.Warn("failed to emit PAS event", "error", err)
			// Don't fail the partition for PAS errors (for now)
		}
	}

	// Update checkpoint
	if c.checkpoint != nil {
		primaryTable := c.cfg.Bronze.PrimaryTable
		cp := &checkpoint.Checkpoint{
			CopierID:            c.cfg.CopierID,
			Network:             c.cfg.Era.Network,
			EraID:               c.cfg.Era.EraID,
			VersionLabel:        c.cfg.Era.VersionLabel,
			LastCommittedLedger: part.End,
			LastPartition: &checkpoint.PartitionInfo{
				Start:    part.Start,
				End:      part.End,
				Checksum: output.Checksums[primaryTable],
			},
			UpdatedAt: time.Now().UTC(),
		}
		if err := c.checkpoint.Save(ctx, cp); err != nil {
			c.log.Warn("failed to save checkpoint", "error", err)
		}
	}

	return nil
}

// writePartitionToStorage writes the parquet data and manifest to storage.
// This is safe to call concurrently.
func (c *Copier) writePartitionToStorage(ctx context.Context, part tables.Partition, output *tables.ParquetOutput) error {
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

		c.log.Debug("wrote table",
			"table", tableName,
			"rows", output.RowCounts[tableName],
			"bytes", len(parquetBytes),
			"checksum", output.Checksums[tableName],
		)
	}
	return nil
}

// recordPartitionMetadata records the partition in the catalog.
// This is safe to call concurrently.
func (c *Copier) recordPartitionMetadata(ctx context.Context, part tables.Partition, output *tables.ParquetOutput) {
	if c.datasetID == 0 {
		return
	}

	// Calculate total byte size
	var totalBytes int64
	for _, parquetBytes := range output.Parquets {
		totalBytes += int64(len(parquetBytes))
	}

	// Build storage path
	primaryTable := c.cfg.Bronze.PrimaryTable
	storagePath := fmt.Sprintf("%s/%s/%s/%s/range=%d-%d",
		c.cfg.Era.Network, c.cfg.Era.EraID, c.cfg.Era.VersionLabel, primaryTable, part.Start, part.End)

	if err := c.meta.RecordPartition(ctx, metadata.PartitionRecord{
		DatasetID:       c.datasetID,
		EraID:           c.cfg.Era.EraID,
		VersionLabel:    c.cfg.Era.VersionLabel,
		Start:           part.Start,
		End:             part.End,
		Checksums:       output.Checksums,
		RowCounts:       output.RowCounts,
		PrimaryTable:    primaryTable,
		ByteSize:        totalBytes,
		StoragePath:     storagePath,
		ProducerVersion: fmt.Sprintf("bronze-copier@%s", Version),
		ProducerGitSHA:  GitSHA,
		SourceType:      c.cfg.Source.Mode,
		SourceLocation:  c.sourceLocation(),
	}); err != nil {
		c.log.Warn("failed to record metadata", "error", err)
	}
}

// emitPASEvent emits a PAS event for a partition.
// This must be called sequentially to maintain hash chain ordering.
func (c *Copier) emitPASEvent(ctx context.Context, part tables.Partition, output *tables.ParquetOutput) error {
	// Build storage paths and byte sizes for each table
	storagePaths := make(map[string]string)
	byteSizes := make(map[string]int64)
	for tableName, parquetBytes := range output.Parquets {
		storagePaths[tableName] = fmt.Sprintf("%s/%s/%s/%s/range=%d-%d",
			c.cfg.Era.Network, c.cfg.Era.EraID, c.cfg.Era.VersionLabel, tableName, part.Start, part.End)
		byteSizes[tableName] = int64(len(parquetBytes))
	}

	return c.pas.EmitPartition(ctx, pas.Event{
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
	})
}

// saveCheckpoint saves the checkpoint for a partition.
// This should be called sequentially after PAS emission.
func (c *Copier) saveCheckpoint(ctx context.Context, part tables.Partition, output *tables.ParquetOutput) {
	if c.checkpoint == nil {
		return
	}

	primaryTable := c.cfg.Bronze.PrimaryTable
	cp := &checkpoint.Checkpoint{
		CopierID:            c.cfg.CopierID,
		Network:             c.cfg.Era.Network,
		EraID:               c.cfg.Era.EraID,
		VersionLabel:        c.cfg.Era.VersionLabel,
		LastCommittedLedger: part.End,
		LastPartition: &checkpoint.PartitionInfo{
			Start:    part.Start,
			End:      part.End,
			Checksum: output.Checksums[primaryTable],
		},
		UpdatedAt: time.Now().UTC(),
	}
	if err := c.checkpoint.Save(ctx, cp); err != nil {
		c.log.Warn("failed to save checkpoint", "error", err)
	}
}
