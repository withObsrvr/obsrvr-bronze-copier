package copier

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/checkpoint"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/metadata"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/pas"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/storage"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/tables"
)

// PublishResult contains the outcome of a successful partition publish.
type PublishResult struct {
	Part       tables.Partition
	Output     *tables.ParquetOutput
	StorageRef storage.PartitionRef
	Published  time.Time
}

// publishPartition is the transactional lifecycle for committing a partition.
//
// The order of operations is critical and must not be changed:
//  1. Check idempotency (skip if already committed)
//  2. Generate parquet in memory
//  3. Compute checksums
//  4. Write to storage (temp -> finalize if supported)
//  5. Write manifest
//  6. Record metadata in catalog (lineage)
//  7. Emit PAS event (must be last - references immutable storage)
//  8. Update checkpoint
//
// If any step fails, earlier steps should be rolled back or idempotent.
func (c *Copier) publishPartition(ctx context.Context, part tables.Partition) (*PublishResult, error) {
	log.Printf("[publish] starting partition %d-%d", part.Start, part.End)
	startTime := time.Now()

	// Step 1: Idempotency check - skip if already committed
	if c.datasetID > 0 {
		exists, err := c.meta.PartitionExists(ctx, c.datasetID, part.Start, part.End)
		if err != nil {
			log.Printf("[publish] warning: idempotency check failed: %v", err)
			// Continue anyway - we'll catch duplicates at storage level
		} else if exists {
			log.Printf("[publish] skipping partition %d-%d (already committed)", part.Start, part.End)
			return nil, ErrPartitionExists
		}
	}

	// Step 2: Check storage for existing partition
	ref := storage.PartitionRef{
		Network:      c.cfg.Era.Network,
		EraID:        c.cfg.Era.EraID,
		VersionLabel: c.cfg.Era.VersionLabel,
		Table:        "ledgers_lcm_raw",
		LedgerStart:  part.Start,
		LedgerEnd:    part.End,
	}

	if exists, _ := c.store.Exists(ctx, ref); exists && !c.cfg.Era.AllowOverwrite {
		log.Printf("[publish] skipping partition %d-%d (exists in storage)", part.Start, part.End)
		return nil, ErrPartitionExists
	}

	// Step 3: Generate parquet
	parquetCfg := tables.ParquetConfig{
		BronzeVersion: c.cfg.Era.VersionLabel,
		EraID:         c.cfg.Era.EraID,
		Network:       c.cfg.Era.Network,
		Compression:   "snappy",
	}

	output, err := part.ToParquet(parquetCfg)
	if err != nil {
		return nil, fmt.Errorf("generate parquet: %w", err)
	}

	// Step 4: Write parquet to storage
	// TODO: Use AtomicStore if available for temp -> finalize
	for tableName, parquetBytes := range output.Parquets {
		tableRef := ref
		tableRef.Table = tableName

		if err := c.store.WriteParquet(ctx, tableRef, parquetBytes); err != nil {
			return nil, fmt.Errorf("write parquet %s: %w", tableName, err)
		}

		// Step 5: Write manifest
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

		if err := c.store.WriteManifest(ctx, tableRef, manifest); err != nil {
			return nil, fmt.Errorf("write manifest %s: %w", tableName, err)
		}

		log.Printf("[publish] wrote %s: %d rows, %d bytes, checksum=%s",
			tableName, output.RowCounts[tableName], len(parquetBytes), output.Checksums[tableName])
	}

	// Step 6: Record in metadata catalog (lineage)
	if c.datasetID > 0 {
		var totalBytes int64
		for _, parquetBytes := range output.Parquets {
			totalBytes += int64(len(parquetBytes))
		}

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
			// Log but don't fail - metadata is important but not critical
			log.Printf("[publish] warning: failed to record metadata: %v", err)
		}
	}

	// Step 7: Emit PAS event
	// This MUST be after storage and metadata are committed
	// because PAS references the immutable, published data
	if c.cfg.PAS.Enabled {
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
			// PAS failure is serious but not fatal
			// The partition is already committed - log and continue
			log.Printf("[publish] warning: failed to emit PAS event: %v", err)
		}
	}

	// Step 8: Update checkpoint
	// This is the last step - only checkpoint after everything else succeeded
	c.updateCheckpoint(ctx, part, output)

	elapsed := time.Since(startTime)
	log.Printf("[publish] completed partition %d-%d in %v", part.Start, part.End, elapsed)

	return &PublishResult{
		Part:       part,
		Output:     output,
		StorageRef: ref,
		Published:  time.Now(),
	}, nil
}

// updateCheckpoint saves the checkpoint after a successful publish.
func (c *Copier) updateCheckpoint(ctx context.Context, part tables.Partition, output *tables.ParquetOutput) {
	if c.checkpoint == nil {
		return
	}

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
		log.Printf("[publish] warning: failed to save checkpoint: %v", err)
	}
}

// ErrPartitionExists is returned when a partition already exists and overwrite is disabled.
var ErrPartitionExists = fmt.Errorf("partition already exists")
