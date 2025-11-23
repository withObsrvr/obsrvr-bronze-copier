package copier

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/logging"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/metrics"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/source"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/storage"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/tables"
)

// Pipeline implements the dispatcher → workers → sequencer flow.
// Workers can build and upload partitions in parallel,
// but the sequencer commits them in ledger order.
type Pipeline struct {
	copier      *Copier
	workers     int
	queueSize   int
	maxRetry    int
	backoffMs   int
	log         *slog.Logger

	workQueue   chan PartitionTask
	resultChan  chan PartitionResult
	wg          sync.WaitGroup
}

// NewPipeline creates a new worker pipeline.
func NewPipeline(c *Copier, workers, queueSize, maxRetry, backoffMs int) *Pipeline {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = workers * 2
	}
	if maxRetry < 1 {
		maxRetry = 3
	}
	if backoffMs < 100 {
		backoffMs = 1000
	}

	return &Pipeline{
		copier:     c,
		workers:    workers,
		queueSize:  queueSize,
		maxRetry:   maxRetry,
		backoffMs:  backoffMs,
		log:        slog.With("component", "pipeline"),
		workQueue:  make(chan PartitionTask, queueSize),
		resultChan: make(chan PartitionResult, queueSize),
	}
}

// RunBackfill processes a range of ledgers using the pipeline.
func (p *Pipeline) RunBackfill(ctx context.Context, start, end uint32) error {
	ranges := p.makeRanges(start, end, p.copier.cfg.Era.PartitionSize)
	if len(ranges) == 0 {
		return nil
	}

	p.log.Info("starting backfill", "partitions", len(ranges), "workers", p.workers)

	// Start worker pool
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.workerLoop(ctx, i)
	}

	// Start dispatcher
	errChan := make(chan error, 1)
	go func() {
		errChan <- p.dispatcherLoop(ctx, ranges)
	}()

	// Close results when workers finish
	go func() {
		p.wg.Wait()
		close(p.resultChan)
	}()

	// Sequencer: commit in order
	if err := p.sequencerLoop(ctx, ranges); err != nil {
		return err
	}

	// Check dispatcher error
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// makeRanges creates a slice of LedgerRange with indices for ordering.
func (p *Pipeline) makeRanges(start, end, size uint32) []LedgerRange {
	var out []LedgerRange
	var idx int64 = 0
	for s := start; s <= end; {
		e := s + size - 1
		if e > end {
			e = end
		}
		out = append(out, LedgerRange{
			Start: s,
			End:   e,
			Index: idx,
		})
		idx++
		s = e + 1
	}
	return out
}

// dispatcherLoop sends partition tasks to workers.
func (p *Pipeline) dispatcherLoop(ctx context.Context, ranges []LedgerRange) error {
	defer close(p.workQueue)

	for _, r := range ranges {
		task := PartitionTask{
			Range:    r,
			Attempt:  0,
			MaxRetry: p.maxRetry,
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.workQueue <- task:
			// Task dispatched
		}
	}

	return nil
}

// workerLoop processes partition tasks.
func (p *Pipeline) workerLoop(ctx context.Context, workerID int) {
	defer p.wg.Done()

	for task := range p.workQueue {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result := p.processTask(ctx, workerID, task)
		p.resultChan <- result
	}
}

// processTask builds a partition and uploads to storage.
// Does NOT finalize or commit - that's the sequencer's job.
func (p *Pipeline) processTask(ctx context.Context, workerID int, task PartitionTask) PartitionResult {
	correlationID := logging.GenerateCorrelationID()
	log := logging.WorkerLogger(workerID).With(
		"correlation_id", correlationID,
		"ledger_start", task.Range.Start,
		"ledger_end", task.Range.End,
	)

	log.Info("processing partition", "attempt", task.Attempt+1)

	startTime := time.Now()

	// Build the partition from source
	part, err := p.buildPartition(ctx, task.Range)
	if err != nil {
		// Retry logic
		if task.Attempt < task.MaxRetry-1 {
			log.Warn("partition build failed, retrying", "error", err)

			// Record retry metric
			if m := metrics.Get(); m != nil {
				labels := p.getMetricsLabels()
				labels.Operation = "partition_build"
				m.IncRetryAttempts(labels)
			}

			// Exponential backoff
			backoff := time.Duration(p.backoffMs*(1<<task.Attempt)) * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return PartitionResult{Task: task, Err: ctx.Err()}
			}

			task.Attempt++
			return p.processTask(ctx, workerID, task)
		}

		return PartitionResult{
			Task: task,
			Err:  fmt.Errorf("failed after %d attempts: %w", task.Attempt+1, err),
		}
	}

	elapsed := time.Since(startTime)
	log.Info("partition built", "duration_ms", elapsed.Milliseconds(), "rows", part.RowCount)

	// Record metrics
	if m := metrics.Get(); m != nil {
		labels := p.getMetricsLabels()
		m.ObservePartitionBuildDuration(labels, elapsed.Seconds())
		m.ObservePartitionRows(labels, float64(part.RowCount))
		if part.ByteSizes != nil {
			var totalBytes int64
			for _, b := range part.ByteSizes {
				totalBytes += b
			}
			m.ObservePartitionBytes(labels, float64(totalBytes))
		}
	}

	return PartitionResult{
		Task:      task,
		Partition: part,
		Err:       nil,
	}
}

// buildPartition builds a single partition from the source.
func (p *Pipeline) buildPartition(ctx context.Context, r LedgerRange) (*BuiltPartition, error) {
	// Stream ledgers for this range
	ledgersCh, errCh := p.copier.src.Stream(ctx, r.Start, r.End)

	// Collect ledgers
	var ledgers []source.LedgerCloseMeta
	for {
		select {
		case err := <-errCh:
			if err != nil {
				return nil, fmt.Errorf("stream error: %w", err)
			}
		case lcm, ok := <-ledgersCh:
			if !ok {
				// Stream complete
				goto doneStreaming
			}
			ledgers = append(ledgers, lcm)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

doneStreaming:
	if len(ledgers) == 0 {
		return nil, fmt.Errorf("no ledgers received for range [%d-%d]", r.Start, r.End)
	}

	// Create partition
	part := tables.Partition{
		Start:   r.Start,
		End:     r.End,
		Ledgers: ledgers,
	}

	// Validate contiguity
	if err := part.ValidateContiguity(); err != nil {
		return nil, fmt.Errorf("contiguity validation: %w", err)
	}

	// Generate parquet
	parquetCfg := tables.ParquetConfig{
		BronzeVersion: p.copier.cfg.Era.VersionLabel,
		EraID:         p.copier.cfg.Era.EraID,
		Network:       p.copier.cfg.Era.Network,
		Compression:   "snappy",
	}

	output, err := part.ToParquet(parquetCfg)
	if err != nil {
		return nil, fmt.Errorf("generate parquet: %w", err)
	}

	// Build result
	buildID := uuid.New().String()
	builtPart := &BuiltPartition{
		Range:        r,
		Dataset:      "ledgers_lcm_raw",
		EraID:        p.copier.cfg.Era.EraID,
		VersionLabel: p.copier.cfg.Era.VersionLabel,
		Network:      p.copier.cfg.Era.Network,
		BuildID:      buildID,
		BuiltAt:      time.Now().UTC(),
		Checksums:    output.Checksums,
		RowCounts:    output.RowCounts,
	}

	// Calculate totals
	var totalRows int64
	var totalBytes int64
	byteSizes := make(map[string]int64)
	for tableName, data := range output.Parquets {
		builtPart.ParquetBytes = data // Store the parquet data
		totalRows += output.RowCounts[tableName]
		totalBytes += int64(len(data))
		byteSizes[tableName] = int64(len(data))
	}
	builtPart.RowCount = totalRows
	builtPart.ByteSizes = byteSizes

	// Use primary checksum (ledgers_lcm_raw)
	if cs, ok := output.Checksums["ledgers_lcm_raw"]; ok {
		builtPart.DataHash = cs
	}

	return builtPart, nil
}

// sequencerLoop commits partitions in ledger order.
func (p *Pipeline) sequencerLoop(ctx context.Context, expected []LedgerRange) error {
	if len(expected) == 0 {
		return nil
	}

	// Build expected index map
	expectByIndex := make(map[int64]LedgerRange, len(expected))
	for _, r := range expected {
		expectByIndex[r.Index] = r
	}
	nextIndex := expected[0].Index
	lastIndex := expected[len(expected)-1].Index

	// Buffer for out-of-order results
	pending := make(map[int64]*PartitionResult)
	totalCommitted := 0
	startTime := time.Now()

	for nextIndex <= lastIndex {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case result, ok := <-p.resultChan:
			if !ok {
				// Results closed early
				if nextIndex <= lastIndex {
					return fmt.Errorf("results closed before all partitions committed, nextIndex=%d", nextIndex)
				}
				return nil
			}

			// Check for errors
			if result.Err != nil {
				return fmt.Errorf("partition [%d-%d]: %w",
					result.Task.Range.Start, result.Task.Range.End, result.Err)
			}

			// Buffer the result
			pending[result.Task.Range.Index] = &result

			// Flush in-order as far as possible
			for {
				r, ok := pending[nextIndex]
				if !ok {
					break
				}

				// Commit this partition (finalize + metadata + PAS)
				if err := p.commitPartition(ctx, r.Partition); err != nil {
					return fmt.Errorf("commit partition [%d-%d]: %w",
						r.Partition.Range.Start, r.Partition.Range.End, err)
				}

				delete(pending, nextIndex)
				nextIndex++
				totalCommitted++

				// Progress log
				elapsed := time.Since(startTime)
				rate := float64(totalCommitted) / elapsed.Seconds()
				p.log.Info("sequencer progress",
					"committed", totalCommitted,
					"total", len(expected),
					"rate_per_sec", fmt.Sprintf("%.2f", rate),
				)
			}
		}
	}

	return nil
}

// commitPartition writes a built partition to storage and commits metadata/PAS.
func (p *Pipeline) commitPartition(ctx context.Context, part *BuiltPartition) error {
	c := p.copier
	log := p.log.With("ledger_start", part.Range.Start, "ledger_end", part.Range.End)

	log.Debug("committing partition")

	// Step 1: Idempotency check
	if c.datasetID > 0 {
		exists, err := c.meta.PartitionExists(ctx, c.datasetID, part.Range.Start, part.Range.End)
		if err != nil {
			log.Warn("idempotency check failed", "error", err)
		} else if exists {
			log.Info("skipping partition (already committed)")
			if m := metrics.Get(); m != nil {
				m.IncPartitionsSkipped(p.getMetricsLabels())
			}
			return nil
		}
	}

	// Step 2: Build storage ref
	ref := storageRefFromBuilt(part, c.cfg.Era.Network)

	// Step 3: Check storage existence
	if exists, _ := c.store.Exists(ctx, ref); exists && !c.cfg.Era.AllowOverwrite {
		log.Info("skipping partition (exists in storage)")
		if m := metrics.Get(); m != nil {
			m.IncPartitionsSkipped(p.getMetricsLabels())
		}
		return nil
	}

	// Step 4 & 5: Write parquet and manifest to storage
	manifest := buildManifest(part, ref)

	// Check if store supports atomic writes
	atomicStore := storage.AsAtomic(c.store)
	if atomicStore != nil {
		// Atomic path: temp -> finalize
		if err := p.writeAtomic(ctx, atomicStore, ref, part.ParquetBytes, manifest); err != nil {
			return fmt.Errorf("atomic write: %w", err)
		}
	} else {
		// Fallback: direct write (non-atomic)
		if err := c.store.WriteParquet(ctx, ref, part.ParquetBytes); err != nil {
			return fmt.Errorf("write parquet: %w", err)
		}
		if err := c.store.WriteManifest(ctx, ref, manifest); err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	}

	// Derive storage URI
	storageKey := ref.Path("")
	storageURI := c.store.URI(storageKey)

	// Step 6: Get previous hash for chaining
	var prevHash string
	if c.datasetID > 0 {
		prev, err := c.meta.GetLastLineage(ctx, c.datasetID)
		if err != nil {
			log.Warn("failed to get last lineage", "error", err)
		} else if prev != nil {
			prevHash = prev.Checksum
		}
	}

	// Step 7: Record lineage (new interface)
	if c.datasetID > 0 {
		lineageRec := buildLineageRecord(c, part, storageURI, prevHash)
		if err := c.meta.InsertLineage(ctx, lineageRec); err != nil {
			log.Warn("failed to insert lineage", "error", err)
		}

		// Record quality pass
		if err := c.meta.InsertQuality(ctx, buildQualityRecord(c.datasetID, part, true, "")); err != nil {
			log.Warn("failed to insert quality", "error", err)
		}
	}

	// Step 8: Emit PAS event
	if c.cfg.PAS.Enabled {
		if err := c.emitPASEvent(ctx, tables.Partition{
			Start: part.Range.Start,
			End:   part.Range.End,
		}, &tables.ParquetOutput{
			Parquets:  map[string][]byte{"ledgers_lcm_raw": part.ParquetBytes},
			Checksums: part.Checksums,
			RowCounts: part.RowCounts,
		}); err != nil {
			log.Warn("failed to emit PAS event", "error", err)
		}
	}

	// Step 9: Update checkpoint
	c.saveCheckpoint(ctx, tables.Partition{
		Start: part.Range.Start,
		End:   part.Range.End,
	}, &tables.ParquetOutput{
		Checksums: part.Checksums,
	})

	log.Info("committed partition", "hash", part.DataHash, "prev_hash", prevHash)

	// Record commit metrics
	if m := metrics.Get(); m != nil {
		labels := p.getMetricsLabels()
		m.IncPartitionsProcessed(labels)
		ledgerCount := float64(part.Range.End - part.Range.Start + 1)
		m.AddLedgersProcessed(labels, ledgerCount)
		m.SetLastLedgerSeq(labels, float64(part.Range.End))
	}

	return nil
}

// getMetricsLabels returns the standard metric labels for this pipeline.
func (p *Pipeline) getMetricsLabels() metrics.Labels {
	return metrics.Labels{
		Network: p.copier.cfg.Era.Network,
		EraID:   p.copier.cfg.Era.EraID,
		Version: p.copier.cfg.Era.VersionLabel,
	}
}

// writeAtomic writes parquet and manifest files atomically using temp files.
// If any step fails, all temp files are cleaned up.
func (p *Pipeline) writeAtomic(ctx context.Context, store storage.AtomicStore, ref storage.PartitionRef, parquetData []byte, manifest *storage.Manifest) error {
	var tempKeys []string

	// Write parquet to temp location
	tempParquet, err := store.WriteParquetTemp(ctx, ref, parquetData)
	if err != nil {
		return fmt.Errorf("write parquet temp: %w", err)
	}
	tempKeys = append(tempKeys, tempParquet)

	// Write manifest to temp location
	tempManifest, err := store.WriteManifestTemp(ctx, ref, manifest)
	if err != nil {
		store.Abort(ctx, tempKeys)
		return fmt.Errorf("write manifest temp: %w", err)
	}
	tempKeys = append(tempKeys, tempManifest)

	// Atomic finalize - moves all temp files to final locations
	if err := store.Finalize(ctx, ref, tempKeys); err != nil {
		// Finalize handles its own cleanup on failure
		return fmt.Errorf("finalize: %w", err)
	}

	return nil
}
