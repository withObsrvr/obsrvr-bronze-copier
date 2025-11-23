package copier

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/source"
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

	log.Printf("[pipeline] starting backfill: %d partitions, %d workers", len(ranges), p.workers)

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
	log.Printf("[worker:%d] processing range [%d-%d] (attempt %d)",
		workerID, task.Range.Start, task.Range.End, task.Attempt+1)

	startTime := time.Now()

	// Build the partition from source
	part, err := p.buildPartition(ctx, task.Range)
	if err != nil {
		// Retry logic
		if task.Attempt < task.MaxRetry-1 {
			log.Printf("[worker:%d] retrying range [%d-%d] after error: %v",
				workerID, task.Range.Start, task.Range.End, err)

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
	log.Printf("[worker:%d] built partition [%d-%d] in %v (%d rows)",
		workerID, task.Range.Start, task.Range.End, elapsed, part.RowCount)

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
				log.Printf("[sequencer] committed %d/%d partitions (%.2f/sec)",
					totalCommitted, len(expected), rate)
			}
		}
	}

	return nil
}

// commitPartition writes a built partition to storage and commits metadata/PAS.
func (p *Pipeline) commitPartition(ctx context.Context, part *BuiltPartition) error {
	c := p.copier

	log.Printf("[sequencer] committing partition [%d-%d]", part.Range.Start, part.Range.End)

	// Step 1: Idempotency check
	if c.datasetID > 0 {
		exists, err := c.meta.PartitionExists(ctx, c.datasetID, part.Range.Start, part.Range.End)
		if err != nil {
			log.Printf("[sequencer] warning: idempotency check failed: %v", err)
		} else if exists {
			log.Printf("[sequencer] skipping partition [%d-%d] (already committed)", part.Range.Start, part.Range.End)
			return nil
		}
	}

	// Step 2: Build storage ref
	ref := storageRefFromBuilt(part, c.cfg.Era.Network)

	// Step 3: Check storage existence
	if exists, _ := c.store.Exists(ctx, ref); exists && !c.cfg.Era.AllowOverwrite {
		log.Printf("[sequencer] skipping partition [%d-%d] (exists in storage)", part.Range.Start, part.Range.End)
		return nil
	}

	// Step 4: Write parquet to storage
	if err := c.store.WriteParquet(ctx, ref, part.ParquetBytes); err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}

	// Step 5: Write manifest
	manifest := buildManifest(part, ref)
	if err := c.store.WriteManifest(ctx, ref, manifest); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Derive storage URI
	storageKey := ref.Path("")
	storageURI := c.store.URI(storageKey)

	// Step 6: Get previous hash for chaining
	var prevHash string
	if c.datasetID > 0 {
		prev, err := c.meta.GetLastLineage(ctx, c.datasetID)
		if err != nil {
			log.Printf("[sequencer] warning: failed to get last lineage: %v", err)
		} else if prev != nil {
			prevHash = prev.Checksum
		}
	}

	// Step 7: Record lineage (new interface)
	if c.datasetID > 0 {
		lineageRec := buildLineageRecord(c, part, storageURI, prevHash)
		if err := c.meta.InsertLineage(ctx, lineageRec); err != nil {
			log.Printf("[sequencer] warning: failed to insert lineage: %v", err)
		}

		// Record quality pass
		if err := c.meta.InsertQuality(ctx, buildQualityRecord(c.datasetID, part, true, "")); err != nil {
			log.Printf("[sequencer] warning: failed to insert quality: %v", err)
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
			log.Printf("[sequencer] warning: failed to emit PAS event: %v", err)
		}
	}

	// Step 9: Update checkpoint
	c.saveCheckpoint(ctx, tables.Partition{
		Start: part.Range.Start,
		End:   part.Range.End,
	}, &tables.ParquetOutput{
		Checksums: part.Checksums,
	})

	log.Printf("[sequencer] committed partition [%d-%d] hash=%s prev=%s",
		part.Range.Start, part.Range.End, part.DataHash, prevHash)

	return nil
}
