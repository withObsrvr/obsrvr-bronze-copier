package copier

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/tables"
)

// ParallelCommitter handles concurrent partition commits while maintaining
// order for operations that require it (like PAS hash chaining).
type ParallelCommitter struct {
	copier      *Copier
	maxInFlight int
	sem         chan struct{}
	mu          sync.Mutex

	// Track in-flight commits for ordered completion
	inFlight    map[uint32]*commitResult
	nextToEmit  uint32
	emitCond    *sync.Cond
}

// commitResult holds the result of a partition commit.
type commitResult struct {
	part    tables.Partition
	output  *tables.ParquetOutput
	err     error
	done    bool
}

// NewParallelCommitter creates a new parallel committer.
func NewParallelCommitter(c *Copier, maxInFlight int) *ParallelCommitter {
	if maxInFlight < 1 {
		maxInFlight = 1
	}
	pc := &ParallelCommitter{
		copier:      c,
		maxInFlight: maxInFlight,
		sem:         make(chan struct{}, maxInFlight),
		inFlight:    make(map[uint32]*commitResult),
	}
	pc.emitCond = sync.NewCond(&pc.mu)
	return pc
}

// CommitAsync starts an asynchronous commit for a partition.
// Returns immediately after the parquet generation and storage write complete.
// PAS emission happens in order via a background goroutine.
func (pc *ParallelCommitter) CommitAsync(ctx context.Context, part tables.Partition) error {
	// Acquire semaphore
	select {
	case pc.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Track this commit
	pc.mu.Lock()
	result := &commitResult{part: part}
	pc.inFlight[part.Start] = result
	pc.mu.Unlock()

	// Start commit in background
	go func() {
		defer func() { <-pc.sem }()

		output, err := pc.commitPartitionParallel(ctx, part)

		pc.mu.Lock()
		result.output = output
		result.err = err
		result.done = true
		pc.emitCond.Broadcast()
		pc.mu.Unlock()
	}()

	return nil
}

// commitPartitionParallel does the parallel-safe parts of commit:
// parquet generation, storage write, catalog write.
// PAS emission is handled separately to maintain ordering.
func (pc *ParallelCommitter) commitPartitionParallel(ctx context.Context, part tables.Partition) (*tables.ParquetOutput, error) {
	log.Printf("[partition:parallel] processing era=%s v=%s range=%d-%d (%d ledgers)",
		pc.copier.cfg.Era.EraID, pc.copier.cfg.Era.VersionLabel, part.Start, part.End, len(part.Ledgers))

	// Generate parquet (CPU-bound, safe to parallelize)
	parquetCfg := tables.ParquetConfig{
		BronzeVersion: pc.copier.cfg.Era.VersionLabel,
		EraID:         pc.copier.cfg.Era.EraID,
		Network:       pc.copier.cfg.Era.Network,
		Compression:   "snappy",
	}

	output, err := part.ToParquet(parquetCfg)
	if err != nil {
		return nil, fmt.Errorf("generate parquet: %w", err)
	}

	// Write to storage (I/O-bound, safe to parallelize)
	if err := pc.copier.writePartitionToStorage(ctx, part, output); err != nil {
		return nil, err
	}

	// Write to catalog (I/O-bound, safe to parallelize)
	pc.copier.recordPartitionMetadata(ctx, part, output)

	return output, nil
}

// WaitAndEmitPAS waits for all in-flight commits to complete
// and emits PAS events in order.
func (pc *ParallelCommitter) WaitAndEmitPAS(ctx context.Context) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for len(pc.inFlight) > 0 {
		// Find the next partition to emit (smallest start ledger)
		var nextStart uint32 = ^uint32(0)
		for start := range pc.inFlight {
			if start < nextStart {
				nextStart = start
			}
		}

		result := pc.inFlight[nextStart]

		// Wait for it to complete
		for !result.done {
			pc.emitCond.Wait()
		}

		// Check for errors
		if result.err != nil {
			return fmt.Errorf("partition %d-%d: %w", result.part.Start, result.part.End, result.err)
		}

		// Emit PAS event (must be sequential for hash chaining)
		if pc.copier.cfg.PAS.Enabled && result.output != nil {
			pc.mu.Unlock()
			if err := pc.copier.emitPASEvent(ctx, result.part, result.output); err != nil {
				pc.mu.Lock()
				return err
			}
			pc.mu.Lock()
		}

		// Update checkpoint
		if pc.copier.checkpoint != nil {
			pc.mu.Unlock()
			pc.copier.saveCheckpoint(ctx, result.part, result.output)
			pc.mu.Lock()
		}

		delete(pc.inFlight, nextStart)
	}

	return nil
}

// Flush waits for all pending commits and returns any error.
func (pc *ParallelCommitter) Flush(ctx context.Context) error {
	return pc.WaitAndEmitPAS(ctx)
}
