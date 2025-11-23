# Pitch 07: Worker Pool + Sequencer

**Appetite:** 3 days
**Risk:** Medium
**Dependencies:** None

---

## Problem

The current `ParallelCommitter` works but has architectural issues:

1. **Implicit sequencing:** PAS ordering happens via condition variables in `WaitAndEmitPAS`, but this pattern is fragile and hard to reason about.

2. **No bounded queue:** Workers can pile up without backpressure.

3. **No per-partition retry:** If a partition fails, the whole batch fails.

4. **Mixed concerns:** Parquet generation, storage writes, metadata, PAS emission all happen in ad-hoc order.

The review specifically called out:
> "You can parallelize parquet build/upload, but PAS + lineage must be committed in canonical order. That implies a 'sequencer' stage."

---

## Solution

### Three-Stage Pipeline

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Dispatcher │───▶│   Workers   │───▶│  Sequencer  │
│  (1 thread) │    │  (N threads)│    │  (1 thread) │
└─────────────┘    └─────────────┘    └─────────────┘
      │                   │                   │
   creates            parallel            sequential
   partition          parquet +           metadata +
   tasks              storage             PAS + checkpoint
```

### Stage 1: Dispatcher

Single goroutine that:
- Receives ledgers from source
- Builds partitions using PartitionBuilder
- Sends partition tasks to worker queue
- Tracks next expected ledger for gap detection

### Stage 2: Worker Pool

N worker goroutines that:
- Pull from bounded work queue
- Generate parquet (CPU-bound)
- Write to storage (I/O-bound)
- Send results to sequencer queue
- Retry on transient failures with exponential backoff

```go
type WorkerPool struct {
    workers    int
    workQueue  chan PartitionTask
    resultChan chan PartitionResult
    wg         sync.WaitGroup
}

type PartitionTask struct {
    Partition tables.Partition
    Attempt   int
    MaxRetry  int
}

type PartitionResult struct {
    Task      PartitionTask
    Output    *tables.ParquetOutput
    StorageRef storage.PartitionRef
    Err       error
}
```

### Stage 3: Sequencer

Single goroutine that:
- Receives results from workers (may be out of order)
- Buffers results until next-in-sequence is ready
- Commits metadata to catalog (in order)
- Emits PAS events (in order - maintains hash chain)
- Updates checkpoint (in order)

```go
type Sequencer struct {
    resultChan   chan PartitionResult
    buffer       map[uint32]*PartitionResult  // keyed by ledger_start
    nextExpected uint32
    copier       *Copier
}

func (s *Sequencer) Run(ctx context.Context) error {
    for {
        select {
        case result := <-s.resultChan:
            s.buffer[result.Task.Partition.Start] = &result
            s.tryFlush(ctx)
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}

func (s *Sequencer) tryFlush(ctx context.Context) {
    for {
        result, ok := s.buffer[s.nextExpected]
        if !ok {
            return // waiting for next partition
        }

        // Commit in order: metadata -> PAS -> checkpoint
        s.commitSequential(ctx, result)

        delete(s.buffer, s.nextExpected)
        s.nextExpected = result.Task.Partition.End + 1
    }
}
```

---

## Scope

### Must Have
- Dispatcher stage with partition creation
- Worker pool with configurable size
- Bounded work queue with backpressure
- Sequencer for ordered commits
- Per-partition retry (3 attempts, exponential backoff)

### Nice to Have
- Metrics: queue depth, worker utilization, retry count
- Configurable queue size
- Graceful shutdown with drain

### Not Doing
- Dynamic worker scaling
- Priority queues
- Dead letter queue for failed partitions

---

## Rabbit Holes

**Don't:**
- Use channels for everything (hard to debug)
- Try to make workers stateful
- Over-engineer the retry logic

**Do:**
- Use simple bounded channels
- Keep workers stateless
- Log clearly on retry/failure

---

## Config

```yaml
perf:
  workers: 4                    # Number of parallel workers
  queue_size: 8                 # Max pending partitions in work queue
  retry_attempts: 3             # Max retries per partition
  retry_backoff_ms: 1000        # Initial backoff (doubles each retry)
```

---

## Done When

1. Three-stage pipeline implemented
2. Workers process partitions in parallel
3. Sequencer commits PAS events in ledger order
4. Per-partition retry works
5. Backpressure prevents unbounded queue growth
6. Test: Out-of-order worker completion still produces ordered PAS chain
