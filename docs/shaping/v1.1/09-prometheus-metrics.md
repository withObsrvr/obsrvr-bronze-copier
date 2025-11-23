# Pitch 09: Prometheus Metrics

**Appetite:** 2 days
**Risk:** Low
**Dependencies:** Pitch 08 (Structured Logging)

---

## Problem

Bronze Copier has no observability metrics:
- Can't monitor throughput in real-time
- Can't alert on backfill stalls
- Can't track error rates
- Can't measure latency distributions

For production operations, we need metrics that integrate with existing monitoring (Prometheus/Grafana).

---

## Solution

### Metrics Endpoint

Add HTTP server with `/metrics` endpoint:

```go
// cmd/bronze-copier/main.go
func main() {
    // Start metrics server in background
    go func() {
        http.Handle("/metrics", promhttp.Handler())
        http.Handle("/health", healthHandler())
        http.ListenAndServe(":9090", nil)
    }()

    // ... rest of main
}
```

### Core Metrics

```go
// internal/metrics/metrics.go

var (
    // Counters
    LedgersProcessed = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "bronze_copier_ledgers_processed_total",
            Help: "Total number of ledgers processed",
        },
        []string{"network", "era_id", "version"},
    )

    PartitionsPublished = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "bronze_copier_partitions_published_total",
            Help: "Total number of partitions published",
        },
        []string{"network", "era_id", "version"},
    )

    PartitionsSkipped = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "bronze_copier_partitions_skipped_total",
            Help: "Partitions skipped (already exist)",
        },
        []string{"network", "era_id", "version"},
    )

    PartitionErrors = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "bronze_copier_partition_errors_total",
            Help: "Partition processing errors",
        },
        []string{"network", "era_id", "version", "error_type"},
    )

    PASEventsEmitted = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "bronze_copier_pas_events_emitted_total",
            Help: "PAS events emitted",
        },
        []string{"network", "era_id", "version"},
    )

    // Gauges
    LastCommittedLedger = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "bronze_copier_last_committed_ledger",
            Help: "Last committed ledger sequence",
        },
        []string{"network", "era_id", "version"},
    )

    WorkerQueueDepth = promauto.NewGauge(
        prometheus.GaugeOpts{
            Name: "bronze_copier_worker_queue_depth",
            Help: "Number of partitions waiting in worker queue",
        },
    )

    ActiveWorkers = promauto.NewGauge(
        prometheus.GaugeOpts{
            Name: "bronze_copier_active_workers",
            Help: "Number of workers currently processing",
        },
    )

    // Histograms
    PartitionDuration = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "bronze_copier_partition_duration_seconds",
            Help:    "Time to process a partition",
            Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
        },
        []string{"network", "era_id", "version"},
    )

    ParquetBytes = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "bronze_copier_parquet_bytes",
            Help:    "Size of generated parquet files",
            Buckets: prometheus.ExponentialBuckets(1024, 4, 10), // 1KB to 256MB
        },
        []string{"network", "era_id", "version", "table"},
    )

    // Summary for throughput
    LedgersPerSecond = promauto.NewGauge(
        prometheus.GaugeOpts{
            Name: "bronze_copier_ledgers_per_second",
            Help: "Current processing rate",
        },
    )
)
```

### Health Endpoint

```go
func healthHandler() http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Check checkpoint age
        // Check last error
        // Return 200 if healthy, 503 if unhealthy
        json.NewEncoder(w).Encode(map[string]interface{}{
            "status": "healthy",
            "last_committed_ledger": lastCommitted,
            "uptime_seconds": uptime,
        })
    })
}
```

---

## Scope

### Must Have
- `/metrics` endpoint with Prometheus format
- `/health` endpoint for liveness probes
- Counters: ledgers, partitions, errors, PAS events
- Gauges: last committed ledger, queue depth
- Histogram: partition duration

### Nice to Have
- `/ready` endpoint for readiness probes
- Parquet size histogram
- Source read latency histogram

### Not Doing
- Push gateway (pull model only)
- Custom dashboards (use Grafana)
- Alerting rules (configure in Prometheus)

---

## Config

```yaml
metrics:
  enabled: true
  port: 9090
  path: "/metrics"
```

Or via environment:
```bash
METRICS_ENABLED=true METRICS_PORT=9090 ./bronze-copier
```

---

## Example Queries

```promql
# Processing rate
rate(bronze_copier_ledgers_processed_total[5m])

# Error rate
rate(bronze_copier_partition_errors_total[5m])

# Backfill progress
bronze_copier_last_committed_ledger{era_id="pre_p23"}

# P99 partition latency
histogram_quantile(0.99, rate(bronze_copier_partition_duration_seconds_bucket[5m]))

# Alert: Backfill stalled (no progress in 30m)
increase(bronze_copier_ledgers_processed_total[30m]) == 0
```

---

## Done When

1. `/metrics` endpoint serves Prometheus format
2. `/health` endpoint returns status
3. All core metrics instrumented
4. Metrics have correct labels
5. Test: Prometheus can scrape metrics endpoint
