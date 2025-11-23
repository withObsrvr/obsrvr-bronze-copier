// Package metrics provides Prometheus metrics for the Bronze Copier.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for the Bronze Copier.
type Metrics struct {
	// Partition metrics
	PartitionsProcessed *prometheus.CounterVec
	PartitionsSkipped   *prometheus.CounterVec
	PartitionsFailed    *prometheus.CounterVec

	// Ledger metrics
	LedgersProcessed *prometheus.CounterVec
	LastLedgerSeq    *prometheus.GaugeVec

	// Timing metrics
	PartitionBuildDuration   *prometheus.HistogramVec
	PartitionUploadDuration  *prometheus.HistogramVec
	PartitionCommitDuration  *prometheus.HistogramVec

	// Size metrics
	PartitionRows  *prometheus.HistogramVec
	PartitionBytes *prometheus.HistogramVec

	// Pipeline metrics
	WorkerQueueDepth     prometheus.Gauge
	SequencerPending     prometheus.Gauge
	InFlightPartitions   prometheus.Gauge

	// Error metrics
	SourceErrors   *prometheus.CounterVec
	StorageErrors  *prometheus.CounterVec
	MetadataErrors *prometheus.CounterVec
	PASErrors      *prometheus.CounterVec
	RetryAttempts  *prometheus.CounterVec

	// Throughput
	LedgersPerSecond prometheus.Gauge
}

// Config holds metrics configuration.
type Config struct {
	Enabled bool
	Address string // Address for metrics HTTP server (e.g., ":9090")
}

var defaultMetrics *Metrics

// Init initializes the metrics package with global metrics.
// Call this once at startup.
func Init(namespace string) *Metrics {
	if namespace == "" {
		namespace = "bronze_copier"
	}

	m := &Metrics{
		PartitionsProcessed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "partitions_processed_total",
				Help:      "Total number of partitions processed",
			},
			[]string{"network", "era_id", "version"},
		),
		PartitionsSkipped: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "partitions_skipped_total",
				Help:      "Total number of partitions skipped (already exist)",
			},
			[]string{"network", "era_id", "version"},
		),
		PartitionsFailed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "partitions_failed_total",
				Help:      "Total number of partitions that failed processing",
			},
			[]string{"network", "era_id", "version"},
		),
		LedgersProcessed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "ledgers_processed_total",
				Help:      "Total number of ledgers processed",
			},
			[]string{"network", "era_id", "version"},
		),
		LastLedgerSeq: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "last_ledger_sequence",
				Help:      "Last committed ledger sequence number",
			},
			[]string{"network", "era_id", "version"},
		),
		PartitionBuildDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "partition_build_duration_seconds",
				Help:      "Time to build a partition (parquet generation)",
				Buckets:   prometheus.ExponentialBuckets(0.1, 2, 10), // 0.1s to ~100s
			},
			[]string{"network", "era_id", "version"},
		),
		PartitionUploadDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "partition_upload_duration_seconds",
				Help:      "Time to upload a partition to storage",
				Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms to ~40s
			},
			[]string{"network", "era_id", "version"},
		),
		PartitionCommitDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "partition_commit_duration_seconds",
				Help:      "Total time to commit a partition (build + upload + metadata)",
				Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s to ~400s
			},
			[]string{"network", "era_id", "version"},
		),
		PartitionRows: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "partition_rows",
				Help:      "Number of rows per partition",
				Buckets:   prometheus.ExponentialBuckets(100, 2, 10), // 100 to ~100k
			},
			[]string{"network", "era_id", "version"},
		),
		PartitionBytes: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "partition_bytes",
				Help:      "Size of partitions in bytes",
				Buckets:   prometheus.ExponentialBuckets(1024, 2, 15), // 1KB to ~32MB
			},
			[]string{"network", "era_id", "version"},
		),
		WorkerQueueDepth: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "worker_queue_depth",
				Help:      "Current number of tasks in the worker queue",
			},
		),
		SequencerPending: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "sequencer_pending",
				Help:      "Number of partitions pending sequencer commit",
			},
		),
		InFlightPartitions: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "in_flight_partitions",
				Help:      "Number of partitions currently being processed",
			},
		),
		SourceErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "source_errors_total",
				Help:      "Total number of source read errors",
			},
			[]string{"network", "source_type"},
		),
		StorageErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "storage_errors_total",
				Help:      "Total number of storage write errors",
			},
			[]string{"network", "backend"},
		),
		MetadataErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "metadata_errors_total",
				Help:      "Total number of metadata catalog errors",
			},
			[]string{"network"},
		),
		PASErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "pas_errors_total",
				Help:      "Total number of PAS emission errors",
			},
			[]string{"network"},
		),
		RetryAttempts: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "retry_attempts_total",
				Help:      "Total number of retry attempts",
			},
			[]string{"network", "operation"},
		),
		LedgersPerSecond: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "ledgers_per_second",
				Help:      "Current ledger processing rate",
			},
		),
	}

	defaultMetrics = m
	return m
}

// Get returns the global metrics instance.
// Returns nil if Init has not been called.
func Get() *Metrics {
	return defaultMetrics
}

// StartServer starts an HTTP server for Prometheus metrics scraping.
// Blocks until the server exits.
func StartServer(address string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	return http.ListenAndServe(address, mux)
}

// Labels is a convenience type for metric labels.
type Labels struct {
	Network    string
	EraID      string
	Version    string
	SourceType string
	Backend    string
	Operation  string
}

// IncPartitionsProcessed increments the partitions processed counter.
func (m *Metrics) IncPartitionsProcessed(l Labels) {
	m.PartitionsProcessed.WithLabelValues(l.Network, l.EraID, l.Version).Inc()
}

// IncPartitionsSkipped increments the partitions skipped counter.
func (m *Metrics) IncPartitionsSkipped(l Labels) {
	m.PartitionsSkipped.WithLabelValues(l.Network, l.EraID, l.Version).Inc()
}

// IncPartitionsFailed increments the partitions failed counter.
func (m *Metrics) IncPartitionsFailed(l Labels) {
	m.PartitionsFailed.WithLabelValues(l.Network, l.EraID, l.Version).Inc()
}

// AddLedgersProcessed adds to the ledgers processed counter.
func (m *Metrics) AddLedgersProcessed(l Labels, count float64) {
	m.LedgersProcessed.WithLabelValues(l.Network, l.EraID, l.Version).Add(count)
}

// SetLastLedgerSeq sets the last committed ledger sequence.
func (m *Metrics) SetLastLedgerSeq(l Labels, seq float64) {
	m.LastLedgerSeq.WithLabelValues(l.Network, l.EraID, l.Version).Set(seq)
}

// ObservePartitionBuildDuration records the partition build time.
func (m *Metrics) ObservePartitionBuildDuration(l Labels, seconds float64) {
	m.PartitionBuildDuration.WithLabelValues(l.Network, l.EraID, l.Version).Observe(seconds)
}

// ObservePartitionUploadDuration records the partition upload time.
func (m *Metrics) ObservePartitionUploadDuration(l Labels, seconds float64) {
	m.PartitionUploadDuration.WithLabelValues(l.Network, l.EraID, l.Version).Observe(seconds)
}

// ObservePartitionCommitDuration records the total partition commit time.
func (m *Metrics) ObservePartitionCommitDuration(l Labels, seconds float64) {
	m.PartitionCommitDuration.WithLabelValues(l.Network, l.EraID, l.Version).Observe(seconds)
}

// ObservePartitionRows records the number of rows in a partition.
func (m *Metrics) ObservePartitionRows(l Labels, rows float64) {
	m.PartitionRows.WithLabelValues(l.Network, l.EraID, l.Version).Observe(rows)
}

// ObservePartitionBytes records the size of a partition in bytes.
func (m *Metrics) ObservePartitionBytes(l Labels, bytes float64) {
	m.PartitionBytes.WithLabelValues(l.Network, l.EraID, l.Version).Observe(bytes)
}

// SetWorkerQueueDepth sets the current worker queue depth.
func (m *Metrics) SetWorkerQueueDepth(depth float64) {
	m.WorkerQueueDepth.Set(depth)
}

// SetSequencerPending sets the number of pending sequencer commits.
func (m *Metrics) SetSequencerPending(pending float64) {
	m.SequencerPending.Set(pending)
}

// SetInFlightPartitions sets the number of in-flight partitions.
func (m *Metrics) SetInFlightPartitions(count float64) {
	m.InFlightPartitions.Set(count)
}

// IncSourceErrors increments the source errors counter.
func (m *Metrics) IncSourceErrors(l Labels) {
	m.SourceErrors.WithLabelValues(l.Network, l.SourceType).Inc()
}

// IncStorageErrors increments the storage errors counter.
func (m *Metrics) IncStorageErrors(l Labels) {
	m.StorageErrors.WithLabelValues(l.Network, l.Backend).Inc()
}

// IncMetadataErrors increments the metadata errors counter.
func (m *Metrics) IncMetadataErrors(l Labels) {
	m.MetadataErrors.WithLabelValues(l.Network).Inc()
}

// IncPASErrors increments the PAS errors counter.
func (m *Metrics) IncPASErrors(l Labels) {
	m.PASErrors.WithLabelValues(l.Network).Inc()
}

// IncRetryAttempts increments the retry attempts counter.
func (m *Metrics) IncRetryAttempts(l Labels) {
	m.RetryAttempts.WithLabelValues(l.Network, l.Operation).Inc()
}

// SetLedgersPerSecond sets the current processing rate.
func (m *Metrics) SetLedgersPerSecond(rate float64) {
	m.LedgersPerSecond.Set(rate)
}
