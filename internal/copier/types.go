package copier

import (
	"time"
)

// LedgerRange is a unit of work for a Bronze partition.
// Index provides monotonic ordering for the sequencer.
type LedgerRange struct {
	Start uint32
	End   uint32
	Index int64 // monotonically increasing job index (for ordering)
}

// BuiltPartition is the local build artifact before publishing.
// Workers produce these; the sequencer consumes them.
type BuiltPartition struct {
	Range         LedgerRange
	Dataset       string
	EraID         string
	VersionLabel  string
	Network       string
	SchemaHash    string
	LocalParquet  string            // temp local file path
	LocalManifest string            // temp local manifest path
	ParquetBytes  []byte            // in-memory parquet data
	ManifestBytes []byte            // in-memory manifest data
	RowCount      int64             // total rows across tables
	DataHash      string            // sha256 over canonical bytes
	TempObjectKey string            // backend temp key after upload
	ObjectKey     string            // canonical key
	StorageURI    string            // canonical URI after finalize
	BuildID       string            // uuid for traceability
	BuiltAt       time.Time         // when partition was built
	Checksums     map[string]string // per-table checksums
	RowCounts     map[string]int64  // per-table row counts
	ByteSizes     map[string]int64  // per-table byte sizes
}

// PartitionTask is sent to workers for processing.
type PartitionTask struct {
	Range    LedgerRange
	Attempt  int // retry count
	MaxRetry int // max retries before failure
}

// PartitionResult is returned from workers to the sequencer.
type PartitionResult struct {
	Task      PartitionTask
	Partition *BuiltPartition
	Err       error
}
