package tables

import (
	"time"
)

// LedgersLCMRawRow represents a single row in the ledgers_lcm_raw table.
// This is the foundational Bronze table that preserves raw LedgerCloseMeta.
type LedgersLCMRawRow struct {
	// Primary identifier
	LedgerSequence uint64 `parquet:"ledger_sequence"`

	// Chain verification fields
	LedgerHash         string `parquet:"ledger_hash"`
	PreviousLedgerHash string `parquet:"previous_ledger_hash"`

	// Temporal fields
	CloseTime time.Time `parquet:"close_time,timestamp(millisecond)"`

	// Protocol information
	ProtocolVersion uint32 `parquet:"protocol_version"`

	// Summary fields (extracted from XDR for queryability)
	TotalCoins     int64  `parquet:"total_coins"`     // stroops
	FeePool        int64  `parquet:"fee_pool"`        // stroops
	TxSetHash      string `parquet:"tx_set_hash"`     // hex-encoded
	TxCount        int32  `parquet:"tx_count"`        // transaction count
	OperationCount int32  `parquet:"operation_count"` // operation count

	// Raw data preservation (THE TRUTH)
	XDRBytes      []byte `parquet:"xdr_bytes"`
	XDRByteLength int32  `parquet:"xdr_byte_length"`
	XDRSHA256     string `parquet:"xdr_sha256"` // hash of xdr_bytes

	// Bronze metadata
	BronzeVersion string `parquet:"bronze_version"` // e.g., "v1"
	EraID         string `parquet:"era_id"`         // e.g., "pre_p23"
	Network       string `parquet:"network"`        // e.g., "pubnet"

	// Ingestion metadata
	IngestedAt time.Time `parquet:"ingested_at,timestamp(millisecond)"`
}

// TableName returns the canonical table name.
func (LedgersLCMRawRow) TableName() string {
	return "ledgers_lcm_raw"
}

// ParquetConfig configures parquet output generation.
type ParquetConfig struct {
	BronzeVersion string // "v1"
	EraID         string // "pre_p23"
	Network       string // "pubnet"
	Compression   string // "snappy" | "zstd" | "none"
}

// DefaultParquetConfig returns sensible defaults.
func DefaultParquetConfig() ParquetConfig {
	return ParquetConfig{
		BronzeVersion: "v1",
		EraID:         "pre_p23",
		Network:       "pubnet",
		Compression:   "snappy",
	}
}

// SchemaVersion returns the version of the schema.
// Increment this when making breaking changes.
const SchemaVersion = "1.0.0"
