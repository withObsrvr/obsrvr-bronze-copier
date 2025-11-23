package tables

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/source"
)

// Partition groups a contiguous ledger range with the decoded payloads.
type Partition struct {
	Start   uint32
	End     uint32
	Ledgers []source.LedgerCloseMeta
}

// PartitionBuilder batches ledgers into partitions of a fixed size.
type PartitionBuilder struct {
	size uint32
	buf  []source.LedgerCloseMeta
}

// NewPartitionBuilder creates a new partition builder.
func NewPartitionBuilder(size uint32) *PartitionBuilder {
	return &PartitionBuilder{size: size}
}

// Add adds a ledger to the current partition.
func (b *PartitionBuilder) Add(lcm source.LedgerCloseMeta) error {
	b.buf = append(b.buf, lcm)
	return nil
}

// Ready returns true if the partition has reached target size.
func (b *PartitionBuilder) Ready() bool {
	return b.size > 0 && uint32(len(b.buf)) >= b.size
}

// Flush returns the current partition and resets the buffer.
func (b *PartitionBuilder) Flush() Partition {
	p := Partition{}
	if len(b.buf) > 0 {
		// Sort by ledger sequence to ensure deterministic output
		sort.Slice(b.buf, func(i, j int) bool {
			return b.buf[i].LedgerSeq < b.buf[j].LedgerSeq
		})

		p.Start = b.buf[0].LedgerSeq
		p.End = b.buf[len(b.buf)-1].LedgerSeq
		p.Ledgers = b.buf
	}
	b.buf = nil
	return p
}

// FlushRemaining returns any remaining ledgers as a partial partition.
func (b *PartitionBuilder) FlushRemaining() *Partition {
	if len(b.buf) == 0 {
		return nil
	}
	p := b.Flush()
	return &p
}

// Count returns the number of ledgers currently buffered.
func (b *PartitionBuilder) Count() int {
	return len(b.buf)
}

// ParquetOutput contains the results of parquet generation.
type ParquetOutput struct {
	Parquets  map[string][]byte  // table name -> parquet bytes
	Checksums map[string]string  // table name -> sha256 checksum
	RowCounts map[string]int64   // table name -> row count
}

// ToParquet converts the partition to parquet format.
func (p Partition) ToParquet(cfg ParquetConfig) (*ParquetOutput, error) {
	if len(p.Ledgers) == 0 {
		return &ParquetOutput{
			Parquets:  make(map[string][]byte),
			Checksums: make(map[string]string),
			RowCounts: make(map[string]int64),
		}, nil
	}

	output := &ParquetOutput{
		Parquets:  make(map[string][]byte),
		Checksums: make(map[string]string),
		RowCounts: make(map[string]int64),
	}

	// Generate ledgers_lcm_raw table
	parquetBytes, rowCount, err := p.generateLedgersLCMRaw(cfg)
	if err != nil {
		return nil, fmt.Errorf("generate ledgers_lcm_raw: %w", err)
	}

	tableName := "ledgers_lcm_raw"
	output.Parquets[tableName] = parquetBytes
	output.Checksums[tableName] = ComputeChecksum(parquetBytes)
	output.RowCounts[tableName] = rowCount

	return output, nil
}

// generateLedgersLCMRaw generates the ledgers_lcm_raw parquet file.
func (p Partition) generateLedgersLCMRaw(cfg ParquetConfig) ([]byte, int64, error) {
	extractor := NewExtractor(cfg)
	ingestedAt := time.Now().UTC()

	// Extract rows
	rows := make([]LedgersLCMRawRow, 0, len(p.Ledgers))
	for _, lcm := range p.Ledgers {
		row, err := extractor.ExtractLedgersLCMRaw(&lcm, ingestedAt)
		if err != nil {
			return nil, 0, fmt.Errorf("extract ledger %d: %w", lcm.LedgerSeq, err)
		}
		rows = append(rows, *row)
	}

	// Sort rows by ledger sequence for determinism
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].LedgerSequence < rows[j].LedgerSequence
	})

	// Write to parquet
	buf := new(bytes.Buffer)

	// Configure compression
	var writerOpts []parquet.WriterOption
	switch cfg.Compression {
	case "snappy":
		writerOpts = append(writerOpts, parquet.Compression(&snappy.Codec{}))
	case "zstd":
		writerOpts = append(writerOpts, parquet.Compression(&zstd.Codec{}))
	case "none", "":
		// No compression
	default:
		writerOpts = append(writerOpts, parquet.Compression(&snappy.Codec{}))
	}

	writer := parquet.NewGenericWriter[LedgersLCMRawRow](buf, writerOpts...)

	// Write rows
	_, err := writer.Write(rows)
	if err != nil {
		return nil, 0, fmt.Errorf("write rows: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, 0, fmt.Errorf("close writer: %w", err)
	}

	return buf.Bytes(), int64(len(rows)), nil
}

// ValidateContiguity checks that ledgers are contiguous.
func (p Partition) ValidateContiguity() error {
	if len(p.Ledgers) == 0 {
		return nil
	}

	// Sort first
	sorted := make([]source.LedgerCloseMeta, len(p.Ledgers))
	copy(sorted, p.Ledgers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LedgerSeq < sorted[j].LedgerSeq
	})

	// Check for gaps
	for i := 1; i < len(sorted); i++ {
		if sorted[i].LedgerSeq != sorted[i-1].LedgerSeq+1 {
			return fmt.Errorf("gap detected: ledger %d followed by %d",
				sorted[i-1].LedgerSeq, sorted[i].LedgerSeq)
		}
	}

	return nil
}
