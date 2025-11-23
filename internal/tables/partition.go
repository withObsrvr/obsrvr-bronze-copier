package tables

import "github.com/withObsrvr/obsrvr-bronze-copier/internal/source"

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

func NewPartitionBuilder(size uint32) *PartitionBuilder {
	return &PartitionBuilder{size: size}
}

func (b *PartitionBuilder) Add(lcm source.LedgerCloseMeta) error {
	b.buf = append(b.buf, lcm)
	return nil
}

func (b *PartitionBuilder) Ready() bool {
	return b.size > 0 && uint32(len(b.buf)) >= b.size
}

func (b *PartitionBuilder) Flush() Partition {
	p := Partition{}
	if len(b.buf) > 0 {
		p.Start = b.buf[0].LedgerSeq
		p.End = b.buf[len(b.buf)-1].LedgerSeq
		p.Ledgers = b.buf
	}
	b.buf = nil
	return p
}

func (p Partition) ToParquet() (map[string][]byte, map[string]string, map[string]int64, error) {
	// TODO: plug in real table writers using DuckDB appender strategy.
	return map[string][]byte{}, map[string]string{}, map[string]int64{}, nil
}
