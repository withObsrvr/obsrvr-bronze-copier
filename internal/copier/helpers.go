package copier

import (
	"fmt"
	"time"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/metadata"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/storage"
)

// storageRefFromBuilt creates a PartitionRef from a BuiltPartition.
func storageRefFromBuilt(part *BuiltPartition, network string) storage.PartitionRef {
	return storage.PartitionRef{
		Network:      network,
		EraID:        part.EraID,
		VersionLabel: part.VersionLabel,
		Table:        part.Dataset,
		LedgerStart:  part.Range.Start,
		LedgerEnd:    part.Range.End,
	}
}

// buildManifest creates a Manifest from a BuiltPartition.
func buildManifest(part *BuiltPartition, ref storage.PartitionRef) *storage.Manifest {
	tables := make(map[string]storage.TableInfo)

	for tableName := range part.Checksums {
		tables[tableName] = storage.TableInfo{
			File:     fmt.Sprintf("part-%d-%d.parquet", part.Range.Start, part.Range.End),
			Checksum: part.Checksums[tableName],
			RowCount: part.RowCounts[tableName],
			ByteSize: part.ByteSizes[tableName],
		}
	}

	return &storage.Manifest{
		Partition: storage.PartitionInfo{
			Start:        part.Range.Start,
			End:          part.Range.End,
			EraID:        part.EraID,
			VersionLabel: part.VersionLabel,
			Network:      part.Network,
		},
		Tables: tables,
		Producer: storage.ProducerInfo{
			Name:    "bronze-copier",
			Version: Version,
			GitSHA:  GitSHA,
		},
		CreatedAt: time.Now().UTC(),
	}
}

// buildLineageRecord creates a LineageRecord from a BuiltPartition.
func buildLineageRecord(c *Copier, part *BuiltPartition, storageURI, prevHash string) metadata.LineageRecord {
	// Calculate total row count and byte size
	var totalRows int64
	var totalBytes int64
	for tableName := range part.Checksums {
		totalRows += part.RowCounts[tableName]
		totalBytes += part.ByteSizes[tableName]
	}

	storagePath := fmt.Sprintf("%s/%s/%s/%s/range=%d-%d",
		c.cfg.Era.Network, c.cfg.Era.EraID, c.cfg.Era.VersionLabel,
		part.Dataset, part.Range.Start, part.Range.End)

	return metadata.LineageRecord{
		DatasetID:       c.datasetID,
		LedgerStart:     part.Range.Start,
		LedgerEnd:       part.Range.End,
		RowCount:        totalRows,
		ByteSize:        totalBytes,
		Checksum:        part.DataHash,
		PrevHash:        prevHash,
		StoragePath:     storagePath,
		StorageURI:      storageURI,
		ProducerVersion: fmt.Sprintf("bronze-copier@%s", Version),
		ProducerGitSHA:  GitSHA,
		SourceType:      c.cfg.Source.Mode,
		SourceLocation:  c.sourceLocation(),
	}
}

// buildQualityRecord creates a QualityRecord from a BuiltPartition.
func buildQualityRecord(datasetID int64, part *BuiltPartition, passed bool, errMsg string) metadata.QualityRecord {
	return metadata.QualityRecord{
		DatasetID:    datasetID,
		LedgerStart:  part.Range.Start,
		LedgerEnd:    part.Range.End,
		Passed:       passed,
		ErrorMessage: errMsg,
	}
}
