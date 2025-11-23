package copier

import (
	"context"
	"fmt"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/metadata"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/tables"
)

// ValidationResult contains the outcome of partition validation.
type ValidationResult struct {
	Passed       bool
	Errors       []string
	Warnings     []string
	RowCount     int64
	ByteSize     int64
}

// ValidatePartition performs quality checks on a partition before commit.
// This validates:
// - Row count consistency (ledger count matches expected)
// - Checksum presence for all tables
// - Parquet integrity (non-empty output)
// - Ledger contiguity within partition
func ValidatePartition(part tables.Partition, output *tables.ParquetOutput) ValidationResult {
	result := ValidationResult{
		Passed: true,
	}

	// Check 1: Non-empty partition
	if len(part.Ledgers) == 0 {
		result.Errors = append(result.Errors, "partition has no ledgers")
		result.Passed = false
	}

	// Check 2: Ledger range consistency
	expectedLedgers := int(part.End - part.Start + 1)
	if len(part.Ledgers) != expectedLedgers {
		result.Errors = append(result.Errors,
			fmt.Sprintf("ledger count mismatch: have %d, expected %d (range %d-%d)",
				len(part.Ledgers), expectedLedgers, part.Start, part.End))
		result.Passed = false
	}

	// Check 3: Ledger contiguity
	if len(part.Ledgers) > 0 {
		for i := 1; i < len(part.Ledgers); i++ {
			prev := part.Ledgers[i-1].LedgerSeq
			curr := part.Ledgers[i].LedgerSeq
			if curr != prev+1 {
				result.Errors = append(result.Errors,
					fmt.Sprintf("ledger gap detected: %d -> %d", prev, curr))
				result.Passed = false
			}
		}

		// Check first and last ledger match partition bounds
		firstLedger := part.Ledgers[0].LedgerSeq
		lastLedger := part.Ledgers[len(part.Ledgers)-1].LedgerSeq

		if firstLedger != part.Start {
			result.Errors = append(result.Errors,
				fmt.Sprintf("first ledger %d doesn't match partition start %d", firstLedger, part.Start))
			result.Passed = false
		}
		if lastLedger != part.End {
			result.Errors = append(result.Errors,
				fmt.Sprintf("last ledger %d doesn't match partition end %d", lastLedger, part.End))
			result.Passed = false
		}
	}

	// Parquet output validation
	if output != nil {
		// Check 4: At least one table output
		if len(output.Parquets) == 0 {
			result.Errors = append(result.Errors, "no parquet tables generated")
			result.Passed = false
		}

		// Check 5: All tables have checksums
		for tableName := range output.Parquets {
			if _, ok := output.Checksums[tableName]; !ok {
				result.Errors = append(result.Errors,
					fmt.Sprintf("missing checksum for table %s", tableName))
				result.Passed = false
			}
		}

		// Check 6: Non-empty parquet files
		for tableName, data := range output.Parquets {
			if len(data) == 0 {
				result.Errors = append(result.Errors,
					fmt.Sprintf("empty parquet data for table %s", tableName))
				result.Passed = false
			}
			result.ByteSize += int64(len(data))
		}

		// Check 7: Row counts present
		for tableName := range output.Parquets {
			if count, ok := output.RowCounts[tableName]; ok {
				result.RowCount += count
			} else {
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("missing row count for table %s", tableName))
			}
		}

		// Check 8: Checksum format (should be sha256:...)
		for tableName, checksum := range output.Checksums {
			if len(checksum) < 7 || checksum[:7] != "sha256:" {
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("checksum for %s may be in non-standard format: %s",
						tableName, checksum[:min(20, len(checksum))]))
			}
		}
	} else {
		result.Errors = append(result.Errors, "no parquet output provided")
		result.Passed = false
	}

	return result
}

// RecordQualityResult records the validation result to the metadata catalog.
func RecordQualityResult(ctx context.Context, meta metadata.Writer, datasetID int64, part tables.Partition, result ValidationResult) error {
	if datasetID == 0 {
		return nil // No catalog configured
	}

	errorMsg := ""
	if !result.Passed && len(result.Errors) > 0 {
		for i, err := range result.Errors {
			if i > 0 {
				errorMsg += "; "
			}
			errorMsg += err
		}
	}

	return meta.InsertQuality(ctx, metadata.QualityRecord{
		DatasetID:    datasetID,
		LedgerStart:  part.Start,
		LedgerEnd:    part.End,
		Passed:       result.Passed,
		ErrorMessage: errorMsg,
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
