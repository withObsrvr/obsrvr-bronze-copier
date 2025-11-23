package copier

import (
	"testing"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/source"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/tables"
)

func TestValidatePartition_Valid(t *testing.T) {
	part := tables.Partition{
		Start: 1000,
		End:   1004,
		Ledgers: []source.LedgerCloseMeta{
			{LedgerSeq: 1000},
			{LedgerSeq: 1001},
			{LedgerSeq: 1002},
			{LedgerSeq: 1003},
			{LedgerSeq: 1004},
		},
	}

	output := &tables.ParquetOutput{
		Parquets: map[string][]byte{
			"ledgers_lcm_raw": []byte("fake parquet data"),
		},
		Checksums: map[string]string{
			"ledgers_lcm_raw": "sha256:abc123def456",
		},
		RowCounts: map[string]int64{
			"ledgers_lcm_raw": 5,
		},
	}

	result := ValidatePartition(part, output)

	if !result.Passed {
		t.Errorf("Valid partition should pass. Errors: %v", result.Errors)
	}
	if len(result.Errors) > 0 {
		t.Errorf("No errors expected, got: %v", result.Errors)
	}
}

func TestValidatePartition_EmptyPartition(t *testing.T) {
	part := tables.Partition{
		Start:   1000,
		End:     1004,
		Ledgers: []source.LedgerCloseMeta{},
	}

	output := &tables.ParquetOutput{
		Parquets: map[string][]byte{
			"ledgers_lcm_raw": []byte("data"),
		},
		Checksums: map[string]string{
			"ledgers_lcm_raw": "sha256:abc",
		},
	}

	result := ValidatePartition(part, output)

	if result.Passed {
		t.Error("Empty partition should fail validation")
	}
	if len(result.Errors) == 0 {
		t.Error("Expected error about empty ledgers")
	}
}

func TestValidatePartition_LedgerGap(t *testing.T) {
	part := tables.Partition{
		Start: 1000,
		End:   1004,
		Ledgers: []source.LedgerCloseMeta{
			{LedgerSeq: 1000},
			{LedgerSeq: 1001},
			{LedgerSeq: 1003}, // Gap! Missing 1002
			{LedgerSeq: 1004},
		},
	}

	output := &tables.ParquetOutput{
		Parquets: map[string][]byte{
			"ledgers": []byte("data"),
		},
		Checksums: map[string]string{
			"ledgers": "sha256:abc",
		},
	}

	result := ValidatePartition(part, output)

	if result.Passed {
		t.Error("Partition with gap should fail validation")
	}

	foundGapError := false
	for _, err := range result.Errors {
		if err == "ledger gap detected: 1001 -> 1003" {
			foundGapError = true
			break
		}
	}
	if !foundGapError {
		t.Errorf("Expected gap error, got: %v", result.Errors)
	}
}

func TestValidatePartition_LedgerCountMismatch(t *testing.T) {
	part := tables.Partition{
		Start: 1000,
		End:   1009, // Range of 10 ledgers
		Ledgers: []source.LedgerCloseMeta{
			{LedgerSeq: 1000},
			{LedgerSeq: 1001},
			{LedgerSeq: 1002},
			// Only 3 ledgers, should have 10
		},
	}

	output := &tables.ParquetOutput{
		Parquets: map[string][]byte{
			"ledgers": []byte("data"),
		},
		Checksums: map[string]string{
			"ledgers": "sha256:abc",
		},
	}

	result := ValidatePartition(part, output)

	if result.Passed {
		t.Error("Partition with wrong ledger count should fail")
	}
}

func TestValidatePartition_MissingChecksum(t *testing.T) {
	part := tables.Partition{
		Start: 1000,
		End:   1000,
		Ledgers: []source.LedgerCloseMeta{
			{LedgerSeq: 1000},
		},
	}

	output := &tables.ParquetOutput{
		Parquets: map[string][]byte{
			"ledgers":     []byte("data"),
			"other_table": []byte("data"),
		},
		Checksums: map[string]string{
			"ledgers": "sha256:abc",
			// Missing checksum for other_table
		},
	}

	result := ValidatePartition(part, output)

	if result.Passed {
		t.Error("Partition with missing checksum should fail")
	}

	foundChecksumError := false
	for _, err := range result.Errors {
		if err == "missing checksum for table other_table" {
			foundChecksumError = true
			break
		}
	}
	if !foundChecksumError {
		t.Errorf("Expected missing checksum error, got: %v", result.Errors)
	}
}

func TestValidatePartition_EmptyParquet(t *testing.T) {
	part := tables.Partition{
		Start: 1000,
		End:   1000,
		Ledgers: []source.LedgerCloseMeta{
			{LedgerSeq: 1000},
		},
	}

	output := &tables.ParquetOutput{
		Parquets: map[string][]byte{
			"ledgers": []byte{}, // Empty!
		},
		Checksums: map[string]string{
			"ledgers": "sha256:abc",
		},
	}

	result := ValidatePartition(part, output)

	if result.Passed {
		t.Error("Partition with empty parquet should fail")
	}
}

func TestValidatePartition_NilOutput(t *testing.T) {
	part := tables.Partition{
		Start: 1000,
		End:   1000,
		Ledgers: []source.LedgerCloseMeta{
			{LedgerSeq: 1000},
		},
	}

	result := ValidatePartition(part, nil)

	if result.Passed {
		t.Error("Partition with nil output should fail")
	}
}

func TestValidatePartition_WarningsForNonStandardChecksum(t *testing.T) {
	part := tables.Partition{
		Start: 1000,
		End:   1000,
		Ledgers: []source.LedgerCloseMeta{
			{LedgerSeq: 1000},
		},
	}

	output := &tables.ParquetOutput{
		Parquets: map[string][]byte{
			"ledgers": []byte("data"),
		},
		Checksums: map[string]string{
			"ledgers": "md5:abc123", // Non-standard format
		},
	}

	result := ValidatePartition(part, output)

	// Should pass but with warning
	if !result.Passed {
		t.Errorf("Should pass with warning, got errors: %v", result.Errors)
	}
	if len(result.Warnings) == 0 {
		t.Error("Expected warning about non-standard checksum format")
	}
}
