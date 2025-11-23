package source

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// LedgerFile represents a single ledger file in the archive.
type LedgerFile struct {
	Path      string // full path to the file
	LedgerSeq uint32 // extracted ledger sequence number
	RangeDir  string // parent range directory
}

// LedgerIndex maintains an ordered list of ledger files.
type LedgerIndex struct {
	files      []LedgerFile
	bySequence map[uint32]*LedgerFile
}

// NewLedgerIndex creates an empty ledger index.
func NewLedgerIndex() *LedgerIndex {
	return &LedgerIndex{
		files:      make([]LedgerFile, 0),
		bySequence: make(map[uint32]*LedgerFile),
	}
}

// Galexie file naming pattern: {hash_prefix}--{ledger_seq}.xdr.zst
// Example: FC6D1D66--59957913.xdr.zst
var ledgerFilePattern = regexp.MustCompile(`^[A-Fa-f0-9]+--(\d+)\.xdr\.zst$`)

// Range directory pattern: {hash_prefix}--{start}-{end}
// Example: FC6DEFFF--59904000-59967999
var rangeDirPattern = regexp.MustCompile(`^[A-Fa-f0-9]+--(\d+)-(\d+)$`)

// ParseLedgerFilename extracts the ledger sequence from a Galexie filename.
func ParseLedgerFilename(filename string) (uint32, bool) {
	base := filepath.Base(filename)
	matches := ledgerFilePattern.FindStringSubmatch(base)
	if matches == nil {
		return 0, false
	}

	seq, err := strconv.ParseUint(matches[1], 10, 32)
	if err != nil {
		return 0, false
	}

	return uint32(seq), true
}

// ParseRangeDirectory extracts start and end ledger from a range directory name.
func ParseRangeDirectory(dirname string) (start, end uint32, ok bool) {
	base := filepath.Base(dirname)
	matches := rangeDirPattern.FindStringSubmatch(base)
	if matches == nil {
		return 0, 0, false
	}

	startVal, err := strconv.ParseUint(matches[1], 10, 32)
	if err != nil {
		return 0, 0, false
	}

	endVal, err := strconv.ParseUint(matches[2], 10, 32)
	if err != nil {
		return 0, 0, false
	}

	return uint32(startVal), uint32(endVal), true
}

// AddFile adds a file to the index if it matches the expected pattern.
func (idx *LedgerIndex) AddFile(path string) bool {
	seq, ok := ParseLedgerFilename(path)
	if !ok {
		return false
	}

	// Extract range directory from path
	rangeDir := filepath.Dir(path)

	file := LedgerFile{
		Path:      path,
		LedgerSeq: seq,
		RangeDir:  rangeDir,
	}

	idx.files = append(idx.files, file)
	idx.bySequence[seq] = &idx.files[len(idx.files)-1]
	return true
}

// Sort orders files by ledger sequence.
func (idx *LedgerIndex) Sort() {
	sort.Slice(idx.files, func(i, j int) bool {
		return idx.files[i].LedgerSeq < idx.files[j].LedgerSeq
	})
}

// GetRange returns files within the specified ledger range (inclusive).
// If end is 0, returns all files from start onwards.
func (idx *LedgerIndex) GetRange(start, end uint32) []LedgerFile {
	idx.Sort()

	result := make([]LedgerFile, 0)
	for _, f := range idx.files {
		if f.LedgerSeq < start {
			continue
		}
		if end > 0 && f.LedgerSeq > end {
			break
		}
		result = append(result, f)
	}
	return result
}

// GetFile returns a specific ledger file by sequence.
func (idx *LedgerIndex) GetFile(seq uint32) (*LedgerFile, bool) {
	f, ok := idx.bySequence[seq]
	return f, ok
}

// Count returns the total number of indexed files.
func (idx *LedgerIndex) Count() int {
	return len(idx.files)
}

// ValidateContiguity checks that all ledgers in a range are present.
// Returns the first missing ledger sequence if a gap is found.
func (idx *LedgerIndex) ValidateContiguity(start, end uint32) (uint32, error) {
	files := idx.GetRange(start, end)
	if len(files) == 0 {
		return start, fmt.Errorf("%w: no files found in range %d-%d", ErrLedgerGap, start, end)
	}

	expectedSeq := start
	for _, f := range files {
		if f.LedgerSeq != expectedSeq {
			return expectedSeq, fmt.Errorf("%w: expected ledger %d, found %d", ErrLedgerGap, expectedSeq, f.LedgerSeq)
		}
		expectedSeq++
	}

	// Check we have all ledgers up to end
	if end > 0 && expectedSeq <= end {
		return expectedSeq, fmt.Errorf("%w: missing ledgers from %d to %d", ErrLedgerGap, expectedSeq, end)
	}

	return 0, nil
}

// MinSequence returns the minimum ledger sequence in the index.
func (idx *LedgerIndex) MinSequence() uint32 {
	if len(idx.files) == 0 {
		return 0
	}
	idx.Sort()
	return idx.files[0].LedgerSeq
}

// MaxSequence returns the maximum ledger sequence in the index.
func (idx *LedgerIndex) MaxSequence() uint32 {
	if len(idx.files) == 0 {
		return 0
	}
	idx.Sort()
	return idx.files[len(idx.files)-1].LedgerSeq
}

// IsXDRFile checks if a path looks like an XDR file.
func IsXDRFile(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".xdr.zst") || strings.HasSuffix(lower, ".xdr")
}

// IsCompressed checks if a file is zstd compressed.
func IsCompressed(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".zst")
}
