package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLocalStoreAtomicOperations(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "bronze-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewLocalStore(tmpDir, "bronze/")
	if err != nil {
		t.Fatalf("NewLocalStore failed: %v", err)
	}

	ctx := context.Background()
	ref := PartitionRef{
		Network:      "testnet",
		EraID:        "pre_p23",
		VersionLabel: "v1",
		Table:        "ledgers_lcm_raw",
		LedgerStart:  1000,
		LedgerEnd:    1009,
	}

	// Test data
	parquetData := []byte("fake parquet data for testing")
	manifest := &Manifest{
		Partition: PartitionInfo{
			Start:        1000,
			End:          1009,
			EraID:        "pre_p23",
			VersionLabel: "v1",
			Network:      "testnet",
		},
		Tables: map[string]TableInfo{
			"ledgers_lcm_raw": {
				File:     "part-1000-1009.parquet",
				Checksum: "sha256:abc123",
				RowCount: 10,
				ByteSize: int64(len(parquetData)),
			},
		},
		Producer: ProducerInfo{
			Name:    "bronze-copier",
			Version: "test",
		},
		CreatedAt: time.Now(),
	}

	// Test WriteParquetTemp
	tempParquet, err := store.WriteParquetTemp(ctx, ref, parquetData)
	if err != nil {
		t.Fatalf("WriteParquetTemp failed: %v", err)
	}

	// Verify temp file exists
	if _, err := os.Stat(tempParquet); os.IsNotExist(err) {
		t.Error("temp parquet file should exist")
	}

	// Test WriteManifestTemp
	tempManifest, err := store.WriteManifestTemp(ctx, ref, manifest)
	if err != nil {
		t.Fatalf("WriteManifestTemp failed: %v", err)
	}

	// Verify temp file exists
	if _, err := os.Stat(tempManifest); os.IsNotExist(err) {
		t.Error("temp manifest file should exist")
	}

	// Final paths shouldn't exist yet
	finalParquet := filepath.Join(tmpDir, ref.Path("bronze/"))
	finalManifest := filepath.Join(tmpDir, ref.ManifestPath("bronze/"))

	if _, err := os.Stat(finalParquet); !os.IsNotExist(err) {
		t.Error("final parquet should not exist before Finalize")
	}

	// Test Finalize
	tempKeys := []string{tempParquet, tempManifest}
	if err := store.Finalize(ctx, ref, tempKeys); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	// Verify final files exist
	if _, err := os.Stat(finalParquet); os.IsNotExist(err) {
		t.Error("final parquet should exist after Finalize")
	}
	if _, err := os.Stat(finalManifest); os.IsNotExist(err) {
		t.Error("final manifest should exist after Finalize")
	}

	// Verify temp files are gone
	if _, err := os.Stat(tempParquet); !os.IsNotExist(err) {
		t.Error("temp parquet should be removed after Finalize")
	}
	if _, err := os.Stat(tempManifest); !os.IsNotExist(err) {
		t.Error("temp manifest should be removed after Finalize")
	}

	// Verify data integrity
	data, err := os.ReadFile(finalParquet)
	if err != nil {
		t.Fatalf("failed to read final parquet: %v", err)
	}
	if string(data) != string(parquetData) {
		t.Error("parquet data mismatch")
	}
}

func TestLocalStoreAbort(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bronze-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewLocalStore(tmpDir, "bronze/")
	if err != nil {
		t.Fatalf("NewLocalStore failed: %v", err)
	}

	ctx := context.Background()
	ref := PartitionRef{
		Network:      "testnet",
		EraID:        "pre_p23",
		VersionLabel: "v1",
		Table:        "ledgers_lcm_raw",
		LedgerStart:  2000,
		LedgerEnd:    2009,
	}

	// Write temp files
	tempParquet, _ := store.WriteParquetTemp(ctx, ref, []byte("test data"))
	manifest := &Manifest{
		Partition: PartitionInfo{Start: 2000, End: 2009},
		CreatedAt: time.Now(),
	}
	tempManifest, _ := store.WriteManifestTemp(ctx, ref, manifest)

	// Verify temp files exist
	if _, err := os.Stat(tempParquet); os.IsNotExist(err) {
		t.Error("temp parquet should exist before Abort")
	}

	// Abort
	tempKeys := []string{tempParquet, tempManifest}
	if err := store.Abort(ctx, tempKeys); err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// Verify temp files are gone
	if _, err := os.Stat(tempParquet); !os.IsNotExist(err) {
		t.Error("temp parquet should be removed after Abort")
	}
	if _, err := os.Stat(tempManifest); !os.IsNotExist(err) {
		t.Error("temp manifest should be removed after Abort")
	}
}

func TestLocalStoreHeadAndList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bronze-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewLocalStore(tmpDir, "bronze/")
	if err != nil {
		t.Fatalf("NewLocalStore failed: %v", err)
	}

	ctx := context.Background()
	ref := PartitionRef{
		Network:      "testnet",
		EraID:        "pre_p23",
		VersionLabel: "v1",
		Table:        "ledgers_lcm_raw",
		LedgerStart:  3000,
		LedgerEnd:    3009,
	}

	// Write a file using standard method
	testData := []byte("test parquet data for head test")
	if err := store.WriteParquet(ctx, ref, testData); err != nil {
		t.Fatalf("WriteParquet failed: %v", err)
	}

	// Test Head
	key := ref.Path("bronze/")
	info, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if info.Size != int64(len(testData)) {
		t.Errorf("Head size = %d, want %d", info.Size, len(testData))
	}

	// Test List
	keys, err := store.List(ctx, "bronze/testnet/pre_p23")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(keys) == 0 {
		t.Error("List should return at least one key")
	}

	// Verify our key is in the list
	found := false
	for _, k := range keys {
		if k == key {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("List should include %s, got %v", key, keys)
	}
}

func TestLocalStoreImplementsAtomicStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bronze-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewLocalStore(tmpDir, "bronze/")
	if err != nil {
		t.Fatalf("NewLocalStore failed: %v", err)
	}

	// Verify it implements AtomicStore
	var _ AtomicStore = store

	// Verify AsAtomic works
	atomic := AsAtomic(store)
	if atomic == nil {
		t.Error("AsAtomic should return non-nil for LocalStore")
	}
}
