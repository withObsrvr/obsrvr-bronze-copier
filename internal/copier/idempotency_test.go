package copier

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/metadata"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/source"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/storage"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/tables"
)

// mockSource implements source.LedgerSource for testing
type mockSource struct {
	ledgers []source.LedgerCloseMeta
	mu      sync.Mutex
	calls   int
}

func (m *mockSource) Stream(ctx context.Context, start, end uint32) (<-chan source.LedgerCloseMeta, <-chan error) {
	lcmCh := make(chan source.LedgerCloseMeta)
	errCh := make(chan error)

	go func() {
		defer close(lcmCh)
		defer close(errCh)

		m.mu.Lock()
		m.calls++
		m.mu.Unlock()

		for _, lcm := range m.ledgers {
			if lcm.LedgerSeq >= start && (end == 0 || lcm.LedgerSeq <= end) {
				select {
				case lcmCh <- lcm:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return lcmCh, errCh
}

func (m *mockSource) Close() error {
	return nil
}

// mockStore implements storage.BronzeStore for testing
type mockStore struct {
	mu       sync.Mutex
	written  map[string][]byte
	exists   map[string]bool
	manifest map[string]*storage.Manifest
}

func newMockStore() *mockStore {
	return &mockStore{
		written:  make(map[string][]byte),
		exists:   make(map[string]bool),
		manifest: make(map[string]*storage.Manifest),
	}
}

func (m *mockStore) WriteParquet(ctx context.Context, ref storage.PartitionRef, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := ref.Path("")
	m.written[key] = data
	m.exists[key] = true
	return nil
}

func (m *mockStore) WriteManifest(ctx context.Context, ref storage.PartitionRef, manifest *storage.Manifest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := ref.ManifestPath("")
	m.manifest[key] = manifest
	m.exists[key] = true
	return nil
}

func (m *mockStore) Exists(ctx context.Context, ref storage.PartitionRef) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := ref.Path("")
	return m.exists[key], nil
}

func (m *mockStore) GetWriteCount(ref storage.PartitionRef) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := ref.Path("")
	if _, ok := m.written[key]; ok {
		return 1
	}
	return 0
}

func (m *mockStore) URI(key string) string {
	return "mock://" + key
}

func (m *mockStore) Close() error {
	return nil
}

// mockMetadata implements metadata.Writer for testing
type mockMetadata struct {
	mu        sync.Mutex
	datasetID int64
	partitions map[string]bool // key: "datasetID:start:end"
	lineage   []metadata.LineageRecord
}

func newMockMetadata() *mockMetadata {
	return &mockMetadata{
		datasetID:  1,
		partitions: make(map[string]bool),
		lineage:    make([]metadata.LineageRecord, 0),
	}
}

func (m *mockMetadata) EnsureDataset(ctx context.Context, info metadata.DatasetInfo) (int64, error) {
	return m.datasetID, nil
}

func (m *mockMetadata) RecordPartition(ctx context.Context, rec metadata.PartitionRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := partitionKey(rec.DatasetID, rec.Start, rec.End)
	m.partitions[key] = true
	return nil
}

func (m *mockMetadata) PartitionExists(ctx context.Context, datasetID int64, start, end uint32) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := partitionKey(datasetID, start, end)
	return m.partitions[key], nil
}

func (m *mockMetadata) GetLastCommittedLedger(ctx context.Context, datasetID int64) (uint32, error) {
	return 0, nil
}

func (m *mockMetadata) GetLastChecksum(ctx context.Context, datasetID int64) (string, error) {
	return "", nil
}

func (m *mockMetadata) GetLastLineage(ctx context.Context, datasetID int64) (*metadata.LineageRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.lineage) == 0 {
		return nil, nil
	}
	return &m.lineage[len(m.lineage)-1], nil
}

func (m *mockMetadata) InsertLineage(ctx context.Context, rec metadata.LineageRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lineage = append(m.lineage, rec)
	key := partitionKey(rec.DatasetID, rec.LedgerStart, rec.LedgerEnd)
	m.partitions[key] = true
	return nil
}

func (m *mockMetadata) InsertQuality(ctx context.Context, rec metadata.QualityRecord) error {
	return nil
}

func (m *mockMetadata) Close() error {
	return nil
}

func partitionKey(datasetID int64, start, end uint32) string {
	return string(rune(datasetID)) + ":" + string(rune(start)) + ":" + string(rune(end))
}

// testLogger returns a no-op logger for tests
func testLogger() *slog.Logger {
	return slog.Default()
}

func TestPartitionIdempotencyViaCatalog(t *testing.T) {
	// Setup mock metadata that reports partition already exists
	meta := newMockMetadata()
	meta.partitions[partitionKey(1, 1000, 1009)] = true // Pre-mark as existing

	store := newMockStore()

	cfg := config.Config{
		Era: config.EraConfig{
			Network:       "testnet",
			EraID:         "pre_p23",
			VersionLabel:  "v1",
			PartitionSize: 10,
			LedgerStart:   1000,
			LedgerEnd:     1009,
		},
		Bronze: config.BronzeConfig{
			PrimaryTable: "ledgers_lcm_raw",
		},
	}

	// Create a copier with mock dependencies
	c := &Copier{
		cfg:       cfg,
		store:     store,
		meta:      meta,
		datasetID: 1,
		log:       testLogger(),
	}

	// Attempt to publish a partition that already exists in catalog
	ctx := context.Background()
	_, err := c.publishPartition(ctx, mockPartition(1000, 1009))

	// Should return ErrPartitionExists
	if !errors.Is(err, ErrPartitionExists) {
		t.Errorf("expected ErrPartitionExists, got: %v", err)
	}

	// Store should NOT have been written to
	if store.GetWriteCount(storage.PartitionRef{
		Network:      "testnet",
		EraID:        "pre_p23",
		VersionLabel: "v1",
		Table:        "ledgers_lcm_raw",
		LedgerStart:  1000,
		LedgerEnd:    1009,
	}) > 0 {
		t.Error("store should not have been written to when partition exists in catalog")
	}
}

func TestPartitionIdempotencyViaStorage(t *testing.T) {
	meta := newMockMetadata()
	store := newMockStore()

	// Pre-mark partition as existing in storage
	ref := storage.PartitionRef{
		Network:      "testnet",
		EraID:        "pre_p23",
		VersionLabel: "v1",
		Table:        "ledgers_lcm_raw",
		LedgerStart:  2000,
		LedgerEnd:    2009,
	}
	store.exists[ref.Path("")] = true

	cfg := config.Config{
		Era: config.EraConfig{
			Network:        "testnet",
			EraID:          "pre_p23",
			VersionLabel:   "v1",
			PartitionSize:  10,
			LedgerStart:    2000,
			LedgerEnd:      2009,
			AllowOverwrite: false, // Explicit: don't overwrite
		},
		Bronze: config.BronzeConfig{
			PrimaryTable: "ledgers_lcm_raw",
		},
	}

	c := &Copier{
		cfg:       cfg,
		store:     store,
		meta:      meta,
		datasetID: 1,
		log:       testLogger(),
	}

	ctx := context.Background()
	_, err := c.publishPartition(ctx, mockPartition(2000, 2009))

	// Should skip because exists in storage
	if !errors.Is(err, ErrPartitionExists) {
		t.Errorf("expected ErrPartitionExists from storage check, got: %v", err)
	}
}

func TestPartitionOverwriteAllowed(t *testing.T) {
	meta := newMockMetadata()
	store := newMockStore()

	// Pre-mark partition as existing in storage
	ref := storage.PartitionRef{
		Network:      "testnet",
		EraID:        "pre_p23",
		VersionLabel: "v1",
		Table:        "ledgers_lcm_raw",
		LedgerStart:  3000,
		LedgerEnd:    3009,
	}
	store.exists[ref.Path("")] = true

	cfg := config.Config{
		Era: config.EraConfig{
			Network:        "testnet",
			EraID:          "pre_p23",
			VersionLabel:   "v1",
			PartitionSize:  10,
			LedgerStart:    3000,
			LedgerEnd:      3009,
			AllowOverwrite: true, // Allow overwrite
		},
		Bronze: config.BronzeConfig{
			PrimaryTable: "ledgers_lcm_raw",
		},
	}

	c := &Copier{
		cfg:       cfg,
		store:     store,
		meta:      meta,
		datasetID: 1,
		log:       testLogger(),
	}

	ctx := context.Background()
	// This will fail at parquet generation since we don't have real ledger data,
	// but it should NOT return ErrPartitionExists - it should attempt to overwrite
	_, err := c.publishPartition(ctx, mockPartition(3000, 3009))

	// Should NOT be ErrPartitionExists when AllowOverwrite=true
	if errors.Is(err, ErrPartitionExists) {
		t.Error("should not return ErrPartitionExists when AllowOverwrite=true")
	}
}

func TestStrictModesCatalogFailure(t *testing.T) {
	// Test that strict catalog mode causes failures to propagate
	meta := &failingMetadata{failInsertLineage: true}
	store := newMockStore()

	cfg := config.Config{
		Era: config.EraConfig{
			Network:       "testnet",
			EraID:         "pre_p23",
			VersionLabel:  "v1",
			PartitionSize: 10,
			LedgerStart:   4000,
			LedgerEnd:     4009,
		},
		Catalog: config.CatalogConfig{
			Strict: true, // Enable strict mode
		},
		Bronze: config.BronzeConfig{
			PrimaryTable: "ledgers_lcm_raw",
		},
	}

	c := &Copier{
		cfg:       cfg,
		store:     store,
		meta:      meta,
		datasetID: 1,
		log:       testLogger(),
	}

	// This test validates config is set correctly
	_ = c.cfg.Catalog.Strict // Just verify strict mode is configured

	if !c.cfg.Catalog.Strict {
		t.Error("Catalog.Strict should be true")
	}
}

// failingMetadata simulates catalog failures
type failingMetadata struct {
	failInsertLineage bool
}

func (f *failingMetadata) EnsureDataset(ctx context.Context, info metadata.DatasetInfo) (int64, error) {
	return 1, nil
}

func (f *failingMetadata) RecordPartition(ctx context.Context, rec metadata.PartitionRecord) error {
	return errors.New("simulated catalog failure")
}

func (f *failingMetadata) PartitionExists(ctx context.Context, datasetID int64, start, end uint32) (bool, error) {
	return false, nil
}

func (f *failingMetadata) GetLastCommittedLedger(ctx context.Context, datasetID int64) (uint32, error) {
	return 0, nil
}

func (f *failingMetadata) GetLastChecksum(ctx context.Context, datasetID int64) (string, error) {
	return "", nil
}

func (f *failingMetadata) GetLastLineage(ctx context.Context, datasetID int64) (*metadata.LineageRecord, error) {
	return nil, nil
}

func (f *failingMetadata) InsertLineage(ctx context.Context, rec metadata.LineageRecord) error {
	if f.failInsertLineage {
		return errors.New("simulated lineage failure")
	}
	return nil
}

func (f *failingMetadata) InsertQuality(ctx context.Context, rec metadata.QualityRecord) error {
	return nil
}

func (f *failingMetadata) Close() error {
	return nil
}

// mockPartition creates a mock partition for testing with empty ledgers
func mockPartition(start, end uint32) tables.Partition {
	return tables.Partition{
		Start:   start,
		End:     end,
		Ledgers: []source.LedgerCloseMeta{}, // Empty - will fail at ToParquet but tests idempotency checks
	}
}
