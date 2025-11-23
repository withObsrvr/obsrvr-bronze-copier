package pas

import (
	"context"
	"log"
	"time"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
)

// Event is the simplified event struct used by the copier.
// This is converted to a PASEvent before emission.
type Event struct {
	EraID        string
	VersionLabel string
	Network      string
	Start        uint32
	End          uint32
	Checksums    map[string]string
	RowCounts    map[string]int64
	ByteSizes    map[string]int64
	StoragePaths map[string]string
	Producer     ProducerInfo

	// Hash chaining fields
	PrevEventHash string // Hash of the previous event in the chain (empty for first)
}

// Emitter is the interface for PAS event emission.
type Emitter interface {
	// EmitPartition emits a PAS event and returns the computed event hash.
	// The event hash should be passed as PrevEventHash in the next event.
	EmitPartition(ctx context.Context, evt Event) (eventHash string, err error)

	// GetLastEventHash returns the hash of the last emitted event.
	// Returns empty string if no events have been emitted yet.
	GetLastEventHash() string

	Close() error
}

// NewEmitter creates an appropriate emitter based on configuration.
func NewEmitter(cfg config.PASConfig) Emitter {
	if !cfg.Enabled {
		log.Println("[pas] disabled, using no-op emitter")
		return &noopEmitter{}
	}

	// If endpoint is configured, use HTTP emitter
	if cfg.Endpoint != "" {
		emitter, err := NewHTTPEmitter(cfg)
		if err != nil {
			log.Printf("[pas] failed to create HTTP emitter: %v, falling back to file-only", err)
			return createFileOnlyEmitter(cfg)
		}
		log.Printf("[pas] using HTTP emitter -> %s", cfg.Endpoint)
		return &httpEmitterWrapper{emitter: emitter}
	}

	// Otherwise use file-only emitter
	return createFileOnlyEmitter(cfg)
}

func createFileOnlyEmitter(cfg config.PASConfig) Emitter {
	emitter, err := NewFileOnlyEmitter(cfg.BackupDir)
	if err != nil {
		log.Printf("[pas] failed to create file emitter: %v, using no-op", err)
		return &noopEmitter{}
	}
	log.Printf("[pas] using file-only emitter -> %s", cfg.BackupDir)
	return &fileOnlyEmitterWrapper{emitter: emitter}
}

// httpEmitterWrapper adapts HTTPEmitter to the Emitter interface with hash chaining.
type httpEmitterWrapper struct {
	emitter       *HTTPEmitter
	lastEventHash string
}

func (w *httpEmitterWrapper) EmitPartition(ctx context.Context, evt Event) (string, error) {
	pasEvent := convertToPASEvent(evt)
	if err := w.emitter.Emit(ctx, &pasEvent); err != nil {
		return "", err
	}
	w.lastEventHash = pasEvent.Chain.EventHash
	return pasEvent.Chain.EventHash, nil
}

func (w *httpEmitterWrapper) GetLastEventHash() string {
	return w.lastEventHash
}

func (w *httpEmitterWrapper) Close() error {
	return w.emitter.Close()
}

// fileOnlyEmitterWrapper adapts FileOnlyEmitter to the Emitter interface with hash chaining.
type fileOnlyEmitterWrapper struct {
	emitter       *FileOnlyEmitter
	lastEventHash string
}

func (w *fileOnlyEmitterWrapper) EmitPartition(ctx context.Context, evt Event) (string, error) {
	pasEvent := convertToPASEvent(evt)
	if err := w.emitter.Emit(&pasEvent); err != nil {
		return "", err
	}
	w.lastEventHash = pasEvent.Chain.EventHash
	return pasEvent.Chain.EventHash, nil
}

func (w *fileOnlyEmitterWrapper) GetLastEventHash() string {
	return w.lastEventHash
}

func (w *fileOnlyEmitterWrapper) Close() error {
	return w.emitter.Close()
}

// convertToPASEvent converts a simplified Event to a full PASEvent with hash chaining.
func convertToPASEvent(evt Event) PASEvent {
	tables := make(map[string]TableInfo)
	for tableName, checksum := range evt.Checksums {
		tables[tableName] = TableInfo{
			Checksum:    checksum,
			RowCount:    evt.RowCounts[tableName],
			ByteSize:    evt.ByteSizes[tableName],
			StoragePath: evt.StoragePaths[tableName],
		}
	}

	pasEvent := PASEvent{
		Version:   "1.1",
		EventType: "bronze_partition",
		Timestamp: time.Now().UTC(),
		Partition: PartitionInfo{
			Network:      evt.Network,
			EraID:        evt.EraID,
			VersionLabel: evt.VersionLabel,
			LedgerStart:  evt.Start,
			LedgerEnd:    evt.End,
		},
		Tables:   tables,
		Producer: evt.Producer,
	}

	// Set hash chain - this computes EventHash based on PrevEventHash and content
	pasEvent.SetChainHashes(evt.PrevEventHash)

	return pasEvent
}

// noopEmitter discards all events but still tracks hashes for consistency.
type noopEmitter struct {
	lastEventHash string
}

func (n *noopEmitter) EmitPartition(_ context.Context, evt Event) (string, error) {
	// Still compute the hash for consistency even though we don't emit
	pasEvent := convertToPASEvent(evt)
	n.lastEventHash = pasEvent.Chain.EventHash
	return pasEvent.Chain.EventHash, nil
}

func (n *noopEmitter) GetLastEventHash() string {
	return n.lastEventHash
}

func (n *noopEmitter) Close() error {
	return nil
}
