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
}

// Emitter is the interface for PAS event emission.
type Emitter interface {
	EmitPartition(ctx context.Context, evt Event) error
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

// httpEmitterWrapper adapts HTTPEmitter to the Emitter interface.
type httpEmitterWrapper struct {
	emitter *HTTPEmitter
}

func (w *httpEmitterWrapper) EmitPartition(ctx context.Context, evt Event) error {
	pasEvent := convertToPASEvent(evt)
	return w.emitter.Emit(ctx, &pasEvent)
}

func (w *httpEmitterWrapper) Close() error {
	return w.emitter.Close()
}

// fileOnlyEmitterWrapper adapts FileOnlyEmitter to the Emitter interface.
type fileOnlyEmitterWrapper struct {
	emitter *FileOnlyEmitter
}

func (w *fileOnlyEmitterWrapper) EmitPartition(ctx context.Context, evt Event) error {
	pasEvent := convertToPASEvent(evt)
	return w.emitter.Emit(&pasEvent)
}

func (w *fileOnlyEmitterWrapper) Close() error {
	return w.emitter.Close()
}

// convertToPASEvent converts a simplified Event to a full PASEvent.
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

	return PASEvent{
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
}

// noopEmitter discards all events.
type noopEmitter struct{}

func (n *noopEmitter) EmitPartition(_ context.Context, _ Event) error {
	return nil
}

func (n *noopEmitter) Close() error {
	return nil
}
