package pas

import "context"

type PASConfig struct {
	Enabled  bool
	Endpoint string
}

type Event struct {
	EraID        string
	VersionLabel string
	Start        uint32
	End          uint32
	Checksums    map[string]string
}

type Emitter interface {
	EmitPartition(ctx context.Context, evt Event) error
}

// NewEmitter returns a placeholder emitter to be replaced with the PAS v1.1
// client when available.
func NewEmitter(cfg PASConfig) Emitter {
	return noopEmitter{cfg: cfg}
}

type noopEmitter struct {
	cfg PASConfig
}

func (n noopEmitter) EmitPartition(_ context.Context, _ Event) error { return nil }
