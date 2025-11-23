package source

import (
	"context"
	"errors"
)

// LedgerCloseMeta represents the raw ledger close metadata payload.
type LedgerCloseMeta struct {
	LedgerSeq uint32
	RawXDR    []byte
}

// LedgerSource streams ledger close meta from a live or archive source.
type LedgerSource interface {
	Stream(ctx context.Context, start uint32, end uint32) (<-chan LedgerCloseMeta, <-chan error)
	Close() error
}

type SourceConfig struct {
	Mode         string
	GRPCEndpoint string
	ArchiveURL   string
}

var ErrInvalidSourceMode = errors.New("invalid source mode")

// NewLedgerSource constructs a ledger source based on the configured mode.
func NewLedgerSource(cfg SourceConfig) (LedgerSource, error) {
	switch cfg.Mode {
	case "live":
		return newLiveSource(cfg.GRPCEndpoint), nil
	case "archive":
		return newArchiveSource(cfg.ArchiveURL), nil
	default:
		return nil, ErrInvalidSourceMode
	}
}

type noopSource struct{}

func (noopSource) Stream(ctx context.Context, start uint32, end uint32) (<-chan LedgerCloseMeta, <-chan error) {
	ledgerCh := make(chan LedgerCloseMeta)
	errCh := make(chan error, 1)
	close(ledgerCh)
	close(errCh)
	return ledgerCh, errCh
}

func (noopSource) Close() error { return nil }

func newLiveSource(_ string) LedgerSource    { return noopSource{} }
func newArchiveSource(_ string) LedgerSource { return noopSource{} }
