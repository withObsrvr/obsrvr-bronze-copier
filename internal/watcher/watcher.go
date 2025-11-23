package watcher

import (
	"log"
	"path/filepath"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/copier"
)

type Watcher struct {
	cfg  *config.Config
	copy *copier.Copier
}

func New(cfg *config.Config) *Watcher {
	return &Watcher{
		cfg:  cfg,
		copy: copier.New(cfg),
	}
}

func (w *Watcher) Run() error {
	// TODO: replace with real GCS/B2 listing
	files := []string{
		"FC6D1D66--59957913.xdr.zst",
	}

	for _, f := range files {
		abs := filepath.Join(w.cfg.SourceBucket, f)
		if err := w.copy.CopyFile(abs); err != nil {
			log.Printf("copy error: %v", err)
		}
	}
	return nil
}
