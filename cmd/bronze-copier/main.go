package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/copier"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/source"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/storage"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("[main] Bronze Copier %s (%s)", copier.Version, copier.GitSHA)

	cfg := config.MustLoad()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown handler
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		sig := <-ch
		log.Printf("[shutdown] received signal: %v", sig)
		cancel()
	}()

	// Create ledger source
	srcCfg := source.SourceConfig{
		Mode:          cfg.Source.Mode,
		LocalPath:     cfg.Source.LocalPath,
		GCSBucket:     cfg.Source.GCSBucket,
		GCSPrefix:     cfg.Source.GCSPrefix,
		S3Bucket:      cfg.Source.S3Bucket,
		S3Prefix:      cfg.Source.S3Prefix,
		S3Endpoint:    cfg.Source.S3Endpoint,
		S3Region:      cfg.Source.S3Region,
		DatastoreType: cfg.Source.DatastoreType,
		DatastorePath: cfg.Source.DatastorePath,
	}

	src, err := source.NewLedgerSource(srcCfg)
	if err != nil {
		log.Fatalf("[main] failed to create source: %v", err)
	}
	defer src.Close()

	// Create storage backend
	storeCfg := storage.StorageConfig{
		Backend:    cfg.Storage.Backend,
		LocalDir:   cfg.Storage.LocalDir,
		GCSBucket:  cfg.Storage.GCSBucket,
		S3Bucket:   cfg.Storage.S3Bucket,
		S3Endpoint: cfg.Storage.S3Endpoint,
		S3Region:   cfg.Storage.S3Region,
		Prefix:     cfg.Storage.Prefix,
	}

	store, err := storage.NewBronzeStore(storeCfg)
	if err != nil {
		log.Fatalf("[main] failed to create storage: %v", err)
	}
	defer store.Close()

	// Create and run copier
	c := copier.New(cfg, src, store)

	if err := c.Run(ctx); err != nil {
		if ctx.Err() != nil {
			log.Printf("[main] shutdown complete")
		} else {
			log.Fatalf("[main] copier failed: %v", err)
		}
	}

	log.Println("[main] bronze copier stopped cleanly")
	time.Sleep(100 * time.Millisecond)
}
