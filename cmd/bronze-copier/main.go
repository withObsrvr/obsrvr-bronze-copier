package main

import (
	"context"
	"flag"
	"fmt"
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
	// Parse CLI flags
	var (
		configFile   = flag.String("config", "", "Path to YAML configuration file")
		showVersion  = flag.Bool("version", false, "Show version and exit")
		validateOnly = flag.Bool("validate", false, "Validate configuration and exit")
	)
	flag.Parse()

	// Handle version flag
	if *showVersion {
		fmt.Printf("bronze-copier %s (%s)\n", copier.Version, copier.GitSHA)
		os.Exit(0)
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("[main] Bronze Copier %s (%s)", copier.Version, copier.GitSHA)

	// Load configuration
	var cfg config.Config
	var err error

	if *configFile != "" {
		cfg, err = config.LoadFromFile(*configFile)
		if err != nil {
			log.Fatalf("[config] failed to load config file: %v", err)
		}
		log.Printf("[config] loaded from %s", *configFile)
	} else if envConfig := os.Getenv("CONFIG_FILE"); envConfig != "" {
		cfg, err = config.LoadFromFile(envConfig)
		if err != nil {
			log.Fatalf("[config] failed to load config file: %v", err)
		}
		log.Printf("[config] loaded from %s (via CONFIG_FILE)", envConfig)
	} else {
		cfg = config.LoadFromEnv()
		log.Printf("[config] loaded from environment variables")
	}

	// Validate configuration
	if err := config.Validate(cfg); err != nil {
		log.Fatalf("[config] validation failed: %v", err)
	}

	if *validateOnly {
		log.Println("[config] configuration is valid")
		os.Exit(0)
	}

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
