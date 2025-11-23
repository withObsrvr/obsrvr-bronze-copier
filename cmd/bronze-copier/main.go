package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/copier"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/logging"
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

	// Load configuration first (need logging config)
	var cfg config.Config
	var err error

	if *configFile != "" {
		cfg, err = config.LoadFromFile(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config: failed to load config file: %v\n", err)
			os.Exit(1)
		}
	} else if envConfig := os.Getenv("CONFIG_FILE"); envConfig != "" {
		cfg, err = config.LoadFromFile(envConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config: failed to load config file: %v\n", err)
			os.Exit(1)
		}
	} else {
		cfg = config.LoadFromEnv()
	}

	// Initialize structured logging
	logging.Setup(logging.Config{
		Format: cfg.Logging.Format,
		Level:  cfg.Logging.Level,
	})

	log := slog.With("component", "main")
	log.Info("bronze copier starting",
		"version", copier.Version,
		"git_sha", copier.GitSHA,
		"log_format", cfg.Logging.Format,
		"log_level", cfg.Logging.Level,
	)

	// Validate configuration
	if err := config.Validate(cfg); err != nil {
		log.Error("configuration validation failed", "error", err)
		os.Exit(1)
	}

	if *validateOnly {
		log.Info("configuration is valid")
		os.Exit(0)
	}

	log.Info("configuration loaded",
		"source_mode", cfg.Source.Mode,
		"storage_backend", cfg.Storage.Backend,
		"network", cfg.Era.Network,
		"era_id", cfg.Era.EraID,
		"version_label", cfg.Era.VersionLabel,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown handler
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		sig := <-ch
		log.Warn("shutdown signal received", "signal", sig.String())
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
		log.Error("failed to create source", "error", err)
		os.Exit(1)
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
		log.Error("failed to create storage", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// Create and run copier
	c := copier.New(cfg, src, store)

	if err := c.Run(ctx); err != nil {
		if ctx.Err() != nil {
			log.Info("shutdown complete")
		} else {
			log.Error("copier failed", "error", err)
			os.Exit(1)
		}
	}

	log.Info("bronze copier stopped cleanly")
	time.Sleep(100 * time.Millisecond)
}
