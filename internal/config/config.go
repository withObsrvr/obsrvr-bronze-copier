package config

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the Bronze Copier.
type Config struct {
	// Copier identification
	CopierID string `yaml:"copier_id"`

	// Era configuration (single era mode - backwards compatible)
	Era EraConfig `yaml:"era"`

	// Multi-era configuration (optional, takes precedence over Era if set)
	Eras       []EraConfig `yaml:"eras"`
	ActiveEras []string    `yaml:"active_eras"` // Which eras to process (empty = all)

	// Source configuration
	Source SourceConfig `yaml:"source"`

	// Storage configuration
	Storage StorageConfig `yaml:"storage"`

	// Catalog configuration
	Catalog CatalogConfig `yaml:"catalog"`

	// PAS configuration
	PAS PASConfig `yaml:"pas"`

	// Checkpoint configuration
	Checkpoint CheckpointConfig `yaml:"checkpoint"`

	// Performance configuration
	Perf PerfConfig `yaml:"perf"`

	// Logging configuration
	Logging LoggingConfig `yaml:"logging"`

	// Metrics configuration
	Metrics MetricsConfig `yaml:"metrics"`
}

// LoggingConfig defines logging settings.
type LoggingConfig struct {
	Format string `yaml:"format"` // "json" | "text"
	Level  string `yaml:"level"`  // "debug" | "info" | "warn" | "error"
}

// MetricsConfig defines Prometheus metrics settings.
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"` // e.g., ":9090"
}

// EraConfig defines the era and version being processed.
type EraConfig struct {
	EraID          string `yaml:"era_id"`          // e.g., "pre_p23", "p23_plus"
	VersionLabel   string `yaml:"version_label"`   // e.g., "v1", "v2"
	Network        string `yaml:"network"`         // e.g., "pubnet", "testnet"
	LedgerStart    uint32 `yaml:"ledger_start"`    // Starting ledger (inclusive)
	LedgerEnd      uint32 `yaml:"ledger_end"`      // Ending ledger (0 = unbounded)
	PartitionSize  uint32 `yaml:"partition_size"`  // Ledgers per partition
	AllowOverwrite bool   `yaml:"allow_overwrite"` // Allow overwriting existing partitions
}

// SourceConfig defines where to read ledger data from.
type SourceConfig struct {
	Mode string `yaml:"mode"` // "local" | "gcs" | "s3" | "datastore"

	// Local filesystem
	LocalPath string `yaml:"local_path"`

	// GCS
	GCSBucket string `yaml:"gcs_bucket"`
	GCSPrefix string `yaml:"gcs_prefix"`

	// S3 (also works for B2, R2, MinIO)
	S3Bucket   string `yaml:"s3_bucket"`
	S3Prefix   string `yaml:"s3_prefix"`
	S3Endpoint string `yaml:"s3_endpoint"` // Custom endpoint for B2/MinIO/R2
	S3Region   string `yaml:"s3_region"`

	// Datastore mode (recommended for Galexie archives)
	DatastoreType string `yaml:"datastore_type"` // "GCS" or "S3"
	DatastorePath string `yaml:"datastore_path"` // Full bucket path
}

// StorageConfig defines where to write Bronze output.
type StorageConfig struct {
	Backend string `yaml:"backend"` // "local" | "gcs" | "s3"

	// Local filesystem
	LocalDir string `yaml:"local_dir"`

	// GCS
	GCSBucket string `yaml:"gcs_bucket"`

	// S3
	S3Bucket   string `yaml:"s3_bucket"`
	S3Endpoint string `yaml:"s3_endpoint"`
	S3Region   string `yaml:"s3_region"`

	// Common
	Prefix string `yaml:"prefix"` // Path prefix (default: "bronze/")
}

// CatalogConfig defines the metadata catalog connection.
type CatalogConfig struct {
	PostgresDSN string `yaml:"postgres_dsn"`
	Namespace   string `yaml:"namespace"` // e.g., "mainnet"
}

// PASConfig defines Public Audit Stream settings.
type PASConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Endpoint  string `yaml:"endpoint"`   // PAS server URL
	BackupDir string `yaml:"backup_dir"` // Local backup directory for events
}

// CheckpointConfig defines checkpoint settings.
type CheckpointConfig struct {
	Enabled bool   `yaml:"enabled"`
	Dir     string `yaml:"dir"` // Directory for checkpoint files
}

// PerfConfig defines performance settings.
type PerfConfig struct {
	MaxInFlightPartitions int `yaml:"max_in_flight_partitions"`
	Workers               int `yaml:"workers"`           // Number of parallel workers (default: MaxInFlightPartitions)
	QueueSize             int `yaml:"queue_size"`        // Work queue size (default: Workers * 2)
	RetryAttempts         int `yaml:"retry_attempts"`    // Max retries per partition (default: 3)
	RetryBackoffMs        int `yaml:"retry_backoff_ms"`  // Initial backoff in ms (default: 1000)
}

// MustLoad loads configuration from YAML file or environment variables.
// YAML file takes precedence if specified via CONFIG_FILE env var.
func MustLoad() Config {
	log.Println("[config] loading")

	// Check for config file
	if configFile := os.Getenv("CONFIG_FILE"); configFile != "" {
		cfg, err := LoadFromFile(configFile)
		if err != nil {
			log.Fatalf("[config] failed to load config file %s: %v", configFile, err)
		}
		return cfg
	}

	// Fall back to environment variables
	return LoadFromEnv()
}

// LoadFromFile loads configuration from a YAML file.
func LoadFromFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config file: %w", err)
	}

	// Apply defaults
	applyDefaults(&cfg)

	return cfg, nil
}

// LoadFromEnv loads configuration from environment variables.
func LoadFromEnv() Config {
	cfg := Config{
		CopierID: getenvDefault("COPIER_ID", "bronze-copier-001"),
		Era: EraConfig{
			EraID:          getenvDefault("ERA_ID", "pre_p23"),
			VersionLabel:   getenvDefault("VERSION_LABEL", "v1"),
			Network:        getenvDefault("NETWORK", "pubnet"),
			LedgerStart:    parseUint32(getenvDefault("LEDGER_START", "1")),
			LedgerEnd:      parseUint32(getenvDefault("LEDGER_END", "0")),
			PartitionSize:  parseUint32(getenvDefault("PARTITION_SIZE", "10000")),
			AllowOverwrite: os.Getenv("ALLOW_OVERWRITE") == "true",
		},
		Source: SourceConfig{
			Mode:          getenvDefault("SOURCE_MODE", "local"),
			LocalPath:     os.Getenv("SOURCE_LOCAL_PATH"),
			GCSBucket:     os.Getenv("SOURCE_GCS_BUCKET"),
			GCSPrefix:     os.Getenv("SOURCE_GCS_PREFIX"),
			S3Bucket:      os.Getenv("SOURCE_S3_BUCKET"),
			S3Prefix:      os.Getenv("SOURCE_S3_PREFIX"),
			S3Endpoint:    os.Getenv("SOURCE_S3_ENDPOINT"),
			S3Region:      os.Getenv("SOURCE_S3_REGION"),
			DatastoreType: os.Getenv("SOURCE_DATASTORE_TYPE"),
			DatastorePath: os.Getenv("SOURCE_DATASTORE_PATH"),
		},
		Storage: StorageConfig{
			Backend:    getenvDefault("STORAGE_BACKEND", "local"),
			LocalDir:   getenvDefault("STORAGE_LOCAL_DIR", "./data"),
			GCSBucket:  os.Getenv("STORAGE_GCS_BUCKET"),
			S3Bucket:   os.Getenv("STORAGE_S3_BUCKET"),
			S3Endpoint: os.Getenv("STORAGE_S3_ENDPOINT"),
			S3Region:   os.Getenv("STORAGE_S3_REGION"),
			Prefix:     getenvDefault("STORAGE_PREFIX", "bronze/"),
		},
		Catalog: CatalogConfig{
			PostgresDSN: os.Getenv("CATALOG_DSN"),
			Namespace:   getenvDefault("CATALOG_NAMESPACE", "mainnet"),
		},
		PAS: PASConfig{
			Enabled:   os.Getenv("PAS_ENABLED") == "true",
			Endpoint:  os.Getenv("PAS_ENDPOINT"),
			BackupDir: getenvDefault("PAS_BACKUP_DIR", "./pas-backup"),
		},
		Checkpoint: CheckpointConfig{
			Enabled: getenvDefault("CHECKPOINT_ENABLED", "true") == "true",
			Dir:     getenvDefault("CHECKPOINT_DIR", "./state"),
		},
		Perf: PerfConfig{
			MaxInFlightPartitions: parseInt(getenvDefault("MAX_IN_FLIGHT_PARTITIONS", "4")),
		},
		Logging: LoggingConfig{
			Format: getenvDefault("LOG_FORMAT", "text"),
			Level:  getenvDefault("LOG_LEVEL", "info"),
		},
		Metrics: MetricsConfig{
			Enabled: os.Getenv("METRICS_ENABLED") == "true",
			Address: getenvDefault("METRICS_ADDRESS", ":9090"),
		},
	}

	return cfg
}

// applyDefaults fills in default values for unset fields.
func applyDefaults(cfg *Config) {
	if cfg.CopierID == "" {
		cfg.CopierID = "bronze-copier-001"
	}
	if cfg.Era.EraID == "" {
		cfg.Era.EraID = "pre_p23"
	}
	if cfg.Era.VersionLabel == "" {
		cfg.Era.VersionLabel = "v1"
	}
	if cfg.Era.Network == "" {
		cfg.Era.Network = "pubnet"
	}
	if cfg.Era.PartitionSize == 0 {
		cfg.Era.PartitionSize = 10000
	}
	if cfg.Source.Mode == "" {
		cfg.Source.Mode = "local"
	}
	if cfg.Storage.Backend == "" {
		cfg.Storage.Backend = "local"
	}
	if cfg.Storage.Prefix == "" {
		cfg.Storage.Prefix = "bronze/"
	}
	if cfg.Checkpoint.Dir == "" {
		cfg.Checkpoint.Dir = "./state"
	}
	if cfg.PAS.BackupDir == "" {
		cfg.PAS.BackupDir = "./pas-backup"
	}
	if cfg.Perf.MaxInFlightPartitions == 0 {
		cfg.Perf.MaxInFlightPartitions = 4
	}
	if cfg.Perf.Workers == 0 {
		cfg.Perf.Workers = cfg.Perf.MaxInFlightPartitions
	}
	if cfg.Perf.QueueSize == 0 {
		cfg.Perf.QueueSize = cfg.Perf.Workers * 2
	}
	if cfg.Perf.RetryAttempts == 0 {
		cfg.Perf.RetryAttempts = 3
	}
	if cfg.Perf.RetryBackoffMs == 0 {
		cfg.Perf.RetryBackoffMs = 1000
	}
	if cfg.Logging.Format == "" {
		cfg.Logging.Format = "text"
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	if cfg.Metrics.Address == "" {
		cfg.Metrics.Address = ":9090"
	}
}

func getenvDefault(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func parseUint32(v string) uint32 {
	parsed, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(parsed)
}

func parseInt(v string) int {
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return parsed
}

// Validate checks the configuration for errors.
func Validate(cfg Config) error {
	// Validate era configuration
	if cfg.Era.Network == "" {
		return fmt.Errorf("era.network is required")
	}
	if cfg.Era.EraID == "" {
		return fmt.Errorf("era.era_id is required")
	}
	if cfg.Era.VersionLabel == "" {
		return fmt.Errorf("era.version_label is required")
	}
	if cfg.Era.PartitionSize == 0 {
		return fmt.Errorf("era.partition_size must be > 0")
	}

	// Validate source configuration
	switch cfg.Source.Mode {
	case "local":
		if cfg.Source.LocalPath == "" {
			return fmt.Errorf("source.local_path is required when mode=local")
		}
	case "gcs":
		if cfg.Source.GCSBucket == "" {
			return fmt.Errorf("source.gcs_bucket is required when mode=gcs")
		}
	case "s3":
		if cfg.Source.S3Bucket == "" {
			return fmt.Errorf("source.s3_bucket is required when mode=s3")
		}
	case "datastore":
		if cfg.Source.DatastorePath == "" {
			return fmt.Errorf("source.datastore_path is required when mode=datastore")
		}
		if cfg.Source.DatastoreType != "GCS" && cfg.Source.DatastoreType != "S3" {
			return fmt.Errorf("source.datastore_type must be 'GCS' or 'S3'")
		}
	default:
		return fmt.Errorf("source.mode must be one of: local, gcs, s3, datastore")
	}

	// Validate storage configuration
	switch cfg.Storage.Backend {
	case "local":
		if cfg.Storage.LocalDir == "" {
			return fmt.Errorf("storage.local_dir is required when backend=local")
		}
	case "gcs":
		if cfg.Storage.GCSBucket == "" {
			return fmt.Errorf("storage.gcs_bucket is required when backend=gcs")
		}
	case "s3":
		if cfg.Storage.S3Bucket == "" {
			return fmt.Errorf("storage.s3_bucket is required when backend=s3")
		}
	default:
		return fmt.Errorf("storage.backend must be one of: local, gcs, s3")
	}

	// Validate perf configuration
	if cfg.Perf.MaxInFlightPartitions < 1 {
		return fmt.Errorf("perf.max_in_flight_partitions must be >= 1")
	}

	return nil
}

// GetEffectiveEras returns the list of era configurations to use.
// If Eras is configured, it returns those; otherwise, it returns a single-element
// slice containing Era (for backwards compatibility).
func (cfg Config) GetEffectiveEras() []EraConfig {
	if len(cfg.Eras) > 0 {
		return cfg.Eras
	}
	// Backwards compatibility: use single Era config
	return []EraConfig{cfg.Era}
}

// IsMultiEra returns true if multiple eras are configured.
func (cfg Config) IsMultiEra() bool {
	return len(cfg.Eras) > 1
}
