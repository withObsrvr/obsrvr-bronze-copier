package config

import (
	"log"
	"os"
	"strconv"
)

type Config struct {
	Era     EraConfig
	Source  SourceConfig
	Storage StorageConfig
	Catalog CatalogConfig
	PAS     PASConfig
	Perf    PerfConfig
}

type EraConfig struct {
	EraID          string
	VersionLabel   string
	LedgerStart    uint32
	LedgerEnd      uint32
	PartitionSize  uint32
	AllowOverwrite bool
}

type SourceConfig struct {
	Mode         string
	GRPCEndpoint string
	ArchiveURL   string
}

type StorageConfig struct {
	Backend  string
	Bucket   string
	Prefix   string
	LocalDir string
}

type CatalogConfig struct {
	PostgresDSN string
	Namespace   string
}

type PASConfig struct {
	Enabled  bool
	Endpoint string
}

type PerfConfig struct {
	MaxInFlightPartitions int
}

// MustLoad is a placeholder configuration loader. It accepts minimal environment
// variables so the skeleton can be wired into CI or extended with YAML/env
// parsing later.
func MustLoad() Config {
	log.Println("[config] loading")

	partitionSize := uint32(10000)
	if v := os.Getenv("PARTITION_SIZE"); v != "" {
		if parsed, err := strconv.ParseUint(v, 10, 32); err == nil {
			partitionSize = uint32(parsed)
		}
	}

	maxInFlight := 4
	if v := os.Getenv("MAX_IN_FLIGHT_PARTITIONS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			maxInFlight = parsed
		}
	}

	return Config{
		Era: EraConfig{
			EraID:          getenvDefault("ERA_ID", "pre_p23"),
			VersionLabel:   getenvDefault("VERSION_LABEL", "v1"),
			LedgerStart:    parseUint32(getenvDefault("LEDGER_START", "1")),
			LedgerEnd:      parseUint32(getenvDefault("LEDGER_END", "0")),
			PartitionSize:  partitionSize,
			AllowOverwrite: os.Getenv("ALLOW_OVERWRITE") == "true",
		},
		Source: SourceConfig{
			Mode:         getenvDefault("SOURCE_MODE", "archive"),
			GRPCEndpoint: os.Getenv("GRPC_ENDPOINT"),
			ArchiveURL:   os.Getenv("ARCHIVE_URL"),
		},
		Storage: StorageConfig{
			Backend:  getenvDefault("STORAGE_BACKEND", "local"),
			Bucket:   os.Getenv("STORAGE_BUCKET"),
			Prefix:   getenvDefault("STORAGE_PREFIX", "bronze/"),
			LocalDir: getenvDefault("LOCAL_DIR", "./data"),
		},
		Catalog: CatalogConfig{
			PostgresDSN: os.Getenv("CATALOG_DSN"),
			Namespace:   getenvDefault("CATALOG_NAMESPACE", "mainnet"),
		},
		PAS: PASConfig{
			Enabled:  os.Getenv("PAS_ENABLED") == "true",
			Endpoint: os.Getenv("PAS_ENDPOINT"),
		},
		Perf: PerfConfig{
			MaxInFlightPartitions: maxInFlight,
		},
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
