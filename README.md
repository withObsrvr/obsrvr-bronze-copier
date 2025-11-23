# Obsrvr Bronze Copier

The Bronze Copier is Obsrvr's trust foundation - a standalone service that copies raw Stellar ledger data from archive sources (Galexie landing buckets) to versioned Bronze storage, producing immutable parquet files with full lineage tracking and audit proofs.

## Features

- **Multi-source support**: Read from local filesystem, GCS, or S3-compatible storage (B2, R2, MinIO)
- **Versioned output**: Era and version-aware path structure for protocol upgrade safety
- **Parquet output**: Efficient columnar format with raw XDR preservation
- **Checkpointing**: Resume from last committed ledger after crashes
- **Deterministic**: Same input always produces identical checksums
- **Manifest files**: Per-partition metadata for verification

## Quick Start

### Using Nix (recommended)

```bash
# Enter development shell
nix develop

# Build the binary
nix build

# Run with default config
./result/bin/bronze-copier
```

### Using Go directly

```bash
# Build
go build -o bronze-copier ./cmd/bronze-copier

# Run
./bronze-copier
```

## Configuration

Bronze Copier can be configured via YAML file or environment variables.

### YAML Configuration

```bash
# Copy example config
cp config.example.yaml config.yaml

# Edit config.yaml for your deployment
vim config.yaml

# Run with config file
CONFIG_FILE=config.yaml ./bronze-copier
```

### Environment Variables

```bash
# Source configuration
export SOURCE_MODE=local
export SOURCE_LOCAL_PATH=./testdata/archive

# Storage configuration
export STORAGE_BACKEND=local
export STORAGE_LOCAL_DIR=./data
export STORAGE_PREFIX=bronze/

# Era configuration
export ERA_ID=pre_p23
export VERSION_LABEL=v1
export NETWORK=pubnet
export LEDGER_START=1
export LEDGER_END=100000
export PARTITION_SIZE=10000

# Run
./bronze-copier
```

## Storage Layout

Bronze Copier writes data in a versioned directory structure:

```
bronze/
  pubnet/
    pre_p23/
      v1/
        ledgers_lcm_raw/
          range=0-9999/
            part-0-9999.parquet
            _manifest.json
          range=10000-19999/
            part-10000-19999.parquet
            _manifest.json
```

### Manifest Format

Each partition includes a manifest file with metadata:

```json
{
  "partition": {
    "start": 0,
    "end": 9999,
    "era_id": "pre_p23",
    "version": "v1",
    "network": "pubnet"
  },
  "tables": {
    "ledgers_lcm_raw": {
      "file": "part-0-9999.parquet",
      "checksum": "sha256:abc123...",
      "row_count": 10000,
      "byte_size": 524288000
    }
  },
  "producer": {
    "name": "bronze-copier",
    "version": "v0.1.0",
    "git_sha": "abc123"
  },
  "created_at": "2025-01-15T10:30:00Z"
}
```

## Schema: ledgers_lcm_raw

The primary Bronze table preserves raw ledger data:

| Column | Type | Description |
|--------|------|-------------|
| ledger_sequence | INT64 | Primary identifier |
| ledger_hash | STRING | Hex-encoded ledger hash |
| previous_ledger_hash | STRING | Previous ledger hash |
| close_time | TIMESTAMP | Ledger close time |
| protocol_version | INT32 | Protocol version |
| total_coins | INT64 | Total coins (stroops) |
| fee_pool | INT64 | Fee pool (stroops) |
| tx_set_hash | STRING | Transaction set hash |
| tx_count | INT32 | Transaction count |
| operation_count | INT32 | Operation count |
| xdr_bytes | BINARY | Raw LedgerCloseMeta XDR |
| xdr_byte_length | INT32 | XDR byte length |
| xdr_sha256 | STRING | SHA256 of xdr_bytes |
| bronze_version | STRING | Bronze schema version |
| era_id | STRING | Era identifier |
| network | STRING | Network name |
| ingested_at | TIMESTAMP | Ingestion timestamp |

## Archive Source Format

Bronze Copier expects Galexie-format archives:

```
landing/ledgers/pubnet/
  FC6DEFFF--59904000-59967999/
    FC6D1D66--59957913.xdr.zst
    FC6D1D67--59957914.xdr.zst
    ...
```

Files are zstd-compressed XDR-encoded `LedgerCloseMeta`.

## Checkpointing

Bronze Copier automatically saves progress after each partition:

```json
{
  "copier_id": "bronze-copier-001",
  "network": "pubnet",
  "era_id": "pre_p23",
  "version_label": "v1",
  "last_committed_ledger": 59957913,
  "last_committed_partition": {
    "start": 59950000,
    "end": 59959999,
    "checksum": "sha256:abc123..."
  },
  "updated_at": "2025-01-15T10:30:00Z"
}
```

On restart, the copier resumes from the last committed ledger.

## Protocol Upgrade Handling

Bronze Copier supports running multiple eras/versions concurrently:

1. **Pre-upgrade**: Run `v1` copier tailing pre-P23 data
2. **Post-upgrade**: Start `v2` copier for P23+ data
3. **Cutover**: Both versions coexist, consumers choose which to read

```bash
# v1 copier (pre-P23)
ERA_ID=pre_p23 VERSION_LABEL=v1 ./bronze-copier

# v2 copier (P23+) - different process
ERA_ID=p23_plus VERSION_LABEL=v2 ./bronze-copier
```

## Development

### Running Tests

```bash
go test ./...
```

### Building Docker Image

```bash
docker build -f Dockerfile.nix -t obsrvr-bronze-copier .
```

### Nix Commands

```bash
# Development shell
nix develop

# Build package
nix build

# Run directly
nix run
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Bronze Copier                            │
│                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │   Source    │───▶│  Partition  │───▶│   Storage   │     │
│  │ (GCS/S3/FS) │    │  Builder    │    │ (GCS/S3/FS) │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│         │                  │                  │             │
│         ▼                  ▼                  ▼             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │   Decoder   │    │   Parquet   │    │  Manifest   │     │
│  │  (XDR/zstd) │    │   Writer    │    │   Writer    │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│                            │                               │
│                            ▼                               │
│                     ┌─────────────┐                        │
│                     │ Checkpoint  │                        │
│                     └─────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

## License

MIT
