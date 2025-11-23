# Bronze Copier - Developer Handoff Document

**Phase:** Cycle 1 Complete - Data Flow MVP with GCS Integration
**Date:** 2025-11-23
**Author:** Claude Code

---

## Executive Summary

This document summarizes the completion of **Cycle 1** of the Bronze Copier implementation. The MVP data flow is now fully functional: the system can read ledger archives from GCS using the official Stellar datastore package, transform them to parquet, and write to storage with checkpointing.

**Key Achievement:** Successfully tested end-to-end with real testnet data from `obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet`.

---

## What Was Completed

### 1. Archive Sources (Pitch 01)

**Files:**
- `internal/source/source.go` - Interface and factory
- `internal/source/datastore.go` - **NEW: Official Stellar datastore integration (recommended)**
- `internal/source/decoder.go` - XDR/zstd decoding
- `internal/source/index.go` - Galexie file indexing
- `internal/source/local.go` - Local filesystem source
- `internal/source/gcs.go` - Direct GCS source (legacy)
- `internal/source/s3.go` - S3-compatible source (B2, R2, MinIO)

**Capabilities:**
- **Datastore mode** (recommended): Uses official Stellar `ledgerbackend.BufferedStorageBackend`
- Reads zstd-compressed XDR files from local/GCS/S3
- Parses Galexie filename format (`{hash}--{ledger_seq}.xdr.zst`)
- Streams ledgers in sequence order with buffered parallel reads
- Validated with real testnet data

**Galexie Schema:**
- 1 ledger per file
- 64000 files per partition (range directory)

### 2. Parquet Transform (Pitch 02)

**Files:**
- `internal/tables/schema.go` - `LedgersLCMRawRow` schema definition
- `internal/tables/extractor.go` - XDR to row extraction
- `internal/tables/partition.go` - Partition building and parquet generation
- `internal/tables/checksum.go` - SHA256 checksum utilities

**Capabilities:**
- Transforms `LedgerCloseMeta` to parquet rows
- Supports LCM versions 0, 1, and 2
- Deterministic output (same input = same checksum)
- Snappy compression by default
- Extracts header fields for queryability while preserving raw XDR

### 3. Storage Backends (Pitch 03)

**Files:**
- `internal/storage/store.go` - Interface and types
- `internal/storage/local.go` - Local filesystem storage
- `internal/storage/gcs.go` - GCS storage
- `internal/storage/s3.go` - S3-compatible storage

**Capabilities:**
- Atomic writes (temp file + rename for local)
- Versioned path structure: `{prefix}/{network}/{era}/{version}/{table}/range={start}-{end}/`
- Manifest file generation with checksums
- Duplicate detection

### 4. Checkpointing (Pitch 04)

**Files:**
- `internal/checkpoint/checkpoint.go` - Checkpoint manager

**Capabilities:**
- Saves progress after each partition
- **Tested: Resumes from last committed ledger on restart**
- Per-era/version checkpoint files
- Validates checkpoint matches current config

### 5. Configuration

**Files:**
- `internal/config/config.go` - YAML and environment variable config
- `config.example.yaml` - Example configuration

**Capabilities:**
- YAML file or environment variable configuration
- All source/storage options configurable
- **New: `datastore` source mode for Galexie archives**
- Era/version/network settings
- Partition size control

---

## What Was NOT Completed (Deferred to Cycle 2+)

### From Original Scope

1. **Catalog Metadata Writer** - `_meta_*` tables not implemented
   - Currently a no-op implementation
   - Needed for: lineage tracking, coverage queries

2. **PAS Emitter** - No actual PAS emission
   - Currently a no-op implementation
   - Needed for: public audit stream, verification

3. **Parallel Commits** - Single-threaded only
   - `MaxInFlightPartitions` config exists but not used
   - Needed for: throughput optimization

4. **Live Source** - Only archive sources implemented
   - No gRPC streaming support
   - Needed for: real-time tailing

---

## Dependencies

```
github.com/stellar/go v0.0.0-20251121214806-3e8d3b7222e6  # XDR types + datastore
github.com/klauspost/compress v1.17.11                    # zstd decompression
github.com/parquet-go/parquet-go v0.24.0                  # Parquet writing
gocloud.dev v0.40.0                                       # GCS/S3 abstraction
gopkg.in/yaml.v3 v3.0.1                                   # YAML config
```

---

## How to Test

### GCS Testnet (Recommended)

```bash
# Build
go build -o bronze-copier ./cmd/bronze-copier

# Run with datastore mode (uses official Stellar datastore)
SOURCE_MODE=datastore \
SOURCE_DATASTORE_TYPE=GCS \
SOURCE_DATASTORE_PATH=obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet \
STORAGE_BACKEND=local \
STORAGE_LOCAL_DIR=./output \
NETWORK=testnet \
ERA_ID=pre_p23 \
VERSION_LABEL=v1 \
LEDGER_START=704000 \
LEDGER_END=704100 \
PARTITION_SIZE=100 \
CHECKPOINT_ENABLED=true \
CHECKPOINT_DIR=./checkpoints \
./bronze-copier
```

### Local Development

```bash
# Run with local source and storage
SOURCE_MODE=local \
SOURCE_LOCAL_PATH=./testdata/archive \
STORAGE_BACKEND=local \
STORAGE_LOCAL_DIR=./output \
LEDGER_START=1 \
LEDGER_END=100 \
PARTITION_SIZE=10 \
./bronze-copier
```

### Test Data Requirements

For local testing, you need Galexie-format archive files:
```
testdata/archive/
  {hash}--{start}-{end}/
    {hash}--{ledger_seq}.xdr.zst
```

For GCS testing, use the testnet bucket: `obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet`

---

## Test Results

### End-to-End Test (2025-11-23)

```
Source: gs://obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet
Range: 704000-704020
Result: SUCCESS

Output:
- ./output/bronze/testnet/pre_p23/v1/ledgers_lcm_raw/range=704000-704009/
- ./output/bronze/testnet/pre_p23/v1/ledgers_lcm_raw/range=704010-704010/
- ./output/bronze/testnet/pre_p23/v1/ledgers_lcm_raw/range=704011-704020/

Throughput: ~29 ledgers/sec
Resume: Verified working
Checksums: Verified deterministic
```

---

## Known Issues / Technical Debt

1. **No unit tests** - Tests should be added
2. **Error handling could be improved** - Some errors are logged but not propagated
3. **No metrics** - Prometheus metrics not implemented
4. **Flake.nix vendorHash** - Needs update for nix build
5. **Legacy GCS/S3 sources** - Direct blob access works but datastore mode is preferred

---

## Recommended Next Steps (Cycle 2)

### Priority 1: Catalog Writer
- Implement PostgreSQL writer for `_meta_lineage`
- Add `_meta_datasets` registration
- Enable coverage queries

### Priority 2: Scale Testing
- Run against full pubnet archive
- Measure throughput at scale
- Tune `BufferSize` and `NumWorkers`

### Priority 3: Metrics & Observability
- Add Prometheus metrics
- Implement structured logging
- Add health check endpoint

---

## File Structure

```
obsrvr-bronze-copier/
├── cmd/bronze-copier/
│   └── main.go                 # Entry point
├── internal/
│   ├── checkpoint/
│   │   └── checkpoint.go       # Checkpoint management
│   ├── config/
│   │   └── config.go           # Configuration loading
│   ├── copier/
│   │   └── copier.go           # Main orchestration
│   ├── metadata/
│   │   └── writer.go           # Catalog writer (no-op)
│   ├── pas/
│   │   └── emitter.go          # PAS emitter (no-op)
│   ├── source/
│   │   ├── datastore.go        # Official Stellar datastore (recommended)
│   │   ├── decoder.go          # XDR decoding
│   │   ├── gcs.go              # Direct GCS source
│   │   ├── index.go            # File indexing
│   │   ├── local.go            # Local source
│   │   ├── s3.go               # S3 source
│   │   └── source.go           # Interface
│   ├── storage/
│   │   ├── gcs.go              # GCS storage
│   │   ├── local.go            # Local storage
│   │   ├── s3.go               # S3 storage
│   │   └── store.go            # Interface
│   ├── tables/
│   │   ├── checksum.go         # Checksum utils
│   │   ├── extractor.go        # XDR extraction
│   │   ├── partition.go        # Partition building
│   │   └── schema.go           # Parquet schema
│   └── util/
│       ├── filesystem.go
│       ├── hash.go
│       └── strconv.go
├── docs/
│   ├── shaping/                # Shape Up documents
│   └── DEVELOPER-HANDOFF.md    # This document
├── config.example.yaml
├── flake.nix
├── go.mod
├── go.sum
└── README.md
```

---

## Contact

For questions about this implementation:
- Review Shape Up documents in `docs/shaping/`
- Check README.md for usage
- Consult PRD v2 in chat history
