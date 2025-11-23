# Bronze Copier - Developer Handoff Document

**Phase:** Cycle 3 Complete - Production Ready with Audit Trail
**Date:** 2025-11-23
**Author:** Claude Code

---

## Executive Summary

This document summarizes the completion of **all three cycles** of the Bronze Copier implementation. The system is now production-ready with:

- Full data flow from Galexie archives to versioned parquet output
- PostgreSQL catalog for metadata tracking and lineage
- Hash-chained PAS (Public Audit Stream) events for tamper detection
- Parallel commit processing for improved throughput
- CLI with config validation

**Status:** Ready for production deployment

---

## Cycle Completion Summary

### Cycle 1: Data Flow MVP ✅

- Archive sources (datastore, GCS, S3, local)
- Parquet transform with deterministic checksums
- Storage backends with manifest files
- Checkpointing for resume capability

### Cycle 2: Production Readiness ✅

- PostgreSQL catalog integration
- Lineage tracking (source -> partition records)
- Dataset registration

### Cycle 3: Audit & Polish ✅

- PAS Emitter with hash chaining
- Parallel commits for throughput
- CLI flags (`-config`, `-version`, `-validate`)
- Config validation
- Documentation and runbooks

---

## What Was Implemented

### PAS (Public Audit Stream)

**Files:**
- `internal/pas/event.go` - PAS v1.1 event schema
- `internal/pas/chain.go` - SHA256 hash computation and chain tracking
- `internal/pas/http.go` - HTTP emitter with retry
- `internal/pas/file.go` - Local file backup
- `internal/pas/emitter.go` - Factory and interface

**Capabilities:**
- Emits hash-chained events for each partition
- Supports HTTP endpoint with exponential backoff retry
- Always writes local backup files
- Persists chain head hashes for continuity across restarts
- Creates tamper-evident audit trail

**Event Structure (v1.1):**
```json
{
  "version": "1.1",
  "event_type": "bronze_partition",
  "event_id": "pas_evt_...",
  "timestamp": "...",
  "partition": { network, era_id, version_label, ledger_start, ledger_end },
  "tables": { "ledgers_lcm_raw": { checksum, row_count, storage_path, byte_size } },
  "producer": { name, version, git_sha },
  "chain": { prev_event_hash, event_hash }
}
```

### Parallel Commits

**Files:**
- `internal/copier/parallel.go` - ParallelCommitter implementation
- `internal/copier/copier.go` - Integration with `runParallel()`

**Capabilities:**
- Semaphore-based concurrency control
- Configurable via `MAX_IN_FLIGHT_PARTITIONS` (default: 4)
- Parallel parquet generation and storage writes
- Sequential PAS emission (maintains hash chain)
- Thread-safe result tracking with condition variables

**Performance:**
- Sequential: ~29 ledgers/sec
- Parallel (4 workers): ~53 ledgers/sec

### CLI Improvements

**Files:**
- `cmd/bronze-copier/main.go` - CLI flag handling
- `internal/config/config.go` - Validation function

**Capabilities:**
- `-config path` - Specify YAML config file
- `-version` - Show version and exit
- `-validate` - Validate config without running
- Comprehensive config validation with clear error messages

### Catalog Metadata

**Files:**
- `internal/metadata/writer.go` - PostgreSQL writer
- `internal/metadata/schema.go` - Database schema

**Capabilities:**
- Dataset registration (domain, table, version, era)
- Partition record tracking (checksums, row counts, byte sizes)
- Lineage information (source type, source location)
- Auto-creates tables on first use

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            Bronze Copier                                  │
│                                                                           │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                   │
│  │   Source    │───▶│  Partition  │───▶│   Storage   │                   │
│  │ (Datastore) │    │  Builder    │    │ (GCS/S3/FS) │                   │
│  └─────────────┘    └─────────────┘    └─────────────┘                   │
│         │                  │                  │                           │
│         ▼                  ▼                  ▼                           │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                   │
│  │   Decoder   │    │   Parquet   │    │  Manifest   │                   │
│  │  (XDR/zstd) │    │   Writer    │    │   Writer    │                   │
│  └─────────────┘    └─────────────┘    └─────────────┘                   │
│                            │                                              │
│              ┌─────────────┼─────────────┬─────────────┐                 │
│              ▼             ▼             ▼             ▼                 │
│       ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐           │
│       │ Checkpoint│ │  Catalog  │ │    PAS    │ │  Parallel │           │
│       │  (JSON)   │ │(PostgreSQL│ │ (HTTP/FS) │ │ Committer │           │
│       └───────────┘ └───────────┘ └───────────┘ └───────────┘           │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## File Structure

```
obsrvr-bronze-copier/
├── cmd/bronze-copier/
│   └── main.go                 # Entry point with CLI flags
├── internal/
│   ├── checkpoint/
│   │   └── checkpoint.go       # Checkpoint management
│   ├── config/
│   │   └── config.go           # Config + validation
│   ├── copier/
│   │   ├── copier.go           # Main orchestration
│   │   └── parallel.go         # Parallel commit handler
│   ├── metadata/
│   │   ├── writer.go           # PostgreSQL catalog
│   │   └── schema.go           # DB schema
│   ├── pas/
│   │   ├── event.go            # PAS v1.1 types
│   │   ├── chain.go            # Hash chain tracking
│   │   ├── http.go             # HTTP emitter
│   │   ├── file.go             # File backup
│   │   └── emitter.go          # Interface + factory
│   ├── source/
│   │   ├── datastore.go        # Stellar datastore (recommended)
│   │   ├── decoder.go          # XDR decoding
│   │   ├── gcs.go, s3.go, local.go
│   │   └── source.go           # Interface
│   ├── storage/
│   │   ├── gcs.go, s3.go, local.go
│   │   └── store.go            # Interface
│   └── tables/
│       ├── schema.go           # Parquet schema
│       ├── extractor.go        # XDR extraction
│       └── partition.go        # Partition building
├── docs/
│   ├── shaping/                # Shape Up documents
│   ├── DEVELOPER-HANDOFF.md    # This document
│   └── RUNBOOK.md              # Operational runbook
├── config.example.yaml
├── flake.nix
└── README.md
```

---

## How to Test

### Full Integration Test

```bash
# Build
nix build
cp result/bin/bronze-copier ./bronze-copier

# Run with all features
SOURCE_MODE=datastore \
SOURCE_DATASTORE_TYPE=GCS \
SOURCE_DATASTORE_PATH=obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet \
STORAGE_BACKEND=local \
STORAGE_LOCAL_DIR=./output \
NETWORK=testnet \
ERA_ID=pre_p23 \
VERSION_LABEL=v1 \
LEDGER_START=704000 \
LEDGER_END=704030 \
PARTITION_SIZE=10 \
PAS_ENABLED=true \
PAS_BACKUP_DIR=./pas-backup \
MAX_IN_FLIGHT_PARTITIONS=4 \
./bronze-copier

# Verify PAS hash chain
for f in pas-backup/*.json; do
    echo "=== $f ==="
    jq '.chain' "$f"
done
```

### Config Validation

```bash
./bronze-copier -config config.example.yaml -validate
```

---

## Test Results (2025-11-23)

### End-to-End Test

```
Source: gs://obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet
Range: 704000-704030
Mode: Parallel (4 workers)
Result: SUCCESS

Output: 4 partitions
- range=704000-704009 (10 ledgers)
- range=704010-704019 (10 ledgers)
- range=704020-704029 (10 ledgers)
- range=704030-704030 (1 ledger)

PAS Events: 4 events, hash chain verified
Throughput: 53 ledgers/sec
```

---

## Known Limitations

1. **No unit tests** - Integration tests only
2. **No Prometheus metrics** - Logs only
3. **Single table only** - `ledgers_lcm_raw` only (other tables in icebox)
4. **No live streaming** - Archive sources only

---

## Recommended Next Steps

### Immediate

1. Deploy to staging with real pubnet data
2. Set up monitoring (watch checkpoint age, PAS events)
3. Configure alerting for stuck backfills

### Future Enhancements

1. **Prometheus metrics** - ledgers/sec, partition latency, error counts
2. **Multiple tables** - transactions, operations, etc.
3. **Live streaming** - gRPC source for real-time tailing
4. **Web UI** - Progress dashboard

---

## Commits (Cycle 3)

1. `ed8e46f` - Implement Cycle 3 Part 1: PAS Emitter with hash chaining
2. `4dfcd70` - Add parallel commits infrastructure for improved throughput
3. `dbf8f4b` - Integrate parallel commits into main copier loop
4. `fd04f41` - Add CLI flags and config validation

---

## Documentation

- **README.md** - User guide with all features
- **docs/RUNBOOK.md** - Operational runbook
- **config.example.yaml** - Reference configuration
- **docs/shaping/** - Shape Up design documents

---

## Contact

For questions:
- GitHub Issues: https://github.com/withObsrvr/obsrvr-bronze-copier/issues
- Design docs: `docs/shaping/`
