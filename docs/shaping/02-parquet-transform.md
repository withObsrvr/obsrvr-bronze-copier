# Pitch 02: Parquet Transform

**Appetite:** 1 week
**Risk:** Medium (schema decisions have long-term implications)
**Dependencies:** Pitch 01 (Archive Sources)

---

## Problem

The Bronze Copier has a `ToParquet()` method that returns empty maps. Without parquet generation, the copier produces no output.

Bronze needs to transform `LedgerCloseMeta` XDR into parquet files that:
1. **Preserve raw data** - No information loss from source
2. **Enable efficient queries** - Columnar format for analytical access
3. **Support versioning** - Schema tied to protocol era
4. **Generate checksums** - Deterministic output for verification

The key tension: Bronze should be "raw preservation" but parquet requires schema decisions. We resolve this by creating a minimal `ledgers_lcm_raw` table that preserves bytes while adding queryable metadata.

---

## Appetite

**1 week** - Schema design is the hard part; parquet writing is mechanical.

We will NOT:
- Implement all 19 Hubble Bronze tables (that's v1.1+)
- Build DuckDB integration (separate from parquet generation)
- Optimize for query performance (correctness first)

---

## Solution

### Fat-marker sketch

```
┌─────────────────────────────────────────────────────────────┐
│                      Partition                               │
│  Ledgers: []LedgerCloseMeta (10,000 ledgers per partition)  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    TableWriters                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │ ledgers_lcm_raw │  │ (future tables) │  │    ...      │ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    ParquetOutput                             │
│  parquets:   map[table][]byte    (parquet file bytes)       │
│  checksums:  map[table]string    (sha256 of parquet)        │
│  rowCounts:  map[table]int64     (rows per table)           │
└─────────────────────────────────────────────────────────────┘
```

### Core components

1. **Table Schema Registry**
   - Define parquet schemas per Bronze version
   - `ledgers_lcm_raw` for v1 (minimal, raw-preserving)
   - Extensible for future Hubble-parity tables

2. **Row Extractor**
   - Transform `LedgerCloseMeta` → table rows
   - Extract header fields (seq, hash, timestamp, etc.)
   - Preserve raw XDR bytes as binary column

3. **Parquet Writer**
   - Write rows to in-memory parquet buffer
   - Use deterministic settings (no compression variance)
   - Return bytes for storage layer

4. **Checksum Generator**
   - SHA256 over parquet bytes
   - Stable ordering within partition (by ledger seq)

---

## Schema: `ledgers_lcm_raw` (v1)

This is the foundational Bronze table. It preserves everything while enabling basic queries.

```
ledgers_lcm_raw (v1)
├── ledger_sequence      : INT64      (partition key, sortable)
├── ledger_hash          : STRING     (hex-encoded, for verification)
├── previous_ledger_hash : STRING     (hex-encoded, chain verification)
├── close_time           : TIMESTAMP  (ledger close time)
├── protocol_version     : INT32      (protocol at close time)
├── total_coins          : INT64      (stroops, for supply tracking)
├── fee_pool             : INT64      (stroops)
├── tx_set_hash          : STRING     (hex-encoded)
├── tx_count             : INT32      (transaction count in ledger)
├── operation_count      : INT32      (operation count in ledger)
├── xdr_bytes            : BINARY     (raw LedgerCloseMeta XDR)
├── xdr_byte_length      : INT32      (for verification)
├── xdr_sha256           : STRING     (hash of xdr_bytes)
├── bronze_version       : STRING     (e.g., "v1")
├── era_id               : STRING     (e.g., "pre_p23")
├── ingested_at          : TIMESTAMP  (when Bronze Copier processed)
```

### Schema design rationale

| Column | Why included |
|--------|--------------|
| `ledger_sequence` | Primary identifier, enables range queries |
| `ledger_hash` | Chain verification, links to downstream |
| `previous_ledger_hash` | Chain integrity checking |
| `close_time` | Time-based queries without XDR parsing |
| `protocol_version` | Era boundary detection |
| `xdr_bytes` | **THE TRUTH** - raw preservation |
| `xdr_sha256` | Deterministic verification of raw data |
| `bronze_version` | Schema versioning |
| `era_id` | Multi-era coexistence |

---

## Scope Line

```
MUST HAVE ══════════════════════════════════════════════════════════
- ledgers_lcm_raw table implementation
- Deterministic parquet output (same input = same bytes)
- SHA256 checksums per table
- Row counts per table
- Schema versioning in output

NICE TO HAVE ───────────────────────────────────────────────────────
- transactions_raw table (tx-level raw preservation)
- operations_raw table (op-level raw preservation)
- Configurable compression (snappy/zstd/none)

COULD HAVE (Cut first) ─────────────────────────────────────────────
- Full Hubble-parity tables (19 tables)
- Schema evolution helpers
- Parquet metadata annotations
```

---

## Rabbit Holes

1. **Don't decode all XDR fields** - Extract header only, store bytes raw
2. **Don't build a schema migration system** - Versions are append-only
3. **Don't optimize row group sizes** - Use sensible defaults
4. **Don't implement streaming writes** - Buffer in memory, write once
5. **Don't add nullable columns unnecessarily** - Makes checksums harder

---

## No-Gos

- No derived/computed columns beyond basic extraction
- No aggregations or rollups (that's Silver)
- No indexes or sorting hints (parquet handles this)
- No custom encodings (use parquet defaults)

---

## Technical Approach

### Dependencies to add

```go
// go.mod additions
require (
    github.com/parquet-go/parquet-go v0.21.0  // Pure Go parquet
    github.com/stellar/go v0.0.0-latest       // XDR types (from Pitch 01)
)
```

### Interface

```go
// internal/tables/partition.go

type ParquetOutput struct {
    Parquets   map[string][]byte  // table name → parquet bytes
    Checksums  map[string]string  // table name → sha256
    RowCounts  map[string]int64   // table name → row count
}

func (p Partition) ToParquet(cfg ParquetConfig) (*ParquetOutput, error)

type ParquetConfig struct {
    BronzeVersion string  // "v1"
    EraID         string  // "pre_p23"
    Compression   string  // "snappy" | "zstd" | "none"
}
```

### File structure

```
internal/tables/
├── partition.go       # Partition type + ToParquet orchestration
├── schema.go          # Schema definitions
├── ledgers_lcm_raw.go # ledgers_lcm_raw table writer
├── extractor.go       # XDR → row extraction
├── checksum.go        # Deterministic checksumming
└── tables_test.go     # Unit tests with golden files
```

### Determinism requirements

For checksums to be meaningful, parquet output must be byte-identical for identical input:

1. **Fixed row order** - Sort by ledger_sequence
2. **Fixed column order** - Schema defines order
3. **Fixed compression** - Same algorithm + level
4. **Fixed timestamps** - Use ledger close_time, not wall clock
5. **No randomness** - No UUIDs, no random sampling

---

## Done Criteria

**Concrete example of "done":**

```bash
# Create partition with 3 ledgers
$ go test -run TestToParquet_Deterministic -v

=== RUN   TestToParquet_Deterministic
--- Creating partition with ledgers [100, 101, 102]
--- First conversion: sha256=abc123def456...
--- Second conversion: sha256=abc123def456...
--- Checksums match: PASS
--- Row count: 3
--- PASS: TestToParquet_Deterministic

# Verify parquet structure
$ parquet-tools schema output.parquet
message ledgers_lcm_raw {
  required int64 ledger_sequence;
  required binary ledger_hash (STRING);
  required binary previous_ledger_hash (STRING);
  required int64 close_time (TIMESTAMP(MILLIS,true));
  required int32 protocol_version;
  required int64 total_coins;
  required int64 fee_pool;
  required binary tx_set_hash (STRING);
  required int32 tx_count;
  required int32 operation_count;
  required binary xdr_bytes;
  required int32 xdr_byte_length;
  required binary xdr_sha256 (STRING);
  required binary bronze_version (STRING);
  required binary era_id (STRING);
  required int64 ingested_at (TIMESTAMP(MILLIS,true));
}
```

**Unit test requirements:**

1. `TestToParquet_Deterministic` - Same input produces identical checksums
2. `TestToParquet_Schema` - Output matches expected parquet schema
3. `TestToParquet_RowCount` - Correct row count reported
4. `TestToParquet_EmptyPartition` - Handles zero ledgers gracefully
5. `TestExtractor_ValidLCM` - Correctly extracts header fields
6. `TestExtractor_AllProtocolVersions` - Works with v1..v21+ XDR
7. `TestGoldenFile_Ledger100` - Compare against known-good output

---

## Hill Progress Tracking

| Stage | Status | Notes |
|-------|--------|-------|
| **Left side (figuring out)** | | |
| Finalize schema for ledgers_lcm_raw | ⬜ | Review with team |
| Spike parquet-go library | ⬜ | Verify determinism possible |
| Test XDR extraction across protocol versions | ⬜ | Need sample files |
| **Right side (making it happen)** | | |
| Implement schema definitions | ⬜ | |
| Implement XDR extractor | ⬜ | |
| Implement parquet writer | ⬜ | |
| Implement checksum generator | ⬜ | |
| Create golden test files | ⬜ | |
| Integration test end-to-end | ⬜ | |

---

## Questions for Betting

1. Should `xdr_bytes` be compressed within parquet, or store raw?
   - **Recommendation:** Let parquet compress it (snappy), simpler

2. Do we need `succeeded_tx_count` vs `failed_tx_count` split?
   - **Recommendation:** No for v1, extract from XDR if needed later

3. Should we include network passphrase in schema?
   - **Recommendation:** Yes, add `network` column for multi-network support

4. What's the maximum partition size we should support?
   - **Assumption:** 10k ledgers ≈ 500MB parquet, fits in memory
