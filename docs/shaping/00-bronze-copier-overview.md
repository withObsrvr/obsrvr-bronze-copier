# Bronze Copier v1 - Project Shaping Overview

**Project:** obsrvr-bronze-copier
**Owner:** Tillman / Obsrvr Core
**Status:** Shaped, ready for betting
**Total Appetite:** 6 weeks (3 × 2-week cycles)

---

## Executive Summary

The Bronze Copier is Obsrvr's trust foundation - a standalone service that copies raw Stellar ledger data from archive sources (Galexie landing buckets) to versioned Bronze storage, producing immutable parquet files with full lineage tracking and audit proofs.

**Current state:** Skeleton with no-op implementations
**Target state:** Production-ready Bronze Copier v1 that can backfill history and tail live

---

## Project Breakdown

The project is shaped into **5 independent pitches** that can be bet on separately:

| Pitch | Appetite | Dependencies | Risk |
|-------|----------|--------------|------|
| 01 - Archive Sources | 1 week | None | Low |
| 02 - Parquet Transform | 1 week | 01 | Medium |
| 03 - Storage Backends | 3 days | None | Low |
| 04 - Catalog & Checkpoint | 1 week | 03 | Medium |
| 05 - PAS Emitter | 3 days | 04 | Low |

**Critical path:** 01 → 02 → (03 parallel) → 04 → 05

---

## Recommended Betting Strategy

### Cycle 1 (2 weeks): Data Flow MVP
- **Bet:** Pitches 01 + 02 + 03
- **Goal:** Can read archives, transform to parquet, write to storage
- **Demo:** `bronze-copier` reads 1000 ledgers from local archive, outputs parquet to local filesystem

### Cycle 2 (2 weeks): Production Readiness
- **Bet:** Pitch 04 (expanded) + integration
- **Goal:** Resume from checkpoint, track lineage, GCS/S3 backends working
- **Demo:** Backfill 1M ledgers with restart, lineage queryable

### Cycle 3 (2 weeks): Audit & Scale
- **Bet:** Pitch 05 + parallel commits + hardening
- **Goal:** PAS emission, parallel writes, operational tooling
- **Demo:** Full mainnet backfill with PAS verification

---

## Scope Line (Project-wide)

```
MUST HAVE ══════════════════════════════════════════════════════════
- Read ledgers from local/GCS/S3 archives (zstd XDR files)
- Output ledgers_lcm_raw parquet table with checksums
- Write to local filesystem and object storage (GCS/S3/B2)
- Resume from last committed ledger
- Basic lineage tracking

NICE TO HAVE ───────────────────────────────────────────────────────
- Full Hubble-parity Bronze tables (19 tables)
- Parallel partition commits
- PAS v1.1 emission with hash chaining
- YAML configuration file support
- Prometheus metrics

COULD HAVE (Cut first) ─────────────────────────────────────────────
- Live source (gRPC streaming)
- Multi-era concurrent processing
- Web UI for monitoring
- Automatic era boundary detection
```

---

## Technical Decisions (Pre-shaped)

These decisions are locked to reduce scope creep:

1. **XDR Decoding:** Use `github.com/stellar/go/xdr` - standard ecosystem choice
2. **Parquet:** Use `github.com/parquet-go/parquet-go` - pure Go, no CGO
3. **Compression:** `github.com/klauspost/compress/zstd` - fast, well-maintained
4. **Object Storage:** `gocloud.dev/blob` - unified interface for GCS/S3/local
5. **Config:** Environment variables first, YAML as enhancement (not blocker)

---

## No-Gos (Explicit out of scope)

- **No XDR transformation beyond raw preservation** - Bronze stores bytes, Silver interprets
- **No query API** - Bronze is write-only, readers use parquet directly
- **No real-time requirements** - Batch processing is acceptable
- **No backwards compatibility with old formats** - Fresh start for v1
- **No multi-tenant isolation** - Single network per deployment

---

## Rabbit Holes to Avoid

1. **Don't build a generic ETL framework** - This is a single-purpose copier
2. **Don't optimize prematurely** - Get correct first, fast second
3. **Don't implement all 19 Hubble tables in v1** - Start with `ledgers_lcm_raw`
4. **Don't build live streaming before archive works** - Archives are the foundation
5. **Don't over-engineer error handling** - Fail fast, retry at partition level

---

## Success Criteria (Definition of Done)

### v1.0 Release Criteria
- [ ] Can backfill 10M ledgers from GCS archive without manual intervention
- [ ] Produces deterministic parquet output (same input = same checksum)
- [ ] Resumes cleanly after crash/restart
- [ ] Lineage records queryable for any partition
- [ ] Documentation covers: setup, backfill, monitoring, correction procedure

### Stretch Goals (v1.1)
- [ ] PAS events emitted for all partitions
- [ ] Parallel commits achieve 3x throughput improvement
- [ ] Full Hubble Bronze table parity

---

## Files in this Directory

- `00-bronze-copier-overview.md` - This document
- `01-archive-sources.md` - Pitch: Archive source implementations
- `02-parquet-transform.md` - Pitch: LCM to Parquet conversion
- `03-storage-backends.md` - Pitch: Storage backend implementations
- `04-catalog-checkpoint.md` - Pitch: Metadata catalog and checkpointing
- `05-pas-emitter.md` - Pitch: Public Audit Stream emission
