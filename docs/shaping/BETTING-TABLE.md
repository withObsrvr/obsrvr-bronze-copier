# Bronze Copier v1 - Betting Table

**Date:** 2025-01-15
**Facilitator:** Tillman
**Total Available Appetite:** 6 weeks (3 × 2-week cycles)

---

## Pitches on the Table

| # | Pitch | Appetite | Risk | Dependencies | Recommendation |
|---|-------|----------|------|--------------|----------------|
| 01 | Archive Sources | 1 week | Low | None | **BET** |
| 02 | Parquet Transform | 1 week | Medium | 01 | **BET** |
| 03 | Storage Backends | 3 days | Low | None | **BET** |
| 04 | Catalog & Checkpoint | 1 week | Medium | 03 | **BET** |
| 05 | PAS Emitter | 3 days | Low | 04 | **BET** |

**Total if all bet:** ~4 weeks of work

---

## Recommended Cycle Allocation

### Cycle 1: Data Flow MVP (2 weeks)

**Bets:**
- Pitch 01: Archive Sources (1 week)
- Pitch 02: Parquet Transform (1 week)
- Pitch 03: Storage Backends (3 days) - parallel with 01

**Goal:** End-to-end data flow working
**Demo:** Read 10k ledgers from local archive, output parquet to local filesystem

**Scope cuts if behind at midpoint:**
- Cut S3 source (keep local + GCS)
- Cut compression options (use snappy only)
- Cut parallel prefetching

**Circuit breaker:** If parquet library doesn't support deterministic output, escalate immediately

---

### Cycle 2: Production Readiness (2 weeks)

**Bets:**
- Pitch 04: Catalog & Checkpoint (1 week)
- Integration + hardening (1 week)
  - GCS/S3 storage integration testing
  - Real mainnet archive testing
  - Error handling improvements

**Goal:** Can run unattended backfill with resume
**Demo:** Backfill 1M ledgers, kill mid-run, resume cleanly

**Scope cuts if behind at midpoint:**
- Cut _meta_quality table
- Cut _meta_changes table
- Cut object storage checkpoint (local file only)

**Circuit breaker:** If PostgreSQL performance is issue, consider SQLite alternative

---

### Cycle 3: Audit & Polish (2 weeks)

**Bets:**
- Pitch 05: PAS Emitter (3 days)
- Parallel commits (4 days)
- CLI/Config improvements (3 days)
- Documentation + operational runbooks (3 days)

**Goal:** Production-ready with audit trail
**Demo:** Full testnet backfill with PAS verification, parallel commit speedup measured

**Scope cuts if behind at midpoint:**
- Cut parallel commits (stay single-threaded)
- Cut YAML config (env vars only)
- Simplify docs to README only

**Circuit breaker:** If PAS service not ready, emit to file only

---

## Cool-down Periods

**After Cycle 1 (2-3 days):**
- Bug fixes from MVP testing
- Dependency updates
- Code review and cleanup

**After Cycle 2 (2-3 days):**
- Performance profiling
- Memory optimization
- Test coverage improvements

**After Cycle 3 (2-3 days):**
- Final documentation review
- Release preparation
- Handoff documentation

---

## What We're NOT Betting On (Icebox)

These ideas are explicitly **not** in scope for v1:

| Idea | Why not now |
|------|-------------|
| Live gRPC streaming | Archives are foundation, live can wait |
| Full 19 Hubble tables | ledgers_lcm_raw is sufficient for v1 |
| Multi-era concurrent processing | Single era per copier instance is fine |
| Web monitoring UI | CLI + logs + metrics are enough |
| Automatic era boundary detection | Manual config is acceptable |
| Multi-writer coordination | Single writer per era is assumed |
| IPFS pinning for PAS | v1.2 scope |

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Galexie format changes | Low | High | Pin to known format, version schema |
| Parquet non-determinism | Medium | High | Spike early, have fallback plan |
| GCS/S3 auth complexity | Low | Medium | Use gocloud.dev, document setup |
| PostgreSQL performance | Low | Medium | Index properly, consider batching |
| PAS service unavailable | Medium | Low | File backup, defer emission |

---

## Success Metrics

### End of Cycle 1
- [ ] Can read 10k ledgers from local archive
- [ ] Produces valid parquet files
- [ ] Checksums are deterministic (same input = same checksum)

### End of Cycle 2
- [ ] Can resume from checkpoint after crash
- [ ] Lineage records queryable in catalog
- [ ] Works with GCS and S3 sources/destinations

### End of Cycle 3
- [ ] PAS events emitted for all partitions
- [ ] Full testnet backfill completed
- [ ] Documentation sufficient for handoff

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2025-01-15 | Use gocloud.dev for storage | Unified interface, less code |
| 2025-01-15 | parquet-go over DuckDB | Pure Go, no CGO complexity |
| 2025-01-15 | PostgreSQL for catalog | Matches existing DuckLake infra |
| 2025-01-15 | File-based checkpoint | Simpler than DB, reliable |
| 2025-01-15 | Single ledgers_lcm_raw table for v1 | Ship fast, expand later |

---

## Questions Requiring Answers Before Betting

1. **Is Galexie landing bucket format stable?**
   - Need: Confirmation of file naming convention
   - Blocker for: Pitch 01

2. **Is PAS v1.1 spec finalized?**
   - Need: Event schema confirmation
   - Blocker for: Pitch 05

3. **Do we have test archive data?**
   - Need: Sample XDR files for unit tests
   - Blocker for: All pitches

4. **PostgreSQL instance available for catalog?**
   - Need: Connection details for dev/staging
   - Blocker for: Pitch 04

---

## Approval

- [ ] Cycle 1 scope approved
- [ ] Cycle 2 scope approved
- [ ] Cycle 3 scope approved
- [ ] Cool-down periods scheduled
- [ ] Circuit breakers understood

**Sign-off:** _________________ Date: _________
