# Bronze Copier v1.1 - Betting Table

**Date:** 2025-11-23
**Facilitator:** Tillman
**Total Available Appetite:** 2 weeks (1 cycle)

---

## Context

v1.0 shipped successfully. Code review identified architectural improvements needed for production-grade quality. This cycle addresses those improvements.

---

## Pitches on the Table

| # | Pitch | Appetite | Risk | Dependencies | Recommendation |
|---|-------|----------|------|--------------|----------------|
| 06 | Era Boundary Config | 2 days | Low | None | **BET** |
| 07 | Worker Pool + Sequencer | 3 days | Medium | None | **BET** |
| 08 | Structured Logging | 2 days | Low | None | **BET** |
| 09 | Prometheus Metrics | 2 days | Low | 08 | **BET** |
| 10 | Atomic Storage Implementation | 2 days | Medium | None | **BET** |

**Total if all bet:** ~11 days of work

---

## Cycle Allocation

### Cycle 4: Production Hardening (2 weeks)

**Bets:**
- Pitch 06: Era Boundary Config (2 days)
- Pitch 07: Worker Pool + Sequencer (3 days)
- Pitch 08: Structured Logging (2 days)
- Pitch 09: Prometheus Metrics (2 days)
- Pitch 10: Atomic Storage Implementation (2 days)

**Goal:** Production-grade observability and reliability
**Demo:** Multi-era backfill with Grafana dashboard, atomic writes verified

**Scope cuts if behind at midpoint:**
- Cut multi-era processing (keep single era with boundary config)
- Cut Prometheus metrics (keep structured logging)
- Simplify worker pool (keep current parallel committer)

**Circuit breaker:** If worker pool causes deadlocks, revert to sequential mode

---

## Cool-down Period

**After Cycle 4 (2-3 days):**
- Bug fixes from hardening
- Performance profiling with metrics
- Documentation updates
- v1.1 release preparation

---

## What We're NOT Betting On (Icebox)

Explicitly **not** in scope for v1.1:

| Idea | Why not now |
|------|-------------|
| Live gRPC streaming | Archives are priority |
| Multiple Hubble tables | ledgers_lcm_raw sufficient |
| OpenTelemetry tracing | Too complex, Prometheus enough |
| Dynamic worker scaling | Fixed pool is simpler |
| Web monitoring UI | Grafana dashboards sufficient |
| Auto era detection | Manual config is reliable |

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Worker pool deadlocks | Low | High | Use proven patterns, extensive testing |
| slog breaking changes | Very Low | Low | slog is stdlib, stable |
| Prometheus cardinality | Low | Medium | Limit label combinations |
| GCS atomic copy failures | Low | Medium | Implement retry logic |

---

## Success Metrics

### End of Cycle 4
- [ ] Era boundary config works with multiple eras defined
- [ ] Worker pool processes partitions with explicit sequencing
- [ ] All logs in structured JSON format
- [ ] Prometheus metrics endpoint functional
- [ ] Atomic storage implemented for local + GCS + S3
- [ ] No regressions in existing functionality

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2025-11-23 | Use slog over zap/zerolog | stdlib, no dependencies |
| 2025-11-23 | Use Prometheus over OpenTelemetry | Simpler, matches existing infra |
| 2025-11-23 | Keep single-cycle for v1.1 | Focused scope, ship fast |

---

## Approval

- [ ] Cycle 4 scope approved
- [ ] Cool-down period scheduled
- [ ] Circuit breakers understood

**Sign-off:** _________________ Date: _________
