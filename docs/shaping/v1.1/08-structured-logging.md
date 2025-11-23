# Pitch 08: Structured Logging

**Appetite:** 2 days
**Risk:** Low
**Dependencies:** None

---

## Problem

Current logging uses Go's standard `log` package:

```go
log.Printf("[copier] progress: %d ledgers, %d partitions, %.2f ledgers/sec",
    ledgerCount, partitionCount, rate)
```

This is:
- Hard to parse programmatically
- No log levels (everything is INFO)
- No structured fields for filtering
- No correlation IDs for tracing requests
- Inconsistent format across packages

For production, we need logs that work with:
- Log aggregation (Loki, CloudWatch, etc.)
- Alerting on specific fields
- Distributed tracing

---

## Solution

### Use slog (Go 1.21+ structured logging)

Replace `log` with `log/slog`:

```go
// Before
log.Printf("[copier] progress: %d ledgers, %d partitions", count, parts)

// After
slog.Info("copier progress",
    "ledgers", count,
    "partitions", parts,
    "rate", rate,
    "era_id", cfg.Era.EraID,
)
```

### Output Format

JSON for production:
```json
{
  "time": "2025-01-15T10:30:00Z",
  "level": "INFO",
  "msg": "copier progress",
  "ledgers": 10000,
  "partitions": 1,
  "rate": 52.43,
  "era_id": "pre_p23",
  "correlation_id": "abc123"
}
```

Text for development:
```
2025/01/15 10:30:00 INFO copier progress ledgers=10000 partitions=1 rate=52.43 era_id=pre_p23
```

### Correlation ID

Add a correlation ID that flows through the entire partition lifecycle:

```go
type PartitionContext struct {
    CorrelationID string
    EraID         string
    VersionLabel  string
    LedgerStart   uint32
    LedgerEnd     uint32
}

func (c *Copier) publishPartition(ctx context.Context, part tables.Partition) error {
    pctx := &PartitionContext{
        CorrelationID: generateCorrelationID(),
        EraID:         c.cfg.Era.EraID,
        // ...
    }

    logger := slog.With(
        "correlation_id", pctx.CorrelationID,
        "era_id", pctx.EraID,
        "ledger_start", part.Start,
        "ledger_end", part.End,
    )

    logger.Info("partition publish started")
    // ... all subsequent logs use logger
    logger.Info("partition publish completed", "duration_ms", elapsed)
}
```

### Log Levels

- **DEBUG:** Detailed tracing (ledger-by-ledger)
- **INFO:** Normal operations (partition commits, progress)
- **WARN:** Recoverable issues (retry, fallback)
- **ERROR:** Failures that need attention

---

## Scope

### Must Have
- Replace `log` with `slog` throughout codebase
- JSON output format for production
- Text output format for development (configurable)
- Correlation ID per partition
- Consistent field names

### Nice to Have
- Log level configuration via env var
- Per-package log level override
- Request ID propagation through context

### Not Doing
- Distributed tracing (OpenTelemetry)
- Log shipping (leave to infrastructure)
- Log rotation (leave to systemd/logrotate)

---

## Config

```yaml
logging:
  format: "json"          # "json" | "text"
  level: "info"           # "debug" | "info" | "warn" | "error"
```

Or via environment:
```bash
LOG_FORMAT=json LOG_LEVEL=debug ./bronze-copier
```

---

## Standard Fields

All logs should include when relevant:

| Field | Type | Description |
|-------|------|-------------|
| `correlation_id` | string | Unique ID per partition |
| `era_id` | string | Era being processed |
| `version_label` | string | Bronze version |
| `network` | string | pubnet/testnet |
| `ledger_start` | int | Partition start |
| `ledger_end` | int | Partition end |
| `duration_ms` | int | Operation duration |
| `error` | string | Error message if any |

---

## Done When

1. All `log.Printf` replaced with `slog`
2. JSON format works in production
3. Correlation ID present in all partition logs
4. Log level configurable
5. Test: Can filter logs by correlation_id in JSON output
