# Bronze Copier Operational Runbook

This document provides operational guidance for running Bronze Copier in production.

## Quick Reference

### Starting a New Backfill

```bash
# 1. Validate configuration
./bronze-copier -config config.yaml -validate

# 2. Run the backfill
./bronze-copier -config config.yaml
```

### Resuming After Crash

Bronze Copier automatically resumes from the last checkpoint:

```bash
# Just restart - it will resume from checkpoint
./bronze-copier -config config.yaml
```

To verify checkpoint:
```bash
cat ./state/checkpoint.json | jq .last_committed_ledger
```

### Monitoring Progress

```bash
# Watch logs for progress
tail -f /var/log/bronze-copier.log | grep "copier"

# Look for lines like:
# [copier] progress: 150000 ledgers, 15 partitions, 52.43 ledgers/sec
```

## Configuration Examples

### Testnet Backfill (GCS to Local)

```yaml
copier_id: "bronze-copier-testnet-001"

era:
  era_id: "pre_p23"
  version_label: "v1"
  network: "testnet"
  ledger_start: 1
  ledger_end: 0  # 0 = run until end of available data
  partition_size: 10000

source:
  mode: "datastore"
  datastore_type: "GCS"
  datastore_path: "obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet"

storage:
  backend: "local"
  local_dir: "/data/bronze"

checkpoint:
  enabled: true
  dir: "/data/state"

pas:
  enabled: true
  backup_dir: "/data/pas-backup"

perf:
  max_in_flight_partitions: 4
```

### Pubnet Backfill (GCS to GCS)

```yaml
copier_id: "bronze-copier-pubnet-001"

era:
  era_id: "pre_p23"
  version_label: "v1"
  network: "pubnet"
  ledger_start: 1
  ledger_end: 0
  partition_size: 10000

source:
  mode: "datastore"
  datastore_type: "GCS"
  datastore_path: "obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet"

storage:
  backend: "gcs"
  gcs_bucket: "obsrvr-bronze-output"

catalog:
  postgres_dsn: "postgres://user:pass@localhost:5432/obsrvr"

checkpoint:
  enabled: true
  dir: "/data/state"

pas:
  enabled: true
  endpoint: "https://pas.obsrvr.io/v1/events"
  backup_dir: "/data/pas-backup"

perf:
  max_in_flight_partitions: 8
```

## Troubleshooting

### Issue: Copier not starting

**Symptoms**: Process exits immediately with config error

**Check**:
```bash
./bronze-copier -config config.yaml -validate
```

**Common fixes**:
- Ensure `source.datastore_path` format is `bucket/prefix` (no `gs://`)
- Ensure `storage.local_dir` exists and is writable
- Ensure PostgreSQL DSN is valid (if using catalog)

### Issue: GCS authentication failure

**Symptoms**: Error like "could not find default credentials"

**Fix**:
```bash
# Authenticate with GCP
gcloud auth application-default login

# Or set service account key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

### Issue: Slow throughput

**Symptoms**: Less than 30 ledgers/sec

**Check**:
1. Network latency to source bucket
2. Disk I/O on storage backend
3. `MAX_IN_FLIGHT_PARTITIONS` setting

**Fix**:
```bash
# Increase parallelism (up to 8 recommended)
MAX_IN_FLIGHT_PARTITIONS=8 ./bronze-copier -config config.yaml
```

### Issue: Checkpoint not resuming

**Symptoms**: Copier starts from beginning instead of checkpoint

**Check**:
```bash
# Verify checkpoint file exists
cat ./state/checkpoint.json

# Verify checkpoint matches config
# network, era_id, and version_label must match
```

**Common causes**:
- Checkpoint directory different from config
- Config changed (network/era/version) since last run
- Checkpoint file corrupted or deleted

### Issue: PAS events not emitting

**Symptoms**: No files in `pas-backup/` directory

**Check**:
```bash
# Verify PAS is enabled
grep -i pas config.yaml

# Check logs for PAS errors
grep "pas" /var/log/bronze-copier.log
```

**Fix**:
- Ensure `pas.enabled: true`
- Ensure `pas.backup_dir` exists and is writable
- If using HTTP endpoint, verify network connectivity

### Issue: Partition already exists

**Symptoms**: Error "partition already exists: ledgers_lcm_raw range=X-Y"

**This is a safety feature** - Bronze Copier won't overwrite existing data.

**To re-process**:
```yaml
era:
  allow_overwrite: true  # Use with caution!
```

Or manually delete the partition:
```bash
rm -rf /data/bronze/pubnet/pre_p23/v1/ledgers_lcm_raw/range=X-Y/
```

## Health Checks

### Basic Health Check

```bash
# Check process is running
pgrep -f bronze-copier

# Check recent activity (within last hour)
find /data/state -name "checkpoint.json" -mmin -60
```

### Full Health Check Script

```bash
#!/bin/bash
# health-check.sh

CHECKPOINT_FILE="${1:-./state/checkpoint.json}"
MAX_AGE_MINUTES=60

if [ ! -f "$CHECKPOINT_FILE" ]; then
    echo "CRITICAL: Checkpoint file not found"
    exit 2
fi

# Check checkpoint age
AGE_MINUTES=$(( ($(date +%s) - $(stat -c %Y "$CHECKPOINT_FILE")) / 60 ))
if [ $AGE_MINUTES -gt $MAX_AGE_MINUTES ]; then
    echo "WARNING: Checkpoint not updated in $AGE_MINUTES minutes"
    exit 1
fi

# Get last committed ledger
LAST_LEDGER=$(jq -r '.last_committed_ledger' "$CHECKPOINT_FILE")
echo "OK: Last committed ledger $LAST_LEDGER ($AGE_MINUTES min ago)"
exit 0
```

## Maintenance

### Verifying Data Integrity

Check PAS chain continuity:
```bash
# Verify hash chain
prev_hash=""
for f in pas-backup/*.json; do
    curr_prev=$(jq -r '.chain.prev_event_hash' "$f")
    if [ -n "$prev_hash" ] && [ "$curr_prev" != "$prev_hash" ]; then
        echo "CHAIN BREAK at $f"
    fi
    prev_hash=$(jq -r '.chain.event_hash' "$f")
done
```

Verify parquet checksums:
```bash
# Compare checksum from manifest to computed
for manifest in /data/bronze/*/*/*/*/_manifest.json; do
    dir=$(dirname "$manifest")
    expected=$(jq -r '.tables.ledgers_lcm_raw.checksum' "$manifest")
    parquet=$(find "$dir" -name "*.parquet")
    actual="sha256:$(sha256sum "$parquet" | cut -d' ' -f1)"
    if [ "$expected" != "$actual" ]; then
        echo "CHECKSUM MISMATCH: $parquet"
    fi
done
```

### Cleanup

Remove old PAS backups (keep last 7 days):
```bash
find /data/pas-backup -name "*.json" -mtime +7 -delete
```

## Emergency Procedures

### Full Reset

To start completely fresh:
```bash
# 1. Stop the copier
pkill -f bronze-copier

# 2. Clear state (DESTRUCTIVE!)
rm -rf /data/state/*
rm -rf /data/pas-backup/*

# 3. Optionally clear output
rm -rf /data/bronze/*

# 4. Restart
./bronze-copier -config config.yaml
```

### Force Restart from Specific Ledger

```bash
# 1. Stop the copier
pkill -f bronze-copier

# 2. Edit checkpoint
cat > /data/state/checkpoint.json << 'EOF'
{
  "copier_id": "bronze-copier-001",
  "network": "pubnet",
  "era_id": "pre_p23",
  "version_label": "v1",
  "last_committed_ledger": 50000000,
  "updated_at": "2025-01-15T10:00:00Z"
}
EOF

# 3. Restart
./bronze-copier -config config.yaml
```

## Metrics to Monitor

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| Ledgers/sec | >30 | <10 |
| Checkpoint age | <5 min | >30 min |
| PAS emit failures | 0 | >10/hour |
| Partition errors | 0 | >0 |
| Disk space | >20% free | <10% free |

## Contact

For issues not covered here, check:
- GitHub Issues: https://github.com/withObsrvr/obsrvr-bronze-copier/issues
- Shaping docs: `docs/shaping/`
