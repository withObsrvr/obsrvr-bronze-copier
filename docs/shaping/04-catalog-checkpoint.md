# Pitch 04: Catalog & Checkpointing

**Appetite:** 1 week
**Risk:** Medium (database schema design, recovery logic)
**Dependencies:** Pitch 03 (Storage Backends)

---

## Problem

The Bronze Copier has two critical gaps:

1. **No metadata tracking** - The `noopWriter` discards all lineage records. There's no way to know what partitions exist, when they were created, or their checksums.

2. **No checkpointing** - If the copier crashes, it restarts from the beginning. For a full mainnet backfill (~60M ledgers), this is unacceptable.

Without these, Bronze Copier is:
- Not queryable (what data do we have?)
- Not resumable (crash = start over)
- Not auditable (no lineage trail)

---

## Appetite

**1 week** - This combines two related features into one cohesive system.

We will NOT:
- Build a full data catalog UI
- Implement complex query patterns
- Handle multi-writer coordination (single writer assumed)
- Build automatic data quality scoring

---

## Solution

### Fat-marker sketch

```
┌─────────────────────────────────────────────────────────────┐
│                     Bronze Copier                            │
│                          │                                   │
│    ┌─────────────────────┼─────────────────────┐            │
│    ▼                     ▼                     ▼            │
│ ┌──────────┐      ┌──────────────┐      ┌──────────────┐   │
│ │ Storage  │      │   Catalog    │      │ Checkpoint   │   │
│ │ (parquet)│      │ (PostgreSQL) │      │   (file)     │   │
│ └──────────┘      └──────────────┘      └──────────────┘   │
│                          │                     │            │
│                   _meta_* tables        state.json         │
└─────────────────────────────────────────────────────────────┘
```

### Core components

**1. Catalog Writer (PostgreSQL)**
- Records partition metadata in DuckLake `_meta_*` tables
- Tracks lineage, quality metrics, and dataset registry
- Enables downstream queries about data coverage

**2. Checkpoint Manager (Local file or object)**
- Persists last successfully committed ledger
- Enables resume from crash
- Simple JSON file in state directory

---

## Catalog Schema

Building on DuckLake metadata tables:

### `_meta_datasets`

Registry of all Bronze datasets (tables × versions):

```sql
CREATE TABLE _meta_datasets (
    id              SERIAL PRIMARY KEY,
    domain          VARCHAR(64) NOT NULL,     -- "bronze"
    dataset         VARCHAR(128) NOT NULL,    -- "ledgers_lcm_raw"
    version         VARCHAR(32) NOT NULL,     -- "v1"
    era_id          VARCHAR(64) NOT NULL,     -- "pre_p23"
    network         VARCHAR(32) NOT NULL,     -- "pubnet"
    schema_hash     VARCHAR(64),              -- hash of parquet schema
    description     TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),

    UNIQUE(domain, dataset, version, era_id, network)
);
```

### `_meta_lineage`

Record of every partition written:

```sql
CREATE TABLE _meta_lineage (
    id                  SERIAL PRIMARY KEY,
    dataset_id          INTEGER REFERENCES _meta_datasets(id),
    ledger_start        BIGINT NOT NULL,
    ledger_end          BIGINT NOT NULL,
    row_count           BIGINT NOT NULL,
    byte_size           BIGINT NOT NULL,
    checksum            VARCHAR(128) NOT NULL,   -- sha256 of parquet
    storage_path        TEXT NOT NULL,
    producer_version    VARCHAR(64) NOT NULL,    -- "bronze-copier@v0.1.0"
    producer_git_sha    VARCHAR(64),
    source_type         VARCHAR(32),             -- "archive" | "live"
    source_location     TEXT,                    -- gs://bucket/path
    created_at          TIMESTAMP DEFAULT NOW(),

    UNIQUE(dataset_id, ledger_start, ledger_end)
);

CREATE INDEX idx_lineage_range ON _meta_lineage(dataset_id, ledger_start, ledger_end);
```

### `_meta_quality`

Quality metrics per partition (for future use):

```sql
CREATE TABLE _meta_quality (
    id              SERIAL PRIMARY KEY,
    lineage_id      INTEGER REFERENCES _meta_lineage(id),
    metric_name     VARCHAR(64) NOT NULL,
    metric_value    DOUBLE PRECISION,
    created_at      TIMESTAMP DEFAULT NOW()
);
```

### `_meta_changes`

Schema evolution tracking:

```sql
CREATE TABLE _meta_changes (
    id              SERIAL PRIMARY KEY,
    dataset_id      INTEGER REFERENCES _meta_datasets(id),
    change_type     VARCHAR(32) NOT NULL,    -- "schema_change" | "correction" | "backfill"
    description     TEXT,
    old_version     VARCHAR(32),
    new_version     VARCHAR(32),
    created_at      TIMESTAMP DEFAULT NOW()
);
```

---

## Checkpoint Format

Simple JSON file stored locally or in object storage:

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

### Checkpoint location options

1. **Local file** (default): `./state/checkpoint.json`
2. **Object storage**: `gs://bucket/bronze/state/checkpoint.json`
3. **Catalog query** (fallback): `SELECT MAX(ledger_end) FROM _meta_lineage WHERE ...`

---

## Scope Line

```
MUST HAVE ══════════════════════════════════════════════════════════
- Catalog writer for _meta_lineage table
- Checkpoint file (local) for resume
- Resume logic on startup (read checkpoint, continue from there)
- Basic _meta_datasets registration

NICE TO HAVE ───────────────────────────────────────────────────────
- Checkpoint to object storage
- _meta_quality metrics
- _meta_changes tracking
- Coverage gap detection queries
- Catalog query fallback for checkpoint

COULD HAVE (Cut first) ─────────────────────────────────────────────
- Multi-writer coordination (locking)
- Automatic backfill gap detection
- Data retention policies
- Catalog replication
```

---

## Rabbit Holes

1. **Don't build a full ORM** - Raw SQL with minimal abstraction
2. **Don't implement distributed checkpointing** - Single writer assumption
3. **Don't build catalog query API** - Direct SQL access is fine
4. **Don't handle schema migrations automatically** - Manual for v1
5. **Don't implement soft deletes** - Catalog is append-only

---

## No-Gos

- No catalog UI (use existing tools like pgAdmin, DBeaver)
- No real-time sync between checkpoint and catalog
- No automatic retry of failed catalog writes (fail the partition)
- No cross-network coordination

---

## Technical Approach

### Dependencies

```go
// go.mod additions
require (
    github.com/jackc/pgx/v5 v5.5.0  // PostgreSQL driver
)
```

### Interface

```go
// internal/metadata/writer.go

type CatalogConfig struct {
    PostgresDSN string   // postgres://user:pass@host:5432/db
    Namespace   string   // "pubnet"
}

type Writer interface {
    // EnsureDataset registers or retrieves a dataset entry
    EnsureDataset(ctx context.Context, ds DatasetInfo) (int64, error)

    // RecordPartition writes a lineage record
    RecordPartition(ctx context.Context, rec PartitionRecord) error

    // Close releases database connections
    Close() error
}

type DatasetInfo struct {
    Domain      string // "bronze"
    Dataset     string // "ledgers_lcm_raw"
    Version     string // "v1"
    EraID       string // "pre_p23"
    Network     string // "pubnet"
    SchemaHash  string
    Description string
}

type PartitionRecord struct {
    DatasetID       int64
    LedgerStart     uint32
    LedgerEnd       uint32
    RowCount        int64
    ByteSize        int64
    Checksum        string
    StoragePath     string
    ProducerVersion string
    ProducerGitSHA  string
    SourceType      string
    SourceLocation  string
}
```

```go
// internal/checkpoint/checkpoint.go

type CheckpointConfig struct {
    Mode     string // "local" | "gcs" | "s3"
    LocalDir string
    Bucket   string
    Prefix   string
}

type Manager interface {
    // Load reads the current checkpoint
    Load(ctx context.Context) (*Checkpoint, error)

    // Save persists the checkpoint
    Save(ctx context.Context, cp *Checkpoint) error
}

type Checkpoint struct {
    CopierID             string    `json:"copier_id"`
    Network              string    `json:"network"`
    EraID                string    `json:"era_id"`
    VersionLabel         string    `json:"version_label"`
    LastCommittedLedger  uint32    `json:"last_committed_ledger"`
    LastPartition        *PartitionInfo `json:"last_committed_partition,omitempty"`
    UpdatedAt            time.Time `json:"updated_at"`
}
```

### File structure

```
internal/
├── metadata/
│   ├── writer.go        # Writer interface + factory
│   ├── postgres.go      # PostgreSQL implementation
│   ├── schema.sql       # DDL for _meta_* tables
│   └── writer_test.go
├── checkpoint/
│   ├── checkpoint.go    # Manager interface + types
│   ├── local.go         # Local file implementation
│   ├── object.go        # Object storage implementation
│   └── checkpoint_test.go
```

### Resume flow

```go
func (c *Copier) Run(ctx context.Context) error {
    // 1. Load checkpoint
    cp, err := c.checkpoint.Load(ctx)
    if err != nil && !errors.Is(err, ErrNoCheckpoint) {
        return fmt.Errorf("load checkpoint: %w", err)
    }

    // 2. Determine start ledger
    startLedger := c.cfg.Era.LedgerStart
    if cp != nil && cp.LastCommittedLedger > 0 {
        startLedger = cp.LastCommittedLedger + 1
        log.Printf("[resume] continuing from ledger %d", startLedger)
    }

    // 3. Stream from start
    ledgersCh, errCh := c.src.Stream(ctx, startLedger, c.cfg.Era.LedgerEnd)

    // ... rest of processing loop ...
}

func (c *Copier) commitPartition(ctx context.Context, part Partition) error {
    // ... write parquet, catalog ...

    // Update checkpoint AFTER successful commit
    if err := c.checkpoint.Save(ctx, &Checkpoint{
        CopierID:            c.cfg.CopierID,
        Network:             c.cfg.Era.Network,
        EraID:               c.cfg.Era.EraID,
        VersionLabel:        c.cfg.Era.VersionLabel,
        LastCommittedLedger: part.End,
        UpdatedAt:           time.Now().UTC(),
    }); err != nil {
        log.Printf("[warn] checkpoint save failed: %v", err)
        // Don't fail the partition, but log loudly
    }

    return nil
}
```

---

## Done Criteria

**Concrete example of "done":**

```bash
# First run - process ledgers 1-30000
$ LEDGER_START=1 LEDGER_END=30000 ./bronze-copier
[copier] starting from ledger 1
[partition] committed range 1-10000
[checkpoint] saved: last_committed=10000
[partition] committed range 10001-20000
[checkpoint] saved: last_committed=20000
[partition] committed range 20001-30000
[checkpoint] saved: last_committed=30000
[copier] complete

# Simulate crash at ledger 45000
$ LEDGER_START=1 LEDGER_END=50000 ./bronze-copier
[copier] starting from ledger 1
[partition] committed range 1-10000
...
[partition] committed range 40001-45000
^C  # Ctrl+C or crash

# Resume - should continue from 45001
$ LEDGER_START=1 LEDGER_END=50000 ./bronze-copier
[resume] checkpoint found: last_committed=45000
[copier] continuing from ledger 45001
[partition] committed range 45001-50000
[copier] complete

# Query catalog for coverage
$ psql -c "SELECT ledger_start, ledger_end, checksum
           FROM _meta_lineage l
           JOIN _meta_datasets d ON l.dataset_id = d.id
           WHERE d.dataset = 'ledgers_lcm_raw'
           ORDER BY ledger_start"
 ledger_start | ledger_end |    checksum
--------------+------------+-----------------
            1 |      10000 | sha256:abc123...
        10001 |      20000 | sha256:def456...
        20001 |      30000 | sha256:ghi789...
        ...
```

**Unit test requirements:**

1. `TestCatalogWriter_EnsureDataset` - Creates dataset on first call, returns ID on second
2. `TestCatalogWriter_RecordPartition` - Inserts lineage record
3. `TestCatalogWriter_DuplicatePartition` - Returns error for duplicate range
4. `TestCheckpoint_SaveLoad` - Round-trip checkpoint
5. `TestCheckpoint_Resume` - Copier resumes from checkpoint
6. `TestCheckpoint_NoCheckpoint` - Copier starts from config start

---

## Hill Progress Tracking

| Stage | Status | Notes |
|-------|--------|-------|
| **Left side (figuring out)** | | |
| Design _meta_* schema | ⬜ | Align with existing DuckLake tables |
| Design checkpoint format | ⬜ | |
| Decide checkpoint storage location | ⬜ | Local vs object |
| **Right side (making it happen)** | | |
| Implement catalog writer | ⬜ | |
| Implement checkpoint manager | ⬜ | |
| Integrate resume logic | ⬜ | |
| Create schema migration | ⬜ | |
| Write tests | ⬜ | |
| Integration test with PostgreSQL | ⬜ | |

---

## Questions for Betting

1. Should checkpoint be stored in catalog (PostgreSQL) instead of file?
   - **Recommendation:** File is simpler, catalog query as fallback

2. Do we need locking for multi-writer scenarios?
   - **Recommendation:** No for v1, assume single writer per era/version

3. Should we detect and alert on coverage gaps?
   - **Recommendation:** Nice-to-have query, not automatic detection

4. How long to retain lineage records?
   - **Assumption:** Forever - they're small and valuable for audit
