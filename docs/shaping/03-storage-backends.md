# Pitch 03: Storage Backends

**Appetite:** 3 days
**Risk:** Low (well-understood problem, libraries exist)
**Dependencies:** None (can be built in parallel with Pitches 01-02)

---

## Problem

The Bronze Copier has a `noopStore` that discards all parquet output. Without real storage backends, the copier produces no persistent output.

Bronze needs to write parquet files to durable storage that:
1. **Supports object storage** - GCS, S3, B2, R2 (bucket as system of record)
2. **Supports local filesystem** - Development, testing, air-gapped deployments
3. **Uses versioned paths** - Era + version in path structure
4. **Writes atomically** - No partial files visible to readers
5. **Checks for duplicates** - Prevent accidental overwrites

---

## Appetite

**3 days** - This is plumbing with well-defined interfaces.

We will NOT:
- Build a caching layer
- Implement replication between backends
- Handle lifecycle management (retention, archival)
- Build a query layer over storage

---

## Solution

### Fat-marker sketch

```
┌─────────────────────────────────────────────────────────────┐
│                    BronzeStore Interface                     │
│  WriteParquet(ctx, ref, bytes) error                        │
│  Exists(ctx, ref) (bool, error)                             │
│  WriteManifest(ctx, ref, manifest) error                    │
└─────────────────────────────────────────────────────────────┘
                              │
           ┌──────────────────┼──────────────────┐
           ▼                  ▼                  ▼
    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
    │ LocalStore  │    │ GCSStore    │    │  S3Store    │
    │ (filesystem)│    │ (gocloud)   │    │ (gocloud)   │
    └─────────────┘    └─────────────┘    └─────────────┘
```

### Path layout

The storage path encodes era, version, table, and range:

```
{prefix}/
  {network}/
    {era_id}/
      {version_label}/
        {table}/
          range={start}-{end}/
            part-{start}-{end}.parquet
            _manifest.json
```

**Example:**
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

### Manifest file

Each partition directory includes a manifest for easy verification:

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
    "version": "bronze-copier@v0.1.0",
    "git_sha": "abc123"
  },
  "created_at": "2025-01-15T10:30:00Z"
}
```

---

## Scope Line

```
MUST HAVE ══════════════════════════════════════════════════════════
- LocalStore: Write to local filesystem
- GCSStore: Write to Google Cloud Storage bucket
- S3Store: Write to S3-compatible storage (AWS, B2, R2, MinIO)
- Atomic writes (temp file → rename pattern)
- Duplicate detection (Exists check)
- Manifest file generation
- Versioned path structure

NICE TO HAVE ───────────────────────────────────────────────────────
- Object lock / WORM support for immutability
- Cross-region replication configuration
- Bandwidth limiting
- Progress callbacks for large writes

COULD HAVE (Cut first) ─────────────────────────────────────────────
- Multi-backend mirroring (write to GCS + B2 simultaneously)
- Compression at storage layer
- Encryption at rest configuration
```

---

## Rabbit Holes

1. **Don't build abstraction over gocloud.dev** - Use it directly
2. **Don't implement retry logic** - Let caller retry whole partition
3. **Don't handle partial uploads** - Atomic or fail
4. **Don't build list/scan operations** - That's catalog's job
5. **Don't implement delete** - Bronze is append-only

---

## No-Gos

- No in-place updates (Bronze is immutable)
- No directory creation on object storage (objects are flat)
- No custom authentication handling (use environment/IAM)
- No streaming writes (buffer in memory, single PUT)

---

## Technical Approach

### Dependencies

```go
// go.mod additions
require (
    gocloud.dev v0.37.0                    // Unified blob interface
    cloud.google.com/go/storage v1.40.0    // GCS driver
    github.com/aws/aws-sdk-go-v2 v1.26.0   // S3 driver
)
```

### Interface refinement

```go
// internal/storage/store.go

type PartitionRef struct {
    Network      string // "pubnet" | "testnet"
    EraID        string // "pre_p23" | "p23_plus"
    VersionLabel string // "v1" | "v2"
    Table        string // "ledgers_lcm_raw"
    LedgerStart  uint32
    LedgerEnd    uint32
}

// Path returns the storage path for this partition
func (r PartitionRef) Path(prefix string) string {
    return fmt.Sprintf("%s/%s/%s/%s/%s/range=%d-%d/part-%d-%d.parquet",
        prefix, r.Network, r.EraID, r.VersionLabel, r.Table,
        r.LedgerStart, r.LedgerEnd, r.LedgerStart, r.LedgerEnd)
}

type Manifest struct {
    Partition PartitionInfo          `json:"partition"`
    Tables    map[string]TableInfo   `json:"tables"`
    Producer  ProducerInfo           `json:"producer"`
    CreatedAt time.Time              `json:"created_at"`
}

type BronzeStore interface {
    WriteParquet(ctx context.Context, ref PartitionRef, parquetBytes []byte) error
    WriteManifest(ctx context.Context, ref PartitionRef, manifest Manifest) error
    Exists(ctx context.Context, ref PartitionRef) (bool, error)
}

type StorageConfig struct {
    Backend    string // "local" | "gcs" | "s3"

    // Local
    LocalDir   string // /path/to/bronze/

    // GCS
    GCSBucket  string

    // S3
    S3Bucket   string
    S3Endpoint string // For B2/MinIO/R2
    S3Region   string

    // Common
    Prefix     string // "bronze/" (within bucket or local dir)
}
```

### File structure

```
internal/storage/
├── store.go       # Interface + factory + PartitionRef
├── local.go       # LocalStore implementation
├── gcs.go         # GCSStore implementation (via gocloud)
├── s3.go          # S3Store implementation (via gocloud)
├── manifest.go    # Manifest types and serialization
└── store_test.go  # Integration tests
```

### Atomic write pattern

```go
// For local filesystem
func (s *LocalStore) WriteParquet(ctx context.Context, ref PartitionRef, data []byte) error {
    finalPath := ref.Path(s.prefix)
    tempPath := finalPath + ".tmp"

    // Write to temp file
    if err := os.WriteFile(tempPath, data, 0644); err != nil {
        return err
    }

    // Atomic rename
    return os.Rename(tempPath, finalPath)
}

// For object storage (GCS/S3)
// Object storage writes are inherently atomic - object appears only after upload completes
func (s *GCSStore) WriteParquet(ctx context.Context, ref PartitionRef, data []byte) error {
    path := ref.Path(s.prefix)
    w, err := s.bucket.NewWriter(ctx, path, nil)
    if err != nil {
        return err
    }
    if _, err := w.Write(data); err != nil {
        w.Close()
        return err
    }
    return w.Close()
}
```

---

## Done Criteria

**Concrete example of "done":**

```bash
# Test local storage
$ STORAGE_BACKEND=local \
  LOCAL_DIR=./output \
  go run ./cmd/bronze-copier --dry-run

[storage] writing to ./output/pubnet/pre_p23/v1/ledgers_lcm_raw/range=0-9999/
[storage] wrote part-0-9999.parquet (524MB, sha256=abc123...)
[storage] wrote _manifest.json

$ ls -la ./output/pubnet/pre_p23/v1/ledgers_lcm_raw/range=0-9999/
part-0-9999.parquet
_manifest.json

$ cat ./output/pubnet/pre_p23/v1/ledgers_lcm_raw/range=0-9999/_manifest.json
{
  "partition": {"start": 0, "end": 9999, ...},
  "tables": {"ledgers_lcm_raw": {"checksum": "sha256:abc123...", ...}},
  ...
}

# Test GCS storage
$ STORAGE_BACKEND=gcs \
  GCS_BUCKET=obsrvr-bronze-test \
  go run ./cmd/bronze-copier --dry-run

[storage] writing to gs://obsrvr-bronze-test/bronze/pubnet/pre_p23/v1/...
[storage] wrote part-0-9999.parquet
[storage] wrote _manifest.json
```

**Unit test requirements:**

1. `TestLocalStore_WriteParquet` - Writes file to correct path
2. `TestLocalStore_AtomicWrite` - Temp file cleaned up on failure
3. `TestLocalStore_Exists` - Correctly detects existing partitions
4. `TestGCSStore_WriteParquet` - Writes to GCS bucket (integration)
5. `TestS3Store_WriteParquet` - Writes to S3/B2 (integration)
6. `TestPartitionRef_Path` - Generates correct path structure
7. `TestManifest_JSON` - Serializes manifest correctly

---

## Hill Progress Tracking

| Stage | Status | Notes |
|-------|--------|-------|
| **Left side (figuring out)** | | |
| Confirm path layout with team | ⬜ | |
| Spike gocloud.dev for GCS+S3 | ⬜ | Verify auth patterns work |
| Design manifest schema | ⬜ | |
| **Right side (making it happen)** | | |
| Implement LocalStore | ⬜ | |
| Implement GCSStore | ⬜ | |
| Implement S3Store | ⬜ | |
| Implement manifest generation | ⬜ | |
| Write tests | ⬜ | |
| Integration test with real buckets | ⬜ | |

---

## Questions for Betting

1. Should manifest include PAS event hash reference?
   - **Recommendation:** Yes, add `pas_event_hash` field for linking

2. Do we need to support custom path templates?
   - **Recommendation:** No, fixed structure is simpler and safer

3. Should we support object versioning on GCS/S3?
   - **Recommendation:** Nice-to-have, rely on AllowOverwrite flag for now

4. What's the maximum file size we should support?
   - **Assumption:** 2GB (S3 single PUT limit), partitions should be smaller
