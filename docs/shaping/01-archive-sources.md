# Pitch 01: Archive Sources

**Appetite:** 1 week
**Risk:** Low
**Dependencies:** None

---

## Problem

The Bronze Copier skeleton has a `noopSource` that returns empty channels. Without real archive sources, the copier cannot read any ledger data.

Obsrvr needs to read historical ledger data from multiple archive locations:
- **Galexie landing buckets** on GCS (current production archive)
- **S3-compatible storage** (Backblaze B2, AWS S3, MinIO for self-hosted)
- **Local filesystem** (development, testing, air-gapped deployments)

All archives store ledgers as **zstd-compressed XDR files** in the Galexie naming format:
```
{hash_prefix}--{ledger_seq}.xdr.zst
```

Grouped into range directories:
```
landing/ledgers/pubnet/{range_hash}--{start}-{end}/
```

---

## Appetite

**1 week** - This is foundational infrastructure with clear boundaries.

We will NOT:
- Implement live gRPC streaming (separate pitch)
- Handle network-specific XDR variations (Bronze stores raw bytes)
- Build retry/backoff sophistication (simple retry is enough for v1)

---

## Solution

### Fat-marker sketch

```
┌─────────────────────────────────────────────────────────────┐
│                     LedgerSource Interface                   │
│  Stream(ctx, start, end) → (chan LedgerCloseMeta, chan err) │
└─────────────────────────────────────────────────────────────┘
                              │
           ┌──────────────────┼──────────────────┐
           ▼                  ▼                  ▼
    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
    │ LocalSource │    │  GCSSource  │    │  S3Source   │
    └─────────────┘    └─────────────┘    └─────────────┘
           │                  │                  │
           └──────────────────┼──────────────────┘
                              ▼
                    ┌─────────────────┐
                    │   XDR Decoder   │
                    │ (zstd + unmarshal)│
                    └─────────────────┘
```

### Core components

1. **Archive Index Builder**
   - List objects in archive path
   - Parse filename to extract ledger sequence
   - Sort by sequence for ordered streaming
   - Support range filtering (start..end)

2. **File Fetcher**
   - Download/read single XDR file
   - Decompress zstd
   - Return raw bytes

3. **XDR Decoder**
   - Unmarshal `LedgerCloseMeta` from XDR bytes
   - Extract minimal header (seq, hash, timestamp)
   - Pass through raw bytes for Bronze storage

4. **Streaming Coordinator**
   - Iterate index in order
   - Fetch → Decode → Emit to channel
   - Handle gaps (error or skip based on config)
   - Respect context cancellation

---

## Scope Line

```
MUST HAVE ══════════════════════════════════════════════════════════
- LocalSource: Read from local filesystem path
- GCSSource: Read from gs:// URLs using Google Cloud client
- S3Source: Read from s3:// URLs using AWS SDK (covers B2, MinIO, R2)
- Zstd decompression
- XDR unmarshaling to LedgerCloseMeta
- Ordered streaming by ledger sequence
- Bounded range support (start..end)
- Unbounded tailing support (end=0 means follow head)

NICE TO HAVE ───────────────────────────────────────────────────────
- Parallel prefetching (download next N while processing current)
- Progress callback for monitoring
- Checksum verification of downloaded files

COULD HAVE (Cut first) ─────────────────────────────────────────────
- Automatic archive discovery (find latest range directory)
- Multi-archive fallback (try GCS, fall back to S3)
- Compression format auto-detection
```

---

## Rabbit Holes

1. **Don't build a generic blob abstraction** - Use `gocloud.dev/blob` which already exists
2. **Don't handle partial/corrupt files** - Fail the ledger, let retry handle it
3. **Don't implement caching** - Bronze Copier is write-through, not a cache
4. **Don't parse XDR deeply** - Extract header only, preserve raw bytes
5. **Don't handle live streaming here** - That's a separate source type

---

## No-Gos

- No HTTP/REST archive sources (not in current infrastructure)
- No Horizon API as source (too slow, not raw XDR)
- No captive-core integration (that's live source territory)
- No archive format conversion (we read Galexie format only)

---

## Technical Approach

### Dependencies to add

```go
// go.mod additions
require (
    github.com/stellar/go v0.0.0-latest    // XDR types
    github.com/klauspost/compress v1.17.0  // zstd
    gocloud.dev v0.37.0                    // unified blob access
    cloud.google.com/go/storage v1.40.0    // GCS (via gocloud)
    github.com/aws/aws-sdk-go-v2 v1.26.0   // S3 (via gocloud)
)
```

### Interface refinement

```go
// internal/source/source.go

type SourceConfig struct {
    Mode     string // "local" | "gcs" | "s3"

    // Local
    LocalPath string // /path/to/archive/

    // GCS
    GCSBucket string // bucket name
    GCSPrefix string // landing/ledgers/pubnet/

    // S3
    S3Bucket   string
    S3Prefix   string
    S3Endpoint string // For B2/MinIO/R2
    S3Region   string

    // Common
    StartLedger uint32
    EndLedger   uint32 // 0 = unbounded
}

type LedgerCloseMeta struct {
    LedgerSeq uint32
    Hash      string // ledger hash for verification
    CloseTime uint64 // unix timestamp
    RawXDR    []byte // preserved exactly as read
}
```

### File structure

```
internal/source/
├── source.go          # Interface + factory
├── local.go           # LocalSource implementation
├── gcs.go             # GCSSource implementation
├── s3.go              # S3Source implementation
├── decoder.go         # XDR decoding utilities
├── index.go           # Archive index building
└── source_test.go     # Integration tests
```

---

## Done Criteria

**Concrete example of "done":**

```bash
# Set up test archive with 100 ledgers
$ ls ./testdata/archive/
FC6D0000--1-100/
  FC6D0001--1.xdr.zst
  FC6D0002--2.xdr.zst
  ...
  FC6D0064--100.xdr.zst

# Run copier with local source
$ SOURCE_MODE=local \
  LOCAL_PATH=./testdata/archive \
  LEDGER_START=1 \
  LEDGER_END=100 \
  go run ./cmd/bronze-copier

[source] indexed 100 ledgers in ./testdata/archive
[source] streaming ledgers 1-100
[copier] received ledger 1 (hash=abc123, 1847 bytes)
[copier] received ledger 2 (hash=def456, 1923 bytes)
...
[copier] received ledger 100 (hash=xyz789, 2104 bytes)
[source] stream complete
```

**Unit test requirements:**

1. `TestLocalSource_StreamBounded` - Reads exact range from local files
2. `TestLocalSource_StreamUnbounded` - Tails directory for new files
3. `TestGCSSource_StreamBounded` - Reads from GCS bucket (integration)
4. `TestS3Source_StreamBounded` - Reads from S3/B2 (integration)
5. `TestDecoder_ValidXDR` - Correctly parses LedgerCloseMeta
6. `TestDecoder_InvalidXDR` - Returns typed error for corrupt data
7. `TestIndex_SortsCorrectly` - Ledgers emitted in sequence order
8. `TestIndex_DetectsGaps` - Errors on missing ledger in range

---

## Hill Progress Tracking

| Stage | Status | Notes |
|-------|--------|-------|
| **Left side (figuring out)** | | |
| Understand Galexie file format | ⬜ | Review actual archive structure |
| Design unified interface | ⬜ | Finalize SourceConfig |
| Spike gocloud.dev blob usage | ⬜ | Verify GCS+S3 work |
| **Right side (making it happen)** | | |
| Implement LocalSource | ⬜ | |
| Implement GCSSource | ⬜ | |
| Implement S3Source | ⬜ | |
| Implement decoder | ⬜ | |
| Write tests | ⬜ | |
| Integration test with real archive | ⬜ | |

---

## Questions for Betting

1. Should we support reading from multiple archives simultaneously (e.g., primary GCS + fallback S3)?
   - **Recommendation:** No for v1, nice-to-have for v1.1

2. What's the expected latency tolerance for archive reads?
   - **Assumption:** Minutes are fine, we're not real-time

3. Do we need to verify archive file integrity (checksums)?
   - **Recommendation:** Nice-to-have, trust the source for v1
