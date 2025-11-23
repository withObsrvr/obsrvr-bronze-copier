# Pitch 05: PAS Emitter

**Appetite:** 3 days
**Risk:** Low (well-defined protocol, straightforward implementation)
**Dependencies:** Pitch 04 (Catalog & Checkpointing)

---

## Problem

The Bronze Copier has a `noopEmitter` that discards all PAS events. Without PAS (Public Audit Stream) emission, there's no public proof that Bronze data was produced correctly.

PAS is Obsrvr's mechanism for:
1. **Public verifiability** - Anyone can verify Bronze integrity
2. **Hash chaining** - Each event links to previous, forming tamper-evident log
3. **Audit trail** - Cryptographic proof of what was produced and when

Without PAS, Bronze is "trust us" instead of "verify independently."

---

## Appetite

**3 days** - PAS v1.1 is a simple append-only log with hash chaining.

We will NOT:
- Build the PAS verification infrastructure (separate project)
- Implement IPFS pinning (v1.2 scope)
- Handle PAS service unavailability gracefully (fail partition, retry)
- Build PAS query/read APIs

---

## Solution

### Fat-marker sketch

```
┌─────────────────────────────────────────────────────────────┐
│                     Bronze Copier                            │
│                          │                                   │
│              After storage + catalog commit                  │
│                          │                                   │
│                          ▼                                   │
│                  ┌──────────────┐                           │
│                  │ PAS Emitter  │                           │
│                  └──────────────┘                           │
│                          │                                   │
│           ┌──────────────┼──────────────┐                   │
│           ▼              ▼              ▼                   │
│    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐        │
│    │  HTTP POST  │ │  Local File │ │   stdout    │        │
│    │ (PAS server)│ │  (backup)   │ │  (dev mode) │        │
│    └─────────────┘ └─────────────┘ └─────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

### PAS Event Structure (v1.1)

Each partition produces one PAS event:

```json
{
  "version": "1.1",
  "event_type": "bronze_partition",
  "event_id": "pas_evt_abc123...",
  "timestamp": "2025-01-15T10:30:00Z",

  "partition": {
    "network": "pubnet",
    "era_id": "pre_p23",
    "version_label": "v1",
    "ledger_start": 0,
    "ledger_end": 9999
  },

  "tables": {
    "ledgers_lcm_raw": {
      "checksum": "sha256:abc123...",
      "row_count": 10000,
      "storage_path": "bronze/pubnet/pre_p23/v1/ledgers_lcm_raw/range=0-9999/"
    }
  },

  "producer": {
    "name": "bronze-copier",
    "version": "v0.1.0",
    "git_sha": "abc123"
  },

  "chain": {
    "prev_event_hash": "sha256:prevhash...",
    "event_hash": "sha256:thishash..."
  }
}
```

### Hash Chaining

The `event_hash` is computed over a canonical JSON representation:

```
event_hash = sha256(canonical_json(event without chain.event_hash))
```

This creates a linked list of events where any tampering breaks the chain.

**Chain rules:**
- First event in a chain has `prev_event_hash: null`
- Each subsequent event references previous event's hash
- Separate chains per (network, era_id, version_label) tuple
- Chain breaks require explicit "chain_reset" event

---

## Scope Line

```
MUST HAVE ══════════════════════════════════════════════════════════
- PAS event generation with hash chaining
- HTTP POST to PAS endpoint (when enabled)
- Local file backup of emitted events
- Chain tracking per (network, era, version)
- Graceful disable (PAS_ENABLED=false)

NICE TO HAVE ───────────────────────────────────────────────────────
- Retry with exponential backoff
- Dead letter queue for failed emissions
- Batch emission (multiple events per request)
- Event signing (cryptographic signature)

COULD HAVE (Cut first) ─────────────────────────────────────────────
- IPFS pinning
- Multiple PAS endpoints
- Event compression
- Merkle tree batch proofs
```

---

## Rabbit Holes

1. **Don't build PAS server** - Just the client/emitter
2. **Don't implement complex retry logic** - Simple retry, then fail
3. **Don't handle chain recovery** - Manual intervention for broken chains
4. **Don't optimize for high throughput** - One event per partition is fine
5. **Don't implement event signing yet** - Hash chaining is sufficient for v1

---

## No-Gos

- No PAS read/query functionality (that's the verifier's job)
- No automatic chain repair
- No multi-signature schemes
- No consensus between emitters

---

## Technical Approach

### Interface

```go
// internal/pas/emitter.go

type PASConfig struct {
    Enabled      bool
    Endpoint     string   // "https://pas.obsrvr.io/v1/events"
    BackupDir    string   // "./pas-backup/"
    RetryCount   int      // 3
    RetryDelay   time.Duration // 1s
}

type Event struct {
    Version   string          `json:"version"`
    EventType string          `json:"event_type"`
    EventID   string          `json:"event_id"`
    Timestamp time.Time       `json:"timestamp"`

    Partition PartitionInfo   `json:"partition"`
    Tables    map[string]TableChecksum `json:"tables"`
    Producer  ProducerInfo    `json:"producer"`
    Chain     ChainInfo       `json:"chain"`
}

type PartitionInfo struct {
    Network      string `json:"network"`
    EraID        string `json:"era_id"`
    VersionLabel string `json:"version_label"`
    LedgerStart  uint32 `json:"ledger_start"`
    LedgerEnd    uint32 `json:"ledger_end"`
}

type TableChecksum struct {
    Checksum    string `json:"checksum"`
    RowCount    int64  `json:"row_count"`
    StoragePath string `json:"storage_path"`
}

type ChainInfo struct {
    PrevEventHash string `json:"prev_event_hash"`
    EventHash     string `json:"event_hash"`
}

type Emitter interface {
    // EmitPartition creates and publishes a PAS event
    EmitPartition(ctx context.Context, evt Event) error

    // GetLastEventHash returns the hash of the last emitted event for chaining
    GetLastEventHash(network, eraID, version string) (string, error)

    // Close flushes any pending events
    Close() error
}
```

### File structure

```
internal/pas/
├── emitter.go       # Emitter interface + factory
├── http.go          # HTTP emitter implementation
├── file.go          # File backup implementation
├── chain.go         # Hash chaining logic
├── event.go         # Event types and canonical JSON
└── emitter_test.go
```

### Hash computation

```go
// internal/pas/chain.go

func ComputeEventHash(evt *Event) string {
    // Create copy without event_hash
    copy := *evt
    copy.Chain.EventHash = ""

    // Canonical JSON (sorted keys, no whitespace)
    canonical, _ := json.Marshal(copy)

    hash := sha256.Sum256(canonical)
    return "sha256:" + hex.EncodeToString(hash[:])
}

func (e *httpEmitter) EmitPartition(ctx context.Context, evt Event) error {
    // 1. Get previous hash for chain
    prevHash, err := e.GetLastEventHash(evt.Partition.Network, evt.Partition.EraID, evt.Partition.VersionLabel)
    if err != nil && !errors.Is(err, ErrNoChainHead) {
        return err
    }

    // 2. Set chain info
    evt.Chain.PrevEventHash = prevHash
    evt.EventID = generateEventID()
    evt.Timestamp = time.Now().UTC()

    // 3. Compute event hash
    evt.Chain.EventHash = ComputeEventHash(&evt)

    // 4. Backup to local file (always, before HTTP)
    if err := e.backupEvent(&evt); err != nil {
        log.Printf("[pas] backup failed: %v", err)
    }

    // 5. POST to PAS endpoint
    if err := e.postEvent(ctx, &evt); err != nil {
        return fmt.Errorf("pas emit failed: %w", err)
    }

    // 6. Update chain head
    e.updateChainHead(evt.Partition.Network, evt.Partition.EraID, evt.Partition.VersionLabel, evt.Chain.EventHash)

    return nil
}
```

### Chain head tracking

Store last event hash per chain in local file:

```json
// ./state/pas-chain-heads.json
{
  "pubnet/pre_p23/v1": "sha256:abc123...",
  "pubnet/p23_plus/v2": "sha256:def456...",
  "testnet/pre_p23/v1": "sha256:ghi789..."
}
```

---

## Done Criteria

**Concrete example of "done":**

```bash
# Run copier with PAS enabled
$ PAS_ENABLED=true \
  PAS_ENDPOINT=https://pas.obsrvr.io/v1/events \
  ./bronze-copier

[copier] starting
[partition] committed range 0-9999
[pas] emitting event for pubnet/pre_p23/v1 range=0-9999
[pas] prev_hash=null (first in chain)
[pas] event_hash=sha256:abc123...
[pas] POST https://pas.obsrvr.io/v1/events -> 201 Created
[pas] backed up to ./pas-backup/pubnet_pre_p23_v1_0-9999.json

[partition] committed range 10000-19999
[pas] emitting event for pubnet/pre_p23/v1 range=10000-19999
[pas] prev_hash=sha256:abc123...
[pas] event_hash=sha256:def456...
[pas] POST https://pas.obsrvr.io/v1/events -> 201 Created

# Verify chain
$ cat ./pas-backup/pubnet_pre_p23_v1_10000-19999.json | jq '.chain'
{
  "prev_event_hash": "sha256:abc123...",
  "event_hash": "sha256:def456..."
}

# Disable PAS for dev
$ PAS_ENABLED=false ./bronze-copier
[copier] starting
[pas] disabled, skipping emission
[partition] committed range 0-9999
```

**Unit test requirements:**

1. `TestComputeEventHash_Deterministic` - Same event produces same hash
2. `TestComputeEventHash_Canonical` - JSON serialization is canonical
3. `TestChain_FirstEvent` - First event has null prev_hash
4. `TestChain_Linking` - Events correctly chain together
5. `TestEmitter_Disabled` - No-op when PAS_ENABLED=false
6. `TestEmitter_BackupAlways` - Local backup even if HTTP fails
7. `TestEmitter_HTTPRetry` - Retries on transient failure

---

## Hill Progress Tracking

| Stage | Status | Notes |
|-------|--------|-------|
| **Left side (figuring out)** | | |
| Finalize PAS v1.1 event schema | ⬜ | Align with PAS spec |
| Design canonical JSON format | ⬜ | Ensure deterministic hashing |
| Decide chain head storage | ⬜ | Local file vs catalog |
| **Right side (making it happen)** | | |
| Implement event types | ⬜ | |
| Implement hash computation | ⬜ | |
| Implement HTTP emitter | ⬜ | |
| Implement file backup | ⬜ | |
| Implement chain tracking | ⬜ | |
| Write tests | ⬜ | |

---

## Questions for Betting

1. Should PAS failure block partition commit?
   - **Recommendation:** Yes for v1 (strict mode), configurable later

2. How to handle chain breaks (missed events)?
   - **Recommendation:** Emit "chain_reset" event, manual intervention

3. Should we sign events cryptographically?
   - **Recommendation:** Nice-to-have for v1.1, hash chain is sufficient now

4. Where to store chain heads?
   - **Recommendation:** Local file alongside checkpoint, simple and reliable
