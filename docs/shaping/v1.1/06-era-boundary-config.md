# Pitch 06: Era Boundary Configuration

**Appetite:** 2 days
**Risk:** Low
**Dependencies:** None

---

## Problem

Bronze Copier currently processes one era at a time with manual configuration:

```yaml
era:
  era_id: "pre_p23"
  version_label: "v1"
  ledger_start: 1
  ledger_end: 50000000
```

This requires operators to:
1. Know exact ledger boundaries for protocol upgrades
2. Manually configure separate copier instances per era
3. Coordinate cutover when upgrades happen

For production backfill and live tailing, we need the copier to understand era boundaries and route ledgers automatically.

---

## Solution

### Era Boundary Table in Config

Add an explicit era boundary table:

```yaml
eras:
  - era_id: "pre_p23"
    version_label: "v1"
    ledger_start: 1
    ledger_end: 52345677        # Last ledger before P23 upgrade
    partition_size: 10000

  - era_id: "p23_plus"
    version_label: "v2"
    ledger_start: 52345678      # First P23 ledger
    ledger_end: 0               # 0 = unbounded (for live tail)
    partition_size: 10000

# Which era(s) to process in this run
active_eras:
  - "pre_p23"                   # Can list multiple for parallel processing
```

### Era Router

New `internal/era/router.go`:

```go
type EraRouter struct {
    eras []EraConfig
}

// Route returns the era for a given ledger sequence
func (r *EraRouter) Route(ledgerSeq uint32) (*EraConfig, error) {
    for _, era := range r.eras {
        if ledgerSeq >= era.LedgerStart &&
           (era.LedgerEnd == 0 || ledgerSeq <= era.LedgerEnd) {
            return &era, nil
        }
    }
    return nil, ErrNoMatchingEra
}

// ActiveEras returns only the eras configured for this run
func (r *EraRouter) ActiveEras() []EraConfig
```

### Copier Changes

The copier uses the router to:
1. Filter source ledgers to only active eras
2. Route each ledger to correct era's partition builder
3. Write to era-specific storage paths

---

## Scope

### Must Have
- Era boundary table in YAML config
- Era router with boundary lookup
- Copier routes ledgers to correct era
- Separate partition builders per era

### Nice to Have
- CLI flag to select active eras: `--eras pre_p23,p23_plus`
- Validation that era boundaries don't overlap

### Not Doing
- Automatic era detection from ledger protocol version
- Dynamic era discovery from external source
- Cross-era partition spanning (partitions stay within one era)

---

## Rabbit Holes

**Don't:**
- Try to auto-detect eras from XDR protocol version field
- Build a "smart" routing system that infers boundaries
- Support overlapping era ranges

**Why:** Keep it simple. Operators know their upgrade boundaries. Auto-detection adds complexity and edge cases.

---

## Example Config

```yaml
# config.yaml for full backfill
eras:
  - era_id: "pre_p23"
    version_label: "v1"
    ledger_start: 1
    ledger_end: 52345677
    partition_size: 10000

  - era_id: "p23_plus"
    version_label: "v2"
    ledger_start: 52345678
    ledger_end: 0
    partition_size: 10000

# Process both eras
active_eras: ["pre_p23", "p23_plus"]

source:
  mode: "datastore"
  datastore_type: "GCS"
  datastore_path: "obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet"
```

---

## Done When

1. Config supports era boundary table
2. `EraRouter` correctly routes ledgers to eras
3. Copier creates separate partition builders per era
4. Storage paths include era: `{network}/{era_id}/{version}/...`
5. Test: Mixed-era ledger range routes correctly
