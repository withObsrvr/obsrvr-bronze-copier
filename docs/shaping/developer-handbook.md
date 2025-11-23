
---

# 📘 **Obsrvr Bronze Layer — Developer Handbook**

**Version 1.0**
**Owner:** Tillman Mosley
**Audience:** Developers, contributors, and operators working on the Obsrvr Bronze Layer and Bronze Copier.

---

# **Table of Contents**

1. [Introduction](#introduction)
2. [What the Bronze Layer Is](#what-the-bronze-layer-is)
3. [What the Bronze Layer Is Not](#what-the-bronze-layer-is-not)
4. [Bronze Architecture Overview](#bronze-architecture-overview)
5. [Ingestion Sources (Galexie Archives)](#ingestion-sources-galexie-archives)
6. [Bronze Copier Responsibilities](#bronze-copier-responsibilities)
7. [Bronze Storage Format](#bronze-storage-format)
8. [Versioning Model (v1, v2, …)](#versioning-model-v1-v2-)
9. [Deterministic Parquet Specification](#deterministic-parquet-specification)
10. [Metadata & Catalog](#metadata--catalog)
11. [Public Audit Stream (PAS)](#public-audit-stream-pas)
12. [Operational Model, Scheduling & Concurrency](#operational-model-scheduling--concurrency)
13. [Failure Modes & Recovery](#failure-modes--recovery)
14. [Developer Invariants](#developer-invariants)
15. [Testing Strategy](#testing-strategy)
16. [Bronze Copier Minimal Go Skeleton](#bronze-copier-minimal-go-skeleton)
17. [Next Steps & Roadmap](#next-steps--roadmap)

---

# 1. **Introduction**

The Bronze Layer is the foundation of the Obsrvr Truth Stack. It provides **an immutable, versioned, reproducible archive** of Stellar ledger data across protocol eras and historical corrections. It is the single source of truth for all higher layers (Silver, Gold, Flow, Agents).

This handbook describes:

* How Bronze works
* How Bronze Copier is implemented
* How developers contribute safely
* What guarantees Bronze must uphold

---

# 2. **What the Bronze Layer Is**

Bronze is:

* **Raw-but-structured preservation** of Stellar data (XDR → Parquet with no semantic interpretation).
* **Versioned** for each protocol era or historical correction.
* **Immutable**: once written, a partition never changes.
* **Deterministic**: two runs produce identical byte-for-byte outputs.
* **Reproducible**: any 3rd party can regenerate Bronze from raw archives.
* **PAS-verifiable**: every partition has cryptographically linked audit events.

Bronze is a *ledger vault*, not a database.

---

# 3. **What the Bronze Layer Is Not**

Bronze is **NOT**:

* A queryable analytics layer
* A schema-evolving domain layer
* A real-time streaming service
* An RPC or indexer
* A ducklake catalog
* A Flow pipeline component

All of those live in **Silver / Gold / Flow**.

Bronze is intentionally dumb — and that is its power.

---

# 4. **Bronze Architecture Overview**

```
       ┌──────────────────────────────┐
       │        Galexie Archives      │
       │ (landing buckets, raw XDR)   │
       └──────────────┬───────────────┘
                      │
                      │  (copy + checksum + transform)
                      ▼
       ┌──────────────────────────────┐
       │       Bronze Copier          │
       │  - deterministic parquet     │
       │  - version routing (v1/v2)   │
       │  - PAS emission              │
       └──────────────┬───────────────┘
                      │
                      │  (write-once)
                      ▼
       ┌──────────────────────────────┐
       │      Bronze Storage (S3/R2)  │
       │ /bronze/v1/ledgers/...       │
       │ /bronze/v2/ledgers/...       │
       └──────────────┬───────────────┘
                      │
                      │  (read-only)
                      ▼
       ┌──────────────────────────────┐
       │   Catalog Metadata           │
       │  lineage, checkpoints, qc    │
       └──────────────┬───────────────┘
                      │
                      ▼
       ┌──────────────────────────────┐
       │     PAS (Public Audit)       │
       │ chain-linked events          │
       └──────────────────────────────┘
```

---

# 5. **Ingestion Sources (Galexie Archives)**

### **Input**

Galexie landing bucket:

```
gs://obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet/
  FC6DEFFF--59904000-59967999/
    FC6D1D66--59957913.xdr.zst
    FC6D1B07--59957914.xdr.zst
```

Bronze Copier must support:

* **XDR.zst archives from Galexie**
* **multiple eras**
* **fix-backfills** from SDF (Protocol 23, Protocol 24, P75 CT confidential transactions, etc.)

---

# 6. **Bronze Copier Responsibilities**

### **Core Responsibilities**

1. Detect new Galexie objects
2. Copy into Bronze era prefix (`v1`, `v2`, etc.)
3. Normalize filename & path structure
4. Generate deterministic Parquet representation
5. Compute cryptographic checksums
6. Emit PAS events
7. Write lineage metadata
8. Never mutate, rewrite, or delete Bronze partitions

### **Non-Responsibilities**

* Decoding SCVAL
* Creating analytics schemas
* Running Flow processors
* Building aggregates
* Service-level orchestration

That all happens in Silver/Gold.

---

# 7. **Bronze Storage Format**

### **Path Convention**

```
bronze/
  v1/
    ledgers/
      05990000/
        ledger-059957913.parquet
        ledger-059957914.parquet
  v2/
    ledgers/
      05990000/
        ledger-059957913.parquet
```

### **Object immutability**

* S3 Object Lock recommended
* Versioned buckets optional but encouraged
* Writes are append-only
* Never overwrite parity

---

# 8. **Versioning Model (v1, v2, …)**

A new version is created when:

* Protocol upgrade changes XDR semantics
* On-chain event format changes (e.g. P23 unified event streams)
* Historical corrections (e.g. P23/P24 replay)
* Confidential Transactions CAP-75 changes ledger representation
* SDF announces a rebuild requirement

**Bronze version ≠ app version ≠ protocol number**, but commonly aligned.

---

# 9. **Deterministic Parquet Specification**

### Requirements:

* Canonical column order
* Canonical row order
* Canonical null handling
* No timezone drift
* Stable decimal encoding
* Stable compression (zstd level pinned)
* Stable dictionary encoding on/off pinned

This ensures a ledger copied today produces byte-identical Parquet to a ledger copied next year.

This is essential for PAS.

---

# 10. **Metadata & Catalog**

Every partition commit MUST write metadata into the catalog:

### `_meta_lineage`

* ledger_start
* ledger_end
* partition_path
* record_count
* checksum_sha256
* pas_event_id

### `_meta_datasets`

* version
* domain (`ledgers`, `transactions`, etc.)
* schema hash

### `_meta_quality`

* QC pass/fail flags
* checksum validity
* file size / expected bounds

Catalog is your **internal truth index**.

---

# 11. **Public Audit Stream (PAS)**

PAS is the public verification system for Bronze.

Each Bronze write produces a PAS event:

```json
{
  "event_id": "pas_v1_059957913",
  "ledger_seq": 59957913,
  "bronze_version": "v2",
  "partition_path": "bronze/v2/ledgers/05990000/ledger-059957913.parquet",
  "data_hash": "sha256:a3f7b2c...",
  "prev_hash": "sha256:d1e2f3...",
  "timestamp": "2025-01-01T12:00:00Z"
}
```

PAS forms a **hash chain** so auditors can validate the entire Bronze dataset.

---

# 12. **Operational Model, Scheduling & Concurrency**

### **Copier Behavior**

* Runs continuously or as cron
* Fault-tolerant: resumes mid-epoch
* Parallel workers per 1,000-ledger chunk
* Auto-detects gaps and retries
* Guaranteed at-least-once for PAS events
* Buffered but not backlogged

### **Throughput targets**

* 50–100 ledgers/sec (local testing)
* 10+ MB/s sustained
* Parallelization via worker pool

---

# 13. **Failure Modes & Recovery**

### 1. **Partial write**

→ Delete incomplete object and retry
→ PAS emits “rollback” event (optional)

### 2. **Checksum mismatch**

→ Mark QC fail
→ Halt ingestion for that ledger
→ Escalate

### 3. **GCS/R2/S3 outage**

→ Buffer locally
→ Retry w/ exponential backoff

### 4. **Version boundary incorrect**

→ Abort
→ Require manual operator confirmation

---

# 14. **Developer Invariants**

Bronze MUST follow these rules:

1. **Never mutate committed data.**
2. **All parquet outputs MUST be deterministic.**
3. **All writes MUST produce PAS.**
4. **Version boundaries MUST be enforced.**
5. **Copier MUST NOT interpret smart contract values.**
6. **Copier MUST degrade gracefully (no panics).**
7. **Metadata MUST remain internally consistent.**
8. **Storage paths MUST be canonical.**

Violating invariants = corrupting the truth layer.

---

# 15. **Testing Strategy**

### **Unit tests**

* Parquet determinism
* Hash consistency
* Path normalization
* Version routing logic

### **Integration tests**

* End-to-end “ingest 100 ledgers”
* Cross-version stitching
* S3/GCS mock tests

### **PAS chain validation tests**

* Recompute chain
* Verify integrity
* Ensure no missing events

---

# 16. **Bronze Copier Minimal Go Skeleton**

Here is the minimal, production-aligned skeleton:

```go
package main

import (
    "context"
    "log"

    "github.com/withObsrvr/obsrvr-bronze-copier/internal/archive"
    "github.com/withObsrvr/obsrvr-bronze-copier/internal/parquet"
    "github.com/withObsrvr/obsrvr-bronze-copier/internal/storage"
    "github.com/withObsrvr/obsrvr-bronze-copier/internal/catalog"
    "github.com/withObsrvr/obsrvr-bronze-copier/internal/pas"
)

func main() {
    ctx := context.Background()

    // Initialize subsystems
    src := archive.NewGalexieSource(os.Getenv("GALEXIE_BUCKET"))
    dest := storage.NewS3(os.Getenv("BRONZE_BUCKET"))
    cat := catalog.NewPostgres(os.Getenv("CATALOG_DSN"))
    pasEmitter := pas.NewEmitter(os.Getenv("PAS_BUCKET"))

    for {
        // 1. Find next ledger to process
        ledger, err := src.NextLedger(ctx)
        if err != nil {
            log.Println("no new ledgers:", err)
            sleep()
            continue
        }

        // 2. Load raw XDR
        raw, err := src.Download(ctx, ledger)
        if err != nil {
            log.Println("download failed:", err)
            continue
        }

        // 3. Convert to deterministic parquet
        parquetBytes, checksum, err := parquet.FromXDR(raw)
        if err != nil {
            log.Println("parquet transform failed:", err)
            continue
        }

        // 4. Write parquet to Bronze storage
        path, err := dest.Write(ctx, ledger, parquetBytes)
        if err != nil {
            log.Println("storage write failed:", err)
            continue
        }

        // 5. Update lineage catalog
        if err := cat.Record(ctx, ledger, path, checksum); err != nil {
            log.Println("catalog write failed:", err)
            continue
        }

        // 6. Emit PAS event
        if err := pasEmitter.Emit(ctx, ledger, path, checksum); err != nil {
            log.Println("PAS emit failed:", err)
        }

        log.Printf("ledger %d committed to Bronze\n", ledger)
    }
}
```

This is intentionally minimal but real.

---

# 17. **Next Steps & Roadmap**

### **v1.0 Bronze (active now)**

* Copier implemented
* Deterministic parquet
* Catalog with lineage
* PAS v1 chain

### **v1.1 Bronze**

* Multi-protocol-era support
* Version router
* Backfill orchestration for P23/P24/P75

### **v2.0 Bronze**

* Confidential Transactions (CAP-75) support
* On-prem MinIO deployments
* Public Bronze mirror

---

# **Done.**

