# Pitch 10: Atomic Storage Implementation

**Appetite:** 2 days
**Risk:** Medium
**Dependencies:** None

---

## Problem

We defined the `AtomicStore` interface in v1.0:

```go
type AtomicStore interface {
    BronzeStore
    WriteParquetTemp(ctx, ref, bytes) (tempKey, error)
    WriteManifestTemp(ctx, ref, manifest) (tempKey, error)
    Finalize(ctx, ref, tempKeys) error
    Abort(ctx, tempKeys) error
    Head(ctx, key) (*ObjectInfo, error)
    List(ctx, prefix) ([]string, error)
}
```

But we didn't implement it. The current storage writes directly to final paths, which means:
- Partial writes can leave corrupt files
- No rollback on failure
- Race conditions possible with concurrent writers

---

## Solution

### Local Storage: Rename-based Atomicity

```go
// internal/storage/local_atomic.go

func (s *LocalStore) WriteParquetTemp(ctx context.Context, ref PartitionRef, data []byte) (string, error) {
    // Write to temp file in same directory (for atomic rename)
    tempPath := ref.Path(s.prefix) + ".tmp." + randomSuffix()

    if err := os.MkdirAll(filepath.Dir(tempPath), 0755); err != nil {
        return "", err
    }

    if err := os.WriteFile(tempPath, data, 0644); err != nil {
        return "", err
    }

    return tempPath, nil
}

func (s *LocalStore) Finalize(ctx context.Context, ref PartitionRef, tempKeys []string) error {
    // Map temp keys to final keys
    finalPaths := []string{
        ref.Path(s.prefix),
        ref.ManifestPath(s.prefix),
    }

    // Rename all temp files to final (atomic on POSIX)
    for i, tempKey := range tempKeys {
        finalPath := finalPaths[i]
        if err := os.Rename(tempKey, finalPath); err != nil {
            // Rollback: remove any already-renamed files
            for j := 0; j < i; j++ {
                os.Remove(finalPaths[j])
            }
            return fmt.Errorf("finalize %s: %w", tempKey, err)
        }
    }

    return nil
}

func (s *LocalStore) Abort(ctx context.Context, tempKeys []string) error {
    for _, key := range tempKeys {
        os.Remove(key) // ignore errors
    }
    return nil
}
```

### GCS Storage: Generation-based Atomicity

```go
// internal/storage/gcs_atomic.go

func (s *GCSStore) WriteParquetTemp(ctx context.Context, ref PartitionRef, data []byte) (string, error) {
    // Write to temp object with unique suffix
    tempKey := ref.Path(s.prefix) + ".tmp." + uuid.New().String()

    obj := s.bucket.Object(tempKey)
    w := obj.NewWriter(ctx)
    if _, err := w.Write(data); err != nil {
        return "", err
    }
    if err := w.Close(); err != nil {
        return "", err
    }

    return tempKey, nil
}

func (s *GCSStore) Finalize(ctx context.Context, ref PartitionRef, tempKeys []string) error {
    finalKeys := []string{
        ref.Path(s.prefix),
        ref.ManifestPath(s.prefix),
    }

    for i, tempKey := range tempKeys {
        finalKey := finalKeys[i]

        // Copy temp to final (with precondition: final doesn't exist)
        src := s.bucket.Object(tempKey)
        dst := s.bucket.Object(finalKey).If(storage.Conditions{DoesNotExist: true})

        if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
            // Rollback: delete any copied objects
            for j := 0; j < i; j++ {
                s.bucket.Object(finalKeys[j]).Delete(ctx)
            }
            // Clean up temp files
            s.Abort(ctx, tempKeys)
            return fmt.Errorf("finalize %s: %w", tempKey, err)
        }

        // Delete temp object
        src.Delete(ctx)
    }

    return nil
}
```

### S3 Storage: Similar to GCS

S3 uses copy + delete, with conditional writes where supported.

---

## Scope

### Must Have
- LocalStore implements AtomicStore
- GCSStore implements AtomicStore
- S3Store implements AtomicStore
- `publishPartition` uses atomic writes when available

### Nice to Have
- ETag/generation verification on read
- Checksum verification after copy

### Not Doing
- Cross-bucket atomic operations
- Distributed transactions
- Two-phase commit

---

## Integration with publishPartition

```go
func (c *Copier) publishPartition(ctx context.Context, part tables.Partition) error {
    // ... generate parquet ...

    // Check if store supports atomic writes
    atomicStore, isAtomic := c.store.(storage.AtomicStore)

    if isAtomic {
        // Atomic path: temp -> finalize
        var tempKeys []string

        for tableName, data := range output.Parquets {
            tempKey, err := atomicStore.WriteParquetTemp(ctx, ref, data)
            if err != nil {
                atomicStore.Abort(ctx, tempKeys)
                return err
            }
            tempKeys = append(tempKeys, tempKey)
        }

        manifestTemp, err := atomicStore.WriteManifestTemp(ctx, ref, manifest)
        if err != nil {
            atomicStore.Abort(ctx, tempKeys)
            return err
        }
        tempKeys = append(tempKeys, manifestTemp)

        // Atomic finalize
        if err := atomicStore.Finalize(ctx, ref, tempKeys); err != nil {
            return err
        }
    } else {
        // Fallback: direct write (non-atomic)
        c.store.WriteParquet(ctx, ref, data)
        c.store.WriteManifest(ctx, ref, manifest)
    }

    // ... metadata, PAS, checkpoint ...
}
```

---

## Done When

1. LocalStore implements AtomicStore with rename
2. GCSStore implements AtomicStore with copy+delete
3. S3Store implements AtomicStore
4. `publishPartition` uses atomic path when available
5. Test: Simulated failure during finalize leaves no partial files
6. Test: Concurrent writers don't corrupt data
