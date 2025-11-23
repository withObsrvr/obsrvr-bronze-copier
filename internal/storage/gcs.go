package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob" // GCS driver
)

// GCSStore writes parquet files to Google Cloud Storage.
type GCSStore struct {
	bucket     *blob.Bucket
	bucketName string
	prefix     string
}

// NewGCSStore creates a new GCS store.
func NewGCSStore(bucketName, prefix string) (*GCSStore, error) {
	ctx := context.Background()

	bucket, err := blob.OpenBucket(ctx, fmt.Sprintf("gs://%s", bucketName))
	if err != nil {
		return nil, fmt.Errorf("open GCS bucket %s: %w", bucketName, err)
	}

	return &GCSStore{
		bucket:     bucket,
		bucketName: bucketName,
		prefix:     prefix,
	}, nil
}

// WriteParquet writes parquet bytes to GCS.
func (s *GCSStore) WriteParquet(ctx context.Context, ref PartitionRef, data []byte) error {
	path := ref.Path(s.prefix)

	w, err := s.bucket.NewWriter(ctx, path, nil)
	if err != nil {
		return fmt.Errorf("create writer for %s: %w", path, err)
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return fmt.Errorf("write data to %s: %w", path, err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer for %s: %w", path, err)
	}

	return nil
}

// WriteManifest writes a manifest file to GCS.
func (s *GCSStore) WriteManifest(ctx context.Context, ref PartitionRef, manifest *Manifest) error {
	path := ref.ManifestPath(s.prefix)

	data, err := manifest.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	w, err := s.bucket.NewWriter(ctx, path, nil)
	if err != nil {
		return fmt.Errorf("create writer for %s: %w", path, err)
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return fmt.Errorf("write manifest to %s: %w", path, err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer for %s: %w", path, err)
	}

	return nil
}

// Exists checks if a partition already exists in GCS.
func (s *GCSStore) Exists(ctx context.Context, ref PartitionRef) (bool, error) {
	path := ref.Path(s.prefix)
	return s.bucket.Exists(ctx, path)
}

// URI returns the canonical URI for the given key.
func (s *GCSStore) URI(key string) string {
	return fmt.Sprintf("gs://%s/%s", s.bucketName, key)
}

// Close releases the bucket connection.
func (s *GCSStore) Close() error {
	if s.bucket != nil {
		return s.bucket.Close()
	}
	return nil
}

// --- AtomicStore implementation ---

// WriteParquetTemp writes parquet bytes to a temporary location.
func (s *GCSStore) WriteParquetTemp(ctx context.Context, ref PartitionRef, data []byte) (string, error) {
	tempKey := ref.Path(s.prefix) + ".tmp." + uuid.New().String()

	w, err := s.bucket.NewWriter(ctx, tempKey, nil)
	if err != nil {
		return "", fmt.Errorf("create writer for %s: %w", tempKey, err)
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return "", fmt.Errorf("write data to %s: %w", tempKey, err)
	}

	if err := w.Close(); err != nil {
		return "", fmt.Errorf("close writer for %s: %w", tempKey, err)
	}

	return tempKey, nil
}

// WriteManifestTemp writes a manifest to a temporary location.
func (s *GCSStore) WriteManifestTemp(ctx context.Context, ref PartitionRef, manifest *Manifest) (string, error) {
	tempKey := ref.ManifestPath(s.prefix) + ".tmp." + uuid.New().String()

	data, err := manifest.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("marshal manifest: %w", err)
	}

	w, err := s.bucket.NewWriter(ctx, tempKey, nil)
	if err != nil {
		return "", fmt.Errorf("create writer for %s: %w", tempKey, err)
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return "", fmt.Errorf("write manifest to %s: %w", tempKey, err)
	}

	if err := w.Close(); err != nil {
		return "", fmt.Errorf("close writer for %s: %w", tempKey, err)
	}

	return tempKey, nil
}

// Finalize atomically moves temp files to their canonical location.
// Uses copy + delete pattern.
func (s *GCSStore) Finalize(ctx context.Context, ref PartitionRef, tempKeys []string) error {
	finalKeys := []string{
		ref.Path(s.prefix),
		ref.ManifestPath(s.prefix),
	}

	if len(tempKeys) != len(finalKeys) {
		return fmt.Errorf("expected %d temp keys, got %d", len(finalKeys), len(tempKeys))
	}

	// Copy all temp files to final locations
	for i, tempKey := range tempKeys {
		finalKey := finalKeys[i]

		if err := s.copyObject(ctx, tempKey, finalKey); err != nil {
			// Rollback: delete any copied objects
			for j := 0; j < i; j++ {
				s.bucket.Delete(ctx, finalKeys[j])
			}
			// Clean up temp files
			s.Abort(ctx, tempKeys)
			return fmt.Errorf("finalize %s -> %s: %w", tempKey, finalKey, err)
		}
	}

	// Delete all temp files after successful copy
	for _, tempKey := range tempKeys {
		s.bucket.Delete(ctx, tempKey) // ignore errors
	}

	return nil
}

// copyObject copies an object within the bucket.
func (s *GCSStore) copyObject(ctx context.Context, srcKey, dstKey string) error {
	// Read source
	r, err := s.bucket.NewReader(ctx, srcKey, nil)
	if err != nil {
		return fmt.Errorf("open source %s: %w", srcKey, err)
	}
	defer r.Close()

	// Write to destination
	w, err := s.bucket.NewWriter(ctx, dstKey, nil)
	if err != nil {
		return fmt.Errorf("create destination %s: %w", dstKey, err)
	}

	if _, err := io.Copy(w, r); err != nil {
		w.Close()
		return fmt.Errorf("copy to %s: %w", dstKey, err)
	}

	return w.Close()
}

// Abort removes temporary files without publishing.
func (s *GCSStore) Abort(ctx context.Context, tempKeys []string) error {
	var lastErr error
	for _, key := range tempKeys {
		if err := s.bucket.Delete(ctx, key); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Head returns metadata about a stored object.
func (s *GCSStore) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	attrs, err := s.bucket.Attributes(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get attributes for %s: %w", key, err)
	}

	return &ObjectInfo{
		Key:     key,
		Size:    attrs.Size,
		ETag:    attrs.ETag,
		ModTime: attrs.ModTime,
	}, nil
}

// List returns all keys with the given prefix.
func (s *GCSStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string

	iter := s.bucket.List(&blob.ListOptions{
		Prefix: prefix,
	})

	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		// Skip temp files
		if obj.IsDir {
			continue
		}
		keys = append(keys, obj.Key)
	}

	return keys, nil
}

// Verify GCSStore implements AtomicStore.
var _ AtomicStore = (*GCSStore)(nil)
