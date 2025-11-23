package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/google/uuid"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob" // S3 driver
)

// S3Store writes parquet files to S3-compatible storage.
type S3Store struct {
	bucket     *blob.Bucket
	bucketName string
	prefix     string
}

// NewS3Store creates a new S3-compatible store.
// Works with AWS S3, Backblaze B2, Cloudflare R2, and MinIO.
func NewS3Store(bucketName, prefix, endpoint, region string) (*S3Store, error) {
	ctx := context.Background()

	// Build URL for gocloud.dev
	bucketURL := fmt.Sprintf("s3://%s", bucketName)

	params := url.Values{}
	if region != "" {
		params.Set("region", region)
	}
	if endpoint != "" {
		params.Set("endpoint", endpoint)
		params.Set("s3ForcePathStyle", "true")
	}
	if len(params) > 0 {
		bucketURL = bucketURL + "?" + params.Encode()
	}

	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("open S3 bucket %s: %w", bucketName, err)
	}

	return &S3Store{
		bucket:     bucket,
		bucketName: bucketName,
		prefix:     prefix,
	}, nil
}

// WriteParquet writes parquet bytes to S3.
func (s *S3Store) WriteParquet(ctx context.Context, ref PartitionRef, data []byte) error {
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

// WriteManifest writes a manifest file to S3.
func (s *S3Store) WriteManifest(ctx context.Context, ref PartitionRef, manifest *Manifest) error {
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

// Exists checks if a partition already exists in S3.
func (s *S3Store) Exists(ctx context.Context, ref PartitionRef) (bool, error) {
	path := ref.Path(s.prefix)
	return s.bucket.Exists(ctx, path)
}

// URI returns the canonical URI for the given key.
func (s *S3Store) URI(key string) string {
	return fmt.Sprintf("s3://%s/%s", s.bucketName, key)
}

// Close releases the bucket connection.
func (s *S3Store) Close() error {
	if s.bucket != nil {
		return s.bucket.Close()
	}
	return nil
}

// --- AtomicStore implementation ---

// WriteParquetTemp writes parquet bytes to a temporary location.
func (s *S3Store) WriteParquetTemp(ctx context.Context, ref PartitionRef, data []byte) (string, error) {
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
func (s *S3Store) WriteManifestTemp(ctx context.Context, ref PartitionRef, manifest *Manifest) (string, error) {
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
// Uses copy + delete pattern (S3 doesn't support rename).
func (s *S3Store) Finalize(ctx context.Context, ref PartitionRef, tempKeys []string) error {
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
func (s *S3Store) copyObject(ctx context.Context, srcKey, dstKey string) error {
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
func (s *S3Store) Abort(ctx context.Context, tempKeys []string) error {
	var lastErr error
	for _, key := range tempKeys {
		if err := s.bucket.Delete(ctx, key); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Head returns metadata about a stored object.
func (s *S3Store) Head(ctx context.Context, key string) (*ObjectInfo, error) {
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
func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
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
		if obj.IsDir {
			continue
		}
		keys = append(keys, obj.Key)
	}

	return keys, nil
}

// Verify S3Store implements AtomicStore.
var _ AtomicStore = (*S3Store)(nil)
