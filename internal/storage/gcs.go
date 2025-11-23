package storage

import (
	"context"
	"fmt"

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
