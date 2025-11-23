package storage

import (
	"context"
	"fmt"
	"net/url"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob" // S3 driver
)

// S3Store writes parquet files to S3-compatible storage.
type S3Store struct {
	bucket *blob.Bucket
	prefix string
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
		bucket: bucket,
		prefix: prefix,
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

// Close releases the bucket connection.
func (s *S3Store) Close() error {
	if s.bucket != nil {
		return s.bucket.Close()
	}
	return nil
}
