package source

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob" // S3 driver
)

// S3Source reads ledger files from S3-compatible storage.
// Works with AWS S3, Backblaze B2, Cloudflare R2, and MinIO.
type S3Source struct {
	bucket  *blob.Bucket
	prefix  string
	decoder *Decoder
}

// NewS3Source creates a new S3-compatible source.
// endpoint can be empty for AWS S3, or a custom URL for B2/R2/MinIO.
func NewS3Source(bucketName, prefix, endpoint, region string) (*S3Source, error) {
	ctx := context.Background()

	// Build URL for gocloud.dev
	// For AWS: s3://bucket-name?region=us-east-1
	// For custom endpoint: s3://bucket-name?endpoint=https://s3.us-west-000.backblazeb2.com&region=us-west-000
	bucketURL := fmt.Sprintf("s3://%s", bucketName)

	params := url.Values{}
	if region != "" {
		params.Set("region", region)
	}
	if endpoint != "" {
		params.Set("endpoint", endpoint)
		// For custom endpoints, we often need to disable host-style addressing
		params.Set("s3ForcePathStyle", "true")
	}
	if len(params) > 0 {
		bucketURL = bucketURL + "?" + params.Encode()
	}

	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("open S3 bucket %s: %w", bucketName, err)
	}

	decoder, err := NewDecoder()
	if err != nil {
		bucket.Close()
		return nil, fmt.Errorf("create decoder: %w", err)
	}

	return &S3Source{
		bucket:  bucket,
		prefix:  prefix,
		decoder: decoder,
	}, nil
}

// Stream implements LedgerSource.Stream for S3.
func (s *S3Source) Stream(ctx context.Context, start, end uint32) (<-chan LedgerCloseMeta, <-chan error) {
	ledgerCh := make(chan LedgerCloseMeta, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(ledgerCh)
		defer close(errCh)

		// Build index by listing objects
		index, err := s.buildIndex(ctx)
		if err != nil {
			errCh <- fmt.Errorf("build index: %w", err)
			return
		}

		log.Printf("[source:s3] indexed %d ledger files with prefix %s", index.Count(), s.prefix)

		if index.Count() == 0 {
			errCh <- fmt.Errorf("no ledger files found with prefix %s", s.prefix)
			return
		}

		// Get files in requested range
		files := index.GetRange(start, end)
		if len(files) == 0 {
			errCh <- fmt.Errorf("no files found for range %d-%d (available: %d-%d)",
				start, end, index.MinSequence(), index.MaxSequence())
			return
		}

		log.Printf("[source:s3] streaming %d ledgers (%d-%d)",
			len(files), files[0].LedgerSeq, files[len(files)-1].LedgerSeq)

		// Stream files in order
		for _, file := range files {
			select {
			case <-ctx.Done():
				return
			default:
			}

			lcm, err := s.readObject(ctx, file.Path)
			if err != nil {
				errCh <- fmt.Errorf("read ledger %d: %w", file.LedgerSeq, err)
				return
			}

			// Verify sequence matches filename
			if lcm.LedgerSeq != file.LedgerSeq {
				errCh <- fmt.Errorf("sequence mismatch: file says %d, XDR says %d",
					file.LedgerSeq, lcm.LedgerSeq)
				return
			}

			select {
			case ledgerCh <- *lcm:
			case <-ctx.Done():
				return
			}
		}

		log.Printf("[source:s3] stream complete")
	}()

	return ledgerCh, errCh
}

// Close releases resources.
func (s *S3Source) Close() error {
	if s.decoder != nil {
		s.decoder.Close()
	}
	if s.bucket != nil {
		return s.bucket.Close()
	}
	return nil
}

// buildIndex lists all objects with the prefix and indexes them.
func (s *S3Source) buildIndex(ctx context.Context) (*LedgerIndex, error) {
	index := NewLedgerIndex()

	iter := s.bucket.List(&blob.ListOptions{
		Prefix: s.prefix,
	})

	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}

		// Skip directories
		if obj.IsDir {
			continue
		}

		// Only process XDR files
		if !IsXDRFile(obj.Key) {
			continue
		}

		index.AddFile(obj.Key)
	}

	index.Sort()
	return index, nil
}

// readObject reads and decodes a single object from S3.
func (s *S3Source) readObject(ctx context.Context, key string) (*LedgerCloseMeta, error) {
	reader, err := s.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("open object %s: %w", key, err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read object %s: %w", key, err)
	}

	if IsCompressed(key) {
		return s.decoder.DecodeCompressed(data)
	}
	return s.decoder.DecodeRaw(data)
}
