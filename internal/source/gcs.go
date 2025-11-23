package source

import (
	"context"
	"fmt"
	"io"
	"log"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob" // GCS driver
)

// GCSSource reads ledger files from Google Cloud Storage.
type GCSSource struct {
	bucket  *blob.Bucket
	prefix  string
	decoder *Decoder
}

// NewGCSSource creates a new GCS source.
// Uses Application Default Credentials (ADC) for authentication.
func NewGCSSource(bucketName, prefix string) (*GCSSource, error) {
	ctx := context.Background()

	// Open bucket using gocloud.dev
	// URL format: gs://bucket-name
	bucket, err := blob.OpenBucket(ctx, fmt.Sprintf("gs://%s", bucketName))
	if err != nil {
		return nil, fmt.Errorf("open GCS bucket %s: %w", bucketName, err)
	}

	decoder, err := NewDecoder()
	if err != nil {
		bucket.Close()
		return nil, fmt.Errorf("create decoder: %w", err)
	}

	return &GCSSource{
		bucket:  bucket,
		prefix:  prefix,
		decoder: decoder,
	}, nil
}

// Stream implements LedgerSource.Stream for GCS.
func (s *GCSSource) Stream(ctx context.Context, start, end uint32) (<-chan LedgerCloseMeta, <-chan error) {
	ledgerCh := make(chan LedgerCloseMeta, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(ledgerCh)
		defer close(errCh)

		// Build index by listing objects (optimized for range)
		index, err := s.buildIndexForRange(ctx, start, end)
		if err != nil {
			errCh <- fmt.Errorf("build index: %w", err)
			return
		}

		log.Printf("[source:gcs] indexed %d ledger files for range %d-%d", index.Count(), start, end)

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

		log.Printf("[source:gcs] streaming %d ledgers (%d-%d)",
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

		log.Printf("[source:gcs] stream complete")
	}()

	return ledgerCh, errCh
}

// Close releases resources.
func (s *GCSSource) Close() error {
	if s.decoder != nil {
		s.decoder.Close()
	}
	if s.bucket != nil {
		return s.bucket.Close()
	}
	return nil
}

// buildIndexForRange builds an index for a specific ledger range.
// This is optimized to only scan directories that might contain the requested range.
func (s *GCSSource) buildIndexForRange(ctx context.Context, start, end uint32) (*LedgerIndex, error) {
	index := NewLedgerIndex()

	// First, list directories at the top level
	rangeDirs, err := s.findRangeDirectories(ctx)
	if err != nil {
		return nil, err
	}

	log.Printf("[source:gcs] found %d range directories", len(rangeDirs))

	// Find directories that overlap with our requested range
	for _, rd := range rangeDirs {
		// Check if this directory overlaps with requested range
		if end > 0 && rd.Start > end {
			continue
		}
		if rd.End < start {
			continue
		}

		log.Printf("[source:gcs] scanning directory %s (contains ledgers %d-%d)", rd.Path, rd.Start, rd.End)

		// List files in this directory
		if err := s.indexDirectory(ctx, rd.Path, index); err != nil {
			return nil, fmt.Errorf("index directory %s: %w", rd.Path, err)
		}
	}

	index.Sort()
	return index, nil
}

// rangeDirectory represents a range directory in the archive.
type rangeDirectory struct {
	Path  string
	Start uint32
	End   uint32
}

// findRangeDirectories lists top-level directories and parses their ranges.
func (s *GCSSource) findRangeDirectories(ctx context.Context) ([]rangeDirectory, error) {
	var dirs []rangeDirectory

	iter := s.bucket.List(&blob.ListOptions{
		Prefix:    s.prefix,
		Delimiter: "/",
	})

	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list directories: %w", err)
		}

		// We only want directories
		if !obj.IsDir {
			continue
		}

		// Parse the range from directory name
		start, end, ok := ParseRangeDirectory(obj.Key)
		if !ok {
			continue
		}

		dirs = append(dirs, rangeDirectory{
			Path:  obj.Key,
			Start: start,
			End:   end,
		})
	}

	return dirs, nil
}

// indexDirectory lists and indexes all XDR files in a specific directory.
func (s *GCSSource) indexDirectory(ctx context.Context, dirPath string, index *LedgerIndex) error {
	iter := s.bucket.List(&blob.ListOptions{
		Prefix: dirPath,
	})

	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
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

	return nil
}

// readObject reads and decodes a single object from GCS.
func (s *GCSSource) readObject(ctx context.Context, key string) (*LedgerCloseMeta, error) {
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
