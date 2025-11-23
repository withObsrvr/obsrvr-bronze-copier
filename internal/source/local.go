package source

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"
)

// LocalSource reads ledger files from the local filesystem.
type LocalSource struct {
	basePath string
	decoder  *Decoder
}

// NewLocalSource creates a new local filesystem source.
func NewLocalSource(basePath string) (*LocalSource, error) {
	// Verify path exists
	info, err := os.Stat(basePath)
	if err != nil {
		return nil, fmt.Errorf("invalid local path %s: %w", basePath, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("local path %s is not a directory", basePath)
	}

	decoder, err := NewDecoder()
	if err != nil {
		return nil, fmt.Errorf("create decoder: %w", err)
	}

	return &LocalSource{
		basePath: basePath,
		decoder:  decoder,
	}, nil
}

// Stream implements LedgerSource.Stream for local files.
func (s *LocalSource) Stream(ctx context.Context, start, end uint32) (<-chan LedgerCloseMeta, <-chan error) {
	ledgerCh := make(chan LedgerCloseMeta, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(ledgerCh)
		defer close(errCh)

		// Build index of available files
		index, err := s.buildIndex()
		if err != nil {
			errCh <- fmt.Errorf("build index: %w", err)
			return
		}

		log.Printf("[source:local] indexed %d ledger files in %s", index.Count(), s.basePath)

		if index.Count() == 0 {
			errCh <- fmt.Errorf("no ledger files found in %s", s.basePath)
			return
		}

		// Get files in requested range
		files := index.GetRange(start, end)
		if len(files) == 0 {
			errCh <- fmt.Errorf("no files found for range %d-%d (available: %d-%d)",
				start, end, index.MinSequence(), index.MaxSequence())
			return
		}

		log.Printf("[source:local] streaming %d ledgers (%d-%d)",
			len(files), files[0].LedgerSeq, files[len(files)-1].LedgerSeq)

		// Stream files in order
		for _, file := range files {
			select {
			case <-ctx.Done():
				return
			default:
			}

			lcm, err := s.readFile(file.Path)
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

		log.Printf("[source:local] stream complete")
	}()

	return ledgerCh, errCh
}

// Close releases resources.
func (s *LocalSource) Close() error {
	if s.decoder != nil {
		s.decoder.Close()
	}
	return nil
}

// buildIndex walks the directory tree and indexes all XDR files.
func (s *LocalSource) buildIndex() (*LedgerIndex, error) {
	index := NewLedgerIndex()

	err := filepath.WalkDir(s.basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Only process XDR files
		if !IsXDRFile(path) {
			return nil
		}

		index.AddFile(path)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("walk directory: %w", err)
	}

	index.Sort()
	return index, nil
}

// readFile reads and decodes a single ledger file.
func (s *LocalSource) readFile(path string) (*LedgerCloseMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	if IsCompressed(path) {
		return s.decoder.DecodeCompressed(data)
	}
	return s.decoder.DecodeRaw(data)
}

// WatchForNewFiles polls for new files (for tailing mode).
// This is a simple implementation; production might use inotify.
func (s *LocalSource) WatchForNewFiles(ctx context.Context, afterSeq uint32, interval time.Duration) <-chan LedgerFile {
	fileCh := make(chan LedgerFile, 10)

	go func() {
		defer close(fileCh)

		lastSeq := afterSeq
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				index, err := s.buildIndex()
				if err != nil {
					log.Printf("[source:local] watch error: %v", err)
					continue
				}

				// Find new files
				for _, f := range index.GetRange(lastSeq+1, 0) {
					select {
					case fileCh <- f:
						lastSeq = f.LedgerSeq
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return fileCh
}
