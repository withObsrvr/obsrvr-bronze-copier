package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// LocalStore writes parquet files to the local filesystem.
type LocalStore struct {
	baseDir string
	prefix  string
}

// NewLocalStore creates a new local filesystem store.
func NewLocalStore(baseDir, prefix string) (*LocalStore, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("create base directory %s: %w", baseDir, err)
	}

	return &LocalStore{
		baseDir: baseDir,
		prefix:  prefix,
	}, nil
}

// WriteParquet writes parquet bytes to the local filesystem.
func (s *LocalStore) WriteParquet(ctx context.Context, ref PartitionRef, data []byte) error {
	path := filepath.Join(s.baseDir, ref.Path(s.prefix))

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create directory %s: %w", dir, err)
	}

	// Write atomically using temp file + rename
	tempPath := path + ".tmp"

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("write temp file %s: %w", tempPath, err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		// Clean up temp file on rename failure
		os.Remove(tempPath)
		return fmt.Errorf("rename %s to %s: %w", tempPath, path, err)
	}

	return nil
}

// WriteManifest writes a manifest file to the local filesystem.
func (s *LocalStore) WriteManifest(ctx context.Context, ref PartitionRef, manifest *Manifest) error {
	path := filepath.Join(s.baseDir, ref.ManifestPath(s.prefix))

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create directory %s: %w", dir, err)
	}

	// Marshal manifest to JSON
	data, err := manifest.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	// Write atomically
	tempPath := path + ".tmp"

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("write temp file %s: %w", tempPath, err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("rename %s to %s: %w", tempPath, path, err)
	}

	return nil
}

// Exists checks if a partition already exists.
func (s *LocalStore) Exists(ctx context.Context, ref PartitionRef) (bool, error) {
	path := filepath.Join(s.baseDir, ref.Path(s.prefix))
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// URI returns the canonical URI for the given key.
func (s *LocalStore) URI(key string) string {
	absPath := filepath.Join(s.baseDir, key)
	return "file://" + absPath
}

// Close is a no-op for local storage.
func (s *LocalStore) Close() error {
	return nil
}
