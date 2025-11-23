package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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

// --- AtomicStore implementation ---

// randomSuffix generates a random hex string for temp file uniqueness.
func randomSuffix() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// WriteParquetTemp writes parquet bytes to a temporary location.
// The temp file is in the same directory as the final location to ensure
// atomic rename works (must be on same filesystem).
func (s *LocalStore) WriteParquetTemp(ctx context.Context, ref PartitionRef, data []byte) (string, error) {
	finalPath := filepath.Join(s.baseDir, ref.Path(s.prefix))
	tempPath := finalPath + ".tmp." + randomSuffix()

	// Ensure directory exists
	dir := filepath.Dir(tempPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("create directory %s: %w", dir, err)
	}

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return "", fmt.Errorf("write temp file %s: %w", tempPath, err)
	}

	return tempPath, nil
}

// WriteManifestTemp writes a manifest to a temporary location.
func (s *LocalStore) WriteManifestTemp(ctx context.Context, ref PartitionRef, manifest *Manifest) (string, error) {
	finalPath := filepath.Join(s.baseDir, ref.ManifestPath(s.prefix))
	tempPath := finalPath + ".tmp." + randomSuffix()

	// Ensure directory exists
	dir := filepath.Dir(tempPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("create directory %s: %w", dir, err)
	}

	data, err := manifest.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("marshal manifest: %w", err)
	}

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return "", fmt.Errorf("write temp file %s: %w", tempPath, err)
	}

	return tempPath, nil
}

// Finalize atomically moves temp files to their canonical location.
// On POSIX systems, rename is atomic when source and dest are on the same filesystem.
func (s *LocalStore) Finalize(ctx context.Context, ref PartitionRef, tempKeys []string) error {
	// Build final paths in order: parquet first, then manifest
	finalPaths := []string{
		filepath.Join(s.baseDir, ref.Path(s.prefix)),
		filepath.Join(s.baseDir, ref.ManifestPath(s.prefix)),
	}

	// Validate we have the expected number of temp files
	if len(tempKeys) != len(finalPaths) {
		return fmt.Errorf("expected %d temp keys, got %d", len(finalPaths), len(tempKeys))
	}

	// Rename each temp file to its final location
	for i, tempKey := range tempKeys {
		finalPath := finalPaths[i]
		if err := os.Rename(tempKey, finalPath); err != nil {
			// Rollback: remove any already-renamed files
			for j := 0; j < i; j++ {
				os.Remove(finalPaths[j])
			}
			// Also remove remaining temp files
			for k := i; k < len(tempKeys); k++ {
				os.Remove(tempKeys[k])
			}
			return fmt.Errorf("finalize %s -> %s: %w", tempKey, finalPath, err)
		}
	}

	return nil
}

// Abort removes temporary files without publishing.
func (s *LocalStore) Abort(ctx context.Context, tempKeys []string) error {
	var lastErr error
	for _, key := range tempKeys {
		if err := os.Remove(key); err != nil && !os.IsNotExist(err) {
			lastErr = err
		}
	}
	return lastErr
}

// Head returns metadata about a stored object.
func (s *LocalStore) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	path := filepath.Join(s.baseDir, key)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("object not found: %s", key)
		}
		return nil, err
	}

	return &ObjectInfo{
		Key:     key,
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}, nil
}

// List returns all keys with the given prefix.
func (s *LocalStore) List(ctx context.Context, prefix string) ([]string, error) {
	searchDir := filepath.Join(s.baseDir, prefix)

	// Check if directory exists
	if _, err := os.Stat(searchDir); os.IsNotExist(err) {
		return []string{}, nil
	}

	var keys []string
	err := filepath.WalkDir(searchDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		// Skip temp files
		if strings.Contains(path, ".tmp.") {
			return nil
		}
		// Get relative key
		relPath, _ := filepath.Rel(s.baseDir, path)
		keys = append(keys, relPath)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("list %s: %w", prefix, err)
	}

	return keys, nil
}

// Verify LocalStore implements AtomicStore.
var _ AtomicStore = (*LocalStore)(nil)
