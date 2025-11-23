package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var (
	// ErrNoCheckpoint is returned when no checkpoint exists.
	ErrNoCheckpoint = errors.New("no checkpoint found")
)

// Checkpoint represents the copier's progress state.
type Checkpoint struct {
	CopierID            string         `json:"copier_id"`
	Network             string         `json:"network"`
	EraID               string         `json:"era_id"`
	VersionLabel        string         `json:"version_label"`
	LastCommittedLedger uint32         `json:"last_committed_ledger"`
	LastPartition       *PartitionInfo `json:"last_committed_partition,omitempty"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

// PartitionInfo describes the last committed partition.
type PartitionInfo struct {
	Start    uint32 `json:"start"`
	End      uint32 `json:"end"`
	Checksum string `json:"checksum,omitempty"`
}

// Manager handles checkpoint persistence and retrieval.
type Manager interface {
	// Load reads the current checkpoint.
	Load(ctx context.Context) (*Checkpoint, error)

	// Save persists the checkpoint.
	Save(ctx context.Context, cp *Checkpoint) error
}

// Config configures the checkpoint manager.
type Config struct {
	Enabled bool
	Dir     string // Directory for checkpoint files
}

// NewManager creates a checkpoint manager based on configuration.
func NewManager(cfg Config) (Manager, error) {
	if !cfg.Enabled {
		return &noopManager{}, nil
	}

	// Ensure checkpoint directory exists
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("create checkpoint directory %s: %w", cfg.Dir, err)
	}

	return &fileManager{dir: cfg.Dir}, nil
}

// fileManager persists checkpoints to local files.
type fileManager struct {
	dir string
}

// checkpointPath returns the path to the checkpoint file for a given context.
func (m *fileManager) checkpointPath(network, eraID, version string) string {
	filename := fmt.Sprintf("checkpoint_%s_%s_%s.json", network, eraID, version)
	return filepath.Join(m.dir, filename)
}

// Load reads the checkpoint from file.
func (m *fileManager) Load(ctx context.Context) (*Checkpoint, error) {
	// Try to find any checkpoint file in the directory
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoCheckpoint
		}
		return nil, fmt.Errorf("read checkpoint directory: %w", err)
	}

	// Look for checkpoint files
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		if len(entry.Name()) < 11 || entry.Name()[:11] != "checkpoint_" {
			continue
		}

		// Found a checkpoint file, load it
		path := filepath.Join(m.dir, entry.Name())
		return m.loadFromPath(path)
	}

	return nil, ErrNoCheckpoint
}

// loadFromPath reads a checkpoint from a specific file.
func (m *fileManager) loadFromPath(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoCheckpoint
		}
		return nil, fmt.Errorf("read checkpoint file: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("parse checkpoint file: %w", err)
	}

	return &cp, nil
}

// Save persists the checkpoint to file.
func (m *fileManager) Save(ctx context.Context, cp *Checkpoint) error {
	path := m.checkpointPath(cp.Network, cp.EraID, cp.VersionLabel)

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	// Write atomically
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("write checkpoint temp file: %w", err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("rename checkpoint file: %w", err)
	}

	return nil
}

// LoadForConfig loads a checkpoint matching the given config.
func (m *fileManager) LoadForConfig(ctx context.Context, network, eraID, version string) (*Checkpoint, error) {
	path := m.checkpointPath(network, eraID, version)
	return m.loadFromPath(path)
}

// noopManager is a no-op checkpoint manager for when checkpointing is disabled.
type noopManager struct{}

func (m *noopManager) Load(ctx context.Context) (*Checkpoint, error) {
	return nil, ErrNoCheckpoint
}

func (m *noopManager) Save(ctx context.Context, cp *Checkpoint) error {
	return nil
}
