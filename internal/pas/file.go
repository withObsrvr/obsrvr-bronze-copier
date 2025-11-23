package pas

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// FileBackup saves PAS events to local files for backup/audit.
type FileBackup struct {
	dir string
}

// NewFileBackup creates a new file backup handler.
func NewFileBackup(dir string) (*FileBackup, error) {
	if dir == "" {
		dir = "./pas-backup"
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create backup dir: %w", err)
	}

	return &FileBackup{dir: dir}, nil
}

// Save writes a PAS event to a local JSON file.
func (f *FileBackup) Save(evt *PASEvent) error {
	// Generate filename: {network}_{era}_{version}_{start}-{end}.json
	filename := fmt.Sprintf("%s_%s_%s_%d-%d.json",
		evt.Partition.Network,
		evt.Partition.EraID,
		evt.Partition.VersionLabel,
		evt.Partition.LedgerStart,
		evt.Partition.LedgerEnd,
	)

	path := filepath.Join(f.dir, filename)

	data, err := json.MarshalIndent(evt, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	log.Printf("[pas] backed up to %s", path)
	return nil
}

// FileOnlyEmitter writes events to files only (no HTTP).
// Used when PAS endpoint is not available.
type FileOnlyEmitter struct {
	chainTracker *ChainTracker
	backup       *FileBackup
}

// NewFileOnlyEmitter creates an emitter that only writes to local files.
func NewFileOnlyEmitter(backupDir string) (*FileOnlyEmitter, error) {
	chainTracker, err := NewChainTracker(backupDir)
	if err != nil {
		return nil, fmt.Errorf("create chain tracker: %w", err)
	}

	backup, err := NewFileBackup(backupDir)
	if err != nil {
		return nil, fmt.Errorf("create file backup: %w", err)
	}

	return &FileOnlyEmitter{
		chainTracker: chainTracker,
		backup:       backup,
	}, nil
}

// Emit writes a PAS event to local file only.
func (e *FileOnlyEmitter) Emit(evt *PASEvent) error {
	chainKey := evt.Partition.ChainKey()

	// Get previous hash for chain
	prevHash, _ := e.chainTracker.GetHead(chainKey)

	// Set chain info
	evt.Chain.PrevEventHash = prevHash
	evt.EventID = GenerateEventID()
	evt.Version = "1.1"
	evt.EventType = "bronze_partition"

	// Compute event hash
	evt.Chain.EventHash = ComputeEventHash(evt)

	log.Printf("[pas] file-only emit for %s range=%d-%d",
		chainKey, evt.Partition.LedgerStart, evt.Partition.LedgerEnd)
	log.Printf("[pas] event_hash=%s", evt.Chain.EventHash)

	// Save to file
	if err := e.backup.Save(evt); err != nil {
		return err
	}

	// Update chain head
	if err := e.chainTracker.SetHead(chainKey, evt.Chain.EventHash); err != nil {
		log.Printf("[pas] warning: failed to update chain head: %v", err)
	}

	return nil
}

// Close releases resources.
func (e *FileOnlyEmitter) Close() error {
	return nil
}
