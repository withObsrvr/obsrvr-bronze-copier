package pas

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	// ErrNoChainHead indicates no previous event exists for this chain.
	ErrNoChainHead = errors.New("no chain head found")
)

// ComputeEventHash computes the SHA256 hash of a PAS event.
// The hash is computed over the canonical JSON representation,
// excluding the event_hash field itself.
func ComputeEventHash(evt *PASEvent) string {
	// Create a copy without event_hash
	evtCopy := *evt
	evtCopy.Chain.EventHash = ""

	// Marshal to canonical JSON (Go's json.Marshal produces sorted keys)
	canonical, err := json.Marshal(evtCopy)
	if err != nil {
		// Should never happen with well-formed events
		return ""
	}

	hash := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(hash[:])
}

// ChainTracker manages the chain heads for PAS event linking.
type ChainTracker struct {
	mu       sync.RWMutex
	heads    map[string]string // chainKey -> eventHash
	filePath string
}

// NewChainTracker creates a chain tracker that persists to the given directory.
func NewChainTracker(dir string) (*ChainTracker, error) {
	if dir == "" {
		dir = "./state"
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create chain tracker dir: %w", err)
	}

	ct := &ChainTracker{
		heads:    make(map[string]string),
		filePath: filepath.Join(dir, "pas-chain-heads.json"),
	}

	// Load existing chain heads
	if err := ct.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load chain heads: %w", err)
	}

	return ct, nil
}

// GetHead returns the last event hash for a chain.
func (ct *ChainTracker) GetHead(chainKey string) (string, error) {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	hash, ok := ct.heads[chainKey]
	if !ok || hash == "" {
		return "", ErrNoChainHead
	}
	return hash, nil
}

// SetHead updates the chain head after a successful event emission.
func (ct *ChainTracker) SetHead(chainKey, eventHash string) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	ct.heads[chainKey] = eventHash

	// Persist to file
	return ct.save()
}

// load reads chain heads from the JSON file.
func (ct *ChainTracker) load() error {
	data, err := os.ReadFile(ct.filePath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &ct.heads)
}

// save writes chain heads to the JSON file.
func (ct *ChainTracker) save() error {
	data, err := json.MarshalIndent(ct.heads, "", "  ")
	if err != nil {
		return err
	}

	// Write atomically using temp file
	tmpPath := ct.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, ct.filePath)
}

// GenerateEventID creates a unique event ID.
func GenerateEventID() string {
	// Use timestamp + random bytes for uniqueness
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	return "pas_evt_" + hex.EncodeToString(hash[:8])
}
