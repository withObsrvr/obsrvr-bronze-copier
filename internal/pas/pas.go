package pas

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type PASEntry struct {
	Ledger        int64     `json:"ledger"`
	BronzeVersion string    `json:"bronze_version"`
	Hash          string    `json:"hash"`
	PrevHash      string    `json:"prev_hash"`
	CreatedAt     time.Time `json:"created_at"`
}

var (
	lastHashes   = map[string]string{}
	lastHashesMu sync.Mutex
)

func AppendPAS(seq int64, version, hash, path string) error {
	lastHashesMu.Lock()
	prev := lastHashes[version]
	lastHashes[version] = hash
	lastHashesMu.Unlock()

	entry := &PASEntry{
		Ledger:        seq,
		BronzeVersion: version,
		Hash:          "sha256:" + hash,
		PrevHash:      prev,
		CreatedAt:     time.Now().UTC(),
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}
