package metadata

import (
	"encoding/json"
	"os"
	"time"
)

type Meta struct {
	Ledger        int64     `json:"ledger"`
	Network       string    `json:"network"`
	Filename      string    `json:"filename"`
	ByteLength    int64     `json:"byte_length"`
	SHA256        string    `json:"sha256"`
	BronzeVersion string    `json:"bronze_version"`
	Range         string    `json:"range"`
	Timestamp     time.Time `json:"timestamp"`
}

func New(seq int64, network, version, filename, ledgerRange, sha string, size int64) *Meta {
	return &Meta{
		Ledger:        seq,
		Network:       network,
		Filename:      filename,
		ByteLength:    size,
		SHA256:        sha,
		BronzeVersion: version,
		Range:         ledgerRange,
		Timestamp:     time.Now().UTC(),
	}
}

func (m *Meta) WriteJSON(path string) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}
