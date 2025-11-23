package tables

import (
	"crypto/sha256"
	"encoding/hex"
)

// ComputeChecksum computes a SHA256 checksum for the given data.
func ComputeChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(hash[:])
}

// VerifyChecksum verifies that data matches the expected checksum.
func VerifyChecksum(data []byte, expected string) bool {
	actual := ComputeChecksum(data)
	return actual == expected
}
