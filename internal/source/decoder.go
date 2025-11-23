package source

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go/xdr"
)

// Decoder handles XDR decompression and parsing.
type Decoder struct {
	zstdDecoder *zstd.Decoder
}

// NewDecoder creates a new XDR decoder.
func NewDecoder() (*Decoder, error) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}
	return &Decoder{zstdDecoder: dec}, nil
}

// Close releases decoder resources.
func (d *Decoder) Close() {
	if d.zstdDecoder != nil {
		d.zstdDecoder.Close()
	}
}

// DecodeCompressed decodes a zstd-compressed XDR file into LedgerCloseMeta.
func (d *Decoder) DecodeCompressed(compressedData []byte) (*LedgerCloseMeta, error) {
	// Decompress zstd
	rawXDR, err := d.zstdDecoder.DecodeAll(compressedData, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress: %w", err)
	}

	return d.DecodeRaw(rawXDR)
}

// DecodeRaw decodes raw (uncompressed) XDR bytes into LedgerCloseMeta.
func (d *Decoder) DecodeRaw(rawXDR []byte) (*LedgerCloseMeta, error) {
	var lcm xdr.LedgerCloseMeta
	_, err := xdr.Unmarshal(bytes.NewReader(rawXDR), &lcm)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidXDR, err)
	}

	// Extract header information based on version
	ledgerSeq, hash, prevHash, closeTime, protocolVersion, err := extractLedgerInfo(&lcm)
	if err != nil {
		return nil, err
	}

	return &LedgerCloseMeta{
		LedgerSeq:          ledgerSeq,
		Hash:               hash,
		PreviousLedgerHash: prevHash,
		CloseTime:          closeTime,
		ProtocolVersion:    protocolVersion,
		RawXDR:             rawXDR,
	}, nil
}

// DecodeFromReader decodes from a reader (for streaming).
func (d *Decoder) DecodeFromReader(r io.Reader, compressed bool) (*LedgerCloseMeta, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read data: %w", err)
	}

	if compressed {
		return d.DecodeCompressed(data)
	}
	return d.DecodeRaw(data)
}

// extractLedgerInfo extracts common ledger information from LedgerCloseMeta.
func extractLedgerInfo(lcm *xdr.LedgerCloseMeta) (seq uint32, hash, prevHash string, closeTime int64, protocolVersion uint32, err error) {
	switch lcm.V {
	case 0:
		header := lcm.MustV0().LedgerHeader.Header
		seq = uint32(header.LedgerSeq)
		hash = hashToHex(lcm.MustV0().LedgerHeader.Hash)
		prevHash = hashToHex(header.PreviousLedgerHash)
		closeTime = int64(header.ScpValue.CloseTime)
		protocolVersion = uint32(header.LedgerVersion)
	case 1:
		header := lcm.MustV1().LedgerHeader.Header
		seq = uint32(header.LedgerSeq)
		hash = hashToHex(lcm.MustV1().LedgerHeader.Hash)
		prevHash = hashToHex(header.PreviousLedgerHash)
		closeTime = int64(header.ScpValue.CloseTime)
		protocolVersion = uint32(header.LedgerVersion)
	case 2:
		header := lcm.MustV2().LedgerHeader.Header
		seq = uint32(header.LedgerSeq)
		hash = hashToHex(lcm.MustV2().LedgerHeader.Hash)
		prevHash = hashToHex(header.PreviousLedgerHash)
		closeTime = int64(header.ScpValue.CloseTime)
		protocolVersion = uint32(header.LedgerVersion)
	default:
		err = fmt.Errorf("unsupported LedgerCloseMeta version: %d", lcm.V)
	}
	return
}

// hashToHex converts a 32-byte hash to hex string.
func hashToHex(hash xdr.Hash) string {
	return hex.EncodeToString(hash[:])
}

// GetLedgerSequenceFromXDR extracts just the ledger sequence from raw XDR.
// This is useful for validation without full parsing.
func GetLedgerSequenceFromXDR(rawXDR []byte) (uint32, error) {
	var lcm xdr.LedgerCloseMeta
	_, err := xdr.Unmarshal(bytes.NewReader(rawXDR), &lcm)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidXDR, err)
	}

	switch lcm.V {
	case 0:
		return uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq), nil
	case 1:
		return uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq), nil
	case 2:
		return uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq), nil
	default:
		return 0, fmt.Errorf("unsupported LedgerCloseMeta version: %d", lcm.V)
	}
}
