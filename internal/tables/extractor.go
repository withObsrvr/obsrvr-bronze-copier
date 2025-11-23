package tables

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/source"
)

// Extractor converts LedgerCloseMeta into table rows.
type Extractor struct {
	cfg ParquetConfig
}

// NewExtractor creates a new row extractor.
func NewExtractor(cfg ParquetConfig) *Extractor {
	return &Extractor{cfg: cfg}
}

// ExtractLedgersLCMRaw extracts a LedgersLCMRawRow from LedgerCloseMeta.
func (e *Extractor) ExtractLedgersLCMRaw(lcm *source.LedgerCloseMeta, ingestedAt time.Time) (*LedgersLCMRawRow, error) {
	// Parse XDR to extract additional fields
	var xdrLcm xdr.LedgerCloseMeta
	_, err := xdr.Unmarshal(bytes.NewReader(lcm.RawXDR), &xdrLcm)
	if err != nil {
		return nil, fmt.Errorf("unmarshal XDR: %w", err)
	}

	// Extract fields based on version
	row, err := e.extractFromXDR(&xdrLcm, lcm.RawXDR, ingestedAt)
	if err != nil {
		return nil, err
	}

	// Override with pre-extracted values from source (already validated)
	row.LedgerSequence = uint64(lcm.LedgerSeq)
	row.LedgerHash = lcm.Hash
	row.PreviousLedgerHash = lcm.PreviousLedgerHash
	row.ProtocolVersion = lcm.ProtocolVersion
	row.CloseTime = time.Unix(lcm.CloseTime, 0).UTC()

	// Set Bronze metadata
	row.BronzeVersion = e.cfg.BronzeVersion
	row.EraID = e.cfg.EraID
	row.Network = e.cfg.Network

	return row, nil
}

// extractFromXDR extracts detailed fields from the XDR structure.
func (e *Extractor) extractFromXDR(lcm *xdr.LedgerCloseMeta, rawXDR []byte, ingestedAt time.Time) (*LedgersLCMRawRow, error) {
	row := &LedgersLCMRawRow{
		XDRBytes:      rawXDR,
		XDRByteLength: int32(len(rawXDR)),
		XDRSHA256:     computeSHA256(rawXDR),
		IngestedAt:    ingestedAt,
	}

	switch lcm.V {
	case 0:
		v0 := lcm.MustV0()
		if err := e.extractV0(&v0, row); err != nil {
			return nil, err
		}
	case 1:
		v1 := lcm.MustV1()
		if err := e.extractV1(&v1, row); err != nil {
			return nil, err
		}
	case 2:
		v2 := lcm.MustV2()
		if err := e.extractV2(&v2, row); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported LedgerCloseMeta version: %d", lcm.V)
	}

	return row, nil
}

func (e *Extractor) extractV0(v0 *xdr.LedgerCloseMetaV0, row *LedgersLCMRawRow) error {
	header := v0.LedgerHeader.Header

	row.TotalCoins = int64(header.TotalCoins)
	row.FeePool = int64(header.FeePool)
	row.TxSetHash = hashToHex(header.ScpValue.TxSetHash)

	// Count transactions and operations from V0 TxSet
	row.TxCount = int32(len(v0.TxSet.Txs))
	var opCount int32
	for _, tx := range v0.TxSet.Txs {
		ops := tx.Operations()
		opCount += int32(len(ops))
	}
	row.OperationCount = opCount

	return nil
}

func (e *Extractor) extractV1(v1 *xdr.LedgerCloseMetaV1, row *LedgersLCMRawRow) error {
	header := v1.LedgerHeader.Header

	row.TotalCoins = int64(header.TotalCoins)
	row.FeePool = int64(header.FeePool)
	row.TxSetHash = hashToHex(header.ScpValue.TxSetHash)

	// Count from TxProcessing for V1
	row.TxCount = int32(len(v1.TxProcessing))
	var opCount int32
	for _, txp := range v1.TxProcessing {
		// Count operations from the results
		results := txp.Result.Result.Result.Results
		if results != nil {
			opCount += int32(len(*results))
		}
	}
	row.OperationCount = opCount

	return nil
}

func (e *Extractor) extractV2(v2 *xdr.LedgerCloseMetaV2, row *LedgersLCMRawRow) error {
	header := v2.LedgerHeader.Header

	row.TotalCoins = int64(header.TotalCoins)
	row.FeePool = int64(header.FeePool)
	row.TxSetHash = hashToHex(header.ScpValue.TxSetHash)

	// Count from TxProcessing for V2
	row.TxCount = int32(len(v2.TxProcessing))
	var opCount int32
	for _, txp := range v2.TxProcessing {
		// Count operations from the results
		results := txp.Result.Result.Result.Results
		if results != nil {
			opCount += int32(len(*results))
		}
	}
	row.OperationCount = opCount

	return nil
}

// computeSHA256 computes SHA256 hash and returns hex string.
func computeSHA256(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// hashToHex converts xdr.Hash to hex string.
func hashToHex(hash xdr.Hash) string {
	return hex.EncodeToString(hash[:])
}
