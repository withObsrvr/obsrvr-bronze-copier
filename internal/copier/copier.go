package copier

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/metadata"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/pas"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/util"
)

type Copier struct {
	cfg *config.Config
}

func New(cfg *config.Config) *Copier {
	return &Copier{cfg: cfg}
}

func (c *Copier) CopyFile(src string) error {
	seq := extractLedgerSeq(src)
	ledgerRange := extractRange(src)

	targetDir := filepath.Join(
		c.cfg.BronzeRoot,
		c.cfg.BronzeVersion,
		fmt.Sprintf("network=%s", c.cfg.Network),
		fmt.Sprintf("ledger_range=%s", ledgerRange),
	)

	if err := util.EnsureDir(targetDir); err != nil {
		return err
	}

	dest := filepath.Join(targetDir, fmt.Sprintf("seq=%d.xdr.zst", seq))

	// Copy raw file
	if err := copyRaw(src, dest); err != nil {
		return err
	}

	info, err := os.Stat(dest)
	if err != nil {
		return err
	}

	// Hash
	sha, err := util.SHA256File(dest)
	if err != nil {
		return err
	}

	// Metadata
	meta := metadata.New(seq, c.cfg.Network, c.cfg.BronzeVersion, filepath.Base(dest), ledgerRange, sha, info.Size())
	metaPath := filepath.Join(targetDir, fmt.Sprintf("seq=%d.meta.json", seq))
	if err := meta.WriteJSON(metaPath); err != nil {
		return err
	}

	// PAS
	pasPath := filepath.Join(targetDir, "pas", fmt.Sprintf("%d.pas.json", seq))
	if err := pas.AppendPAS(seq, c.cfg.BronzeVersion, sha, pasPath); err != nil {
		return err
	}

	log.Printf("bronze: wrote %s (sha=%s)", dest, sha)
	return nil
}

func copyRaw(src, dest string) error {
	if err := util.EnsureDir(filepath.Dir(dest)); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func extractLedgerSeq(path string) int64 {
	base := filepath.Base(path)
	clean := strings.TrimSuffix(base, ".xdr.zst")
	if strings.HasPrefix(clean, "seq=") {
		clean = strings.TrimPrefix(clean, "seq=")
	}
	parts := strings.Split(clean, "--")
	seqStr := parts[len(parts)-1]
	out, _ := util.Atoi(seqStr)
	return out
}

func extractRange(path string) string {
	dir := filepath.Base(filepath.Dir(path))
	if idx := strings.LastIndex(dir, "--"); idx >= 0 && idx+2 < len(dir) {
		return dir[idx+2:]
	}
	return "unknown"
}
