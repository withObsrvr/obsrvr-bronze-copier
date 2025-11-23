package copier

import (
	"context"
	"fmt"
	"log"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/metadata"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/pas"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/source"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/storage"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/tables"
)

type Copier struct {
	cfg     config.Config
	src     source.LedgerSource
	store   storage.BronzeStore
	meta    metadata.Writer
	pas     pas.Emitter
	builder *tables.PartitionBuilder
}

func New(cfg config.Config, src source.LedgerSource, store storage.BronzeStore) *Copier {
	return &Copier{
		cfg:     cfg,
		src:     src,
		store:   store,
		meta:    metadata.NewWriter(metadata.CatalogConfig(cfg.Catalog)),
		pas:     pas.NewEmitter(pas.PASConfig(cfg.PAS)),
		builder: tables.NewPartitionBuilder(cfg.Era.PartitionSize),
	}
}

func (c *Copier) Run(ctx context.Context) error {
	ledgersCh, errCh := c.src.Stream(ctx, c.cfg.Era.LedgerStart, c.cfg.Era.LedgerEnd)

	for {
		select {
		case <-ctx.Done():
			return c.src.Close()

		case err := <-errCh:
			return err

		case lcm, ok := <-ledgersCh:
			if !ok {
				return nil
			}

			if err := c.builder.Add(lcm); err != nil {
				return err
			}

			if c.builder.Ready() {
				part := c.builder.Flush()
				if err := c.commitPartition(ctx, part); err != nil {
					return err
				}
			}
		}
	}
}

func (c *Copier) commitPartition(ctx context.Context, part tables.Partition) error {
	log.Printf("[partition] era=%s v=%s range=%d-%d",
		c.cfg.Era.EraID, c.cfg.Era.VersionLabel, part.Start, part.End)

	parquets, checksums, rowCounts, err := part.ToParquet()
	if err != nil {
		return err
	}

	for table, bytes := range parquets {
		ref := storage.PartitionRef{
			EraID:        c.cfg.Era.EraID,
			VersionLabel: c.cfg.Era.VersionLabel,
			Table:        table,
			LedgerStart:  part.Start,
			LedgerEnd:    part.End,
		}

		if exists, _ := c.store.Exists(ctx, ref); exists && !c.cfg.Era.AllowOverwrite {
			return fmt.Errorf("partition already exists: %+v", ref)
		}

		if err := c.store.WriteParquet(ctx, ref, bytes); err != nil {
			return err
		}
	}

	if err := c.meta.RecordPartition(ctx, metadata.PartitionRecord{
		EraID:           c.cfg.Era.EraID,
		VersionLabel:    c.cfg.Era.VersionLabel,
		Start:           part.Start,
		End:             part.End,
		Checksums:       checksums,
		RowCounts:       rowCounts,
		ProducerVersion: "bronze-copier@v0.1.0",
	}); err != nil {
		return err
	}

	if c.cfg.PAS.Enabled {
		if err := c.pas.EmitPartition(ctx, pas.Event{
			EraID:        c.cfg.Era.EraID,
			VersionLabel: c.cfg.Era.VersionLabel,
			Start:        part.Start,
			End:          part.End,
			Checksums:    checksums,
		}); err != nil {
			return err
		}
	}

	return nil
}
