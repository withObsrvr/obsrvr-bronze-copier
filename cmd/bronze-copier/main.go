package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/copier"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/source"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/storage"
)

func main() {
	cfg := config.MustLoad()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		log.Println("[shutdown] signal received")
		cancel()
	}()

	src, err := source.NewLedgerSource(source.SourceConfig(cfg.Source))
	if err != nil {
		log.Fatal(err)
	}

	store, err := storage.NewBronzeStore(storage.StorageConfig(cfg.Storage))
	if err != nil {
		log.Fatal(err)
	}

	c := copier.New(cfg, src, store)

	if err := c.Run(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("[exit] bronze copier stopped cleanly")
	time.Sleep(100 * time.Millisecond)
}
