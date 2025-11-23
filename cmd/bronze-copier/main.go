package main

import (
	"log"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
	"github.com/withObsrvr/obsrvr-bronze-copier/internal/watcher"
)

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	w := watcher.New(cfg)
	if err := w.Run(); err != nil {
		log.Fatalf("copier failed: %v", err)
	}
}
