package pas

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/withObsrvr/obsrvr-bronze-copier/internal/config"
)

// HTTPEmitter sends PAS events to an HTTP endpoint.
type HTTPEmitter struct {
	cfg          config.PASConfig
	client       *http.Client
	chainTracker *ChainTracker
	backup       *FileBackup
}

// NewHTTPEmitter creates a new HTTP emitter.
func NewHTTPEmitter(cfg config.PASConfig) (*HTTPEmitter, error) {
	// Create chain tracker
	chainTracker, err := NewChainTracker(cfg.BackupDir)
	if err != nil {
		return nil, fmt.Errorf("create chain tracker: %w", err)
	}

	// Create file backup
	backup, err := NewFileBackup(cfg.BackupDir)
	if err != nil {
		return nil, fmt.Errorf("create file backup: %w", err)
	}

	return &HTTPEmitter{
		cfg: cfg,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		chainTracker: chainTracker,
		backup:       backup,
	}, nil
}

// Emit sends a PAS event to the configured endpoint.
func (e *HTTPEmitter) Emit(ctx context.Context, evt *PASEvent) error {
	chainKey := evt.Partition.ChainKey()

	// 1. Get previous hash for chain
	prevHash, err := e.chainTracker.GetHead(chainKey)
	if err != nil && !errors.Is(err, ErrNoChainHead) {
		return fmt.Errorf("get chain head: %w", err)
	}

	// 2. Set chain info and generate event ID
	evt.Chain.PrevEventHash = prevHash
	evt.EventID = GenerateEventID()
	evt.Timestamp = time.Now().UTC()
	evt.Version = "1.1"
	evt.EventType = "bronze_partition"

	// 3. Compute event hash
	evt.Chain.EventHash = ComputeEventHash(evt)

	log.Printf("[pas] emitting event for %s range=%d-%d",
		chainKey, evt.Partition.LedgerStart, evt.Partition.LedgerEnd)
	if prevHash == "" {
		log.Printf("[pas] prev_hash=null (first in chain)")
	} else {
		log.Printf("[pas] prev_hash=%s", prevHash)
	}
	log.Printf("[pas] event_hash=%s", evt.Chain.EventHash)

	// 4. Backup to local file (always, before HTTP)
	if err := e.backup.Save(evt); err != nil {
		log.Printf("[pas] warning: backup failed: %v", err)
		// Continue anyway - HTTP POST is the primary path
	}

	// 5. POST to PAS endpoint (with retry)
	if err := e.postWithRetry(ctx, evt); err != nil {
		return fmt.Errorf("pas emit failed: %w", err)
	}

	// 6. Update chain head
	if err := e.chainTracker.SetHead(chainKey, evt.Chain.EventHash); err != nil {
		log.Printf("[pas] warning: failed to update chain head: %v", err)
	}

	return nil
}

// postWithRetry sends the event to the PAS endpoint with retries.
func (e *HTTPEmitter) postWithRetry(ctx context.Context, evt *PASEvent) error {
	var lastErr error
	retries := 3
	delay := time.Second

	for attempt := 1; attempt <= retries; attempt++ {
		err := e.post(ctx, evt)
		if err == nil {
			return nil
		}

		lastErr = err
		if attempt < retries {
			log.Printf("[pas] attempt %d/%d failed: %v, retrying in %v", attempt, retries, err, delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2 // Exponential backoff
		}
	}

	return fmt.Errorf("all %d attempts failed: %w", retries, lastErr)
}

// post sends a single POST request to the PAS endpoint.
func (e *HTTPEmitter) post(ctx context.Context, evt *PASEvent) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", e.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("[pas] POST %s -> %d %s", e.cfg.Endpoint, resp.StatusCode, resp.Status)
		return nil
	}

	// Read error body
	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("http %d: %s", resp.StatusCode, string(respBody))
}

// Close releases resources.
func (e *HTTPEmitter) Close() error {
	return nil
}
