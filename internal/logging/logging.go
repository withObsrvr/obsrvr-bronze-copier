// Package logging provides structured logging using slog.
package logging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"os"
	"strings"
)

// Config holds logging configuration.
type Config struct {
	Format string // "json" | "text"
	Level  string // "debug" | "info" | "warn" | "error"
}

// Setup initializes the global slog logger based on configuration.
func Setup(cfg Config) {
	level := parseLevel(cfg.Level)

	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level: level,
	}

	switch strings.ToLower(cfg.Format) {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	default:
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	slog.SetDefault(slog.New(handler))
}

// parseLevel converts a string level to slog.Level.
func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// correlationIDKey is the context key for correlation IDs.
type correlationIDKey struct{}

// WithCorrelationID adds a correlation ID to the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, id)
}

// CorrelationID retrieves the correlation ID from context.
func CorrelationID(ctx context.Context) string {
	if id, ok := ctx.Value(correlationIDKey{}).(string); ok {
		return id
	}
	return ""
}

// GenerateCorrelationID creates a new unique correlation ID.
func GenerateCorrelationID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// PartitionLogger creates a logger with partition context fields.
func PartitionLogger(correlationID, eraID, versionLabel, network string, start, end uint32) *slog.Logger {
	return slog.With(
		"correlation_id", correlationID,
		"era_id", eraID,
		"version_label", versionLabel,
		"network", network,
		"ledger_start", start,
		"ledger_end", end,
	)
}

// WorkerLogger creates a logger with worker context.
func WorkerLogger(workerID int) *slog.Logger {
	return slog.With("worker_id", workerID)
}

// Component returns a logger with a component name.
func Component(name string) *slog.Logger {
	return slog.With("component", name)
}
