package metadata

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// PostgresWriter implements Writer using PostgreSQL.
type PostgresWriter struct {
	pool      *pgxpool.Pool
	cfg       CatalogConfig
	mu        sync.RWMutex
	datasetCache map[string]int64 // cache dataset IDs
}

// NewPostgresWriter creates a new PostgreSQL catalog writer.
func NewPostgresWriter(cfg CatalogConfig) (*PostgresWriter, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}

	// Configure connection pool
	poolCfg.MaxConns = 5
	poolCfg.MinConns = 1
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	w := &PostgresWriter{
		pool:         pool,
		cfg:          cfg,
		datasetCache: make(map[string]int64),
	}

	// Initialize schema
	if err := w.initSchema(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	log.Println("[metadata] connected to PostgreSQL catalog")
	return w, nil
}

// initSchema creates the _meta_* tables if they don't exist.
func (w *PostgresWriter) initSchema(ctx context.Context) error {
	_, err := w.pool.Exec(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("execute schema: %w", err)
	}
	return nil
}

// EnsureDataset registers or retrieves a dataset entry.
func (w *PostgresWriter) EnsureDataset(ctx context.Context, info DatasetInfo) (int64, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("%s.%s.%s.%s.%s", info.Domain, info.Dataset, info.Version, info.EraID, info.Network)
	w.mu.RLock()
	if id, ok := w.datasetCache[cacheKey]; ok {
		w.mu.RUnlock()
		return id, nil
	}
	w.mu.RUnlock()

	// Try to insert, or get existing
	query := `
		INSERT INTO _meta_datasets (domain, dataset, version, era_id, network, schema_hash, description)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (domain, dataset, version, era_id, network)
		DO UPDATE SET updated_at = NOW()
		RETURNING id
	`

	var id int64
	err := w.pool.QueryRow(ctx, query,
		info.Domain,
		info.Dataset,
		info.Version,
		info.EraID,
		info.Network,
		info.SchemaHash,
		info.Description,
	).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("ensure dataset: %w", err)
	}

	// Cache the ID
	w.mu.Lock()
	w.datasetCache[cacheKey] = id
	w.mu.Unlock()

	return id, nil
}

// RecordPartition writes a lineage record for a committed partition.
func (w *PostgresWriter) RecordPartition(ctx context.Context, rec PartitionRecord) error {
	// Get dataset ID if not provided
	datasetID := rec.DatasetID
	if datasetID == 0 {
		// Need to look up or create the dataset first
		return fmt.Errorf("DatasetID is required (call EnsureDataset first)")
	}

	// Calculate total row count and byte size from maps
	var totalRowCount int64
	for _, count := range rec.RowCounts {
		totalRowCount += count
	}

	// Get primary checksum (from ledgers_lcm_raw table)
	checksum := rec.Checksums["ledgers_lcm_raw"]
	if checksum == "" {
		// Fallback to first available checksum
		for _, cs := range rec.Checksums {
			checksum = cs
			break
		}
	}

	query := `
		INSERT INTO _meta_lineage (
			dataset_id, ledger_start, ledger_end, row_count, byte_size,
			checksum, storage_path, producer_version, producer_git_sha,
			source_type, source_location
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (dataset_id, ledger_start, ledger_end)
		DO UPDATE SET
			row_count = EXCLUDED.row_count,
			byte_size = EXCLUDED.byte_size,
			checksum = EXCLUDED.checksum,
			created_at = NOW()
	`

	_, err := w.pool.Exec(ctx, query,
		datasetID,
		int64(rec.Start),
		int64(rec.End),
		totalRowCount,
		rec.ByteSize,
		checksum,
		rec.StoragePath,
		rec.ProducerVersion,
		rec.ProducerGitSHA,
		rec.SourceType,
		rec.SourceLocation,
	)

	if err != nil {
		return fmt.Errorf("record partition: %w", err)
	}

	log.Printf("[metadata] recorded lineage for range %d-%d", rec.Start, rec.End)
	return nil
}

// PartitionExists checks if a partition has already been committed.
func (w *PostgresWriter) PartitionExists(ctx context.Context, datasetID int64, start, end uint32) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1 FROM _meta_lineage
			WHERE dataset_id = $1 AND ledger_start = $2 AND ledger_end = $3
		)
	`

	var exists bool
	err := w.pool.QueryRow(ctx, query, datasetID, int64(start), int64(end)).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check partition exists: %w", err)
	}
	return exists, nil
}

// GetLastCommittedLedger returns the highest committed ledger for a dataset.
func (w *PostgresWriter) GetLastCommittedLedger(ctx context.Context, datasetID int64) (uint32, error) {
	query := `
		SELECT COALESCE(MAX(ledger_end), 0)
		FROM _meta_lineage
		WHERE dataset_id = $1
	`

	var maxLedger int64
	err := w.pool.QueryRow(ctx, query, datasetID).Scan(&maxLedger)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("get latest ledger: %w", err)
	}

	return uint32(maxLedger), nil
}

// GetLastChecksum returns the checksum of the most recent partition.
// Used for PAS hash chaining.
func (w *PostgresWriter) GetLastChecksum(ctx context.Context, datasetID int64) (string, error) {
	query := `
		SELECT checksum FROM _meta_lineage
		WHERE dataset_id = $1
		ORDER BY ledger_end DESC
		LIMIT 1
	`

	var checksum string
	err := w.pool.QueryRow(ctx, query, datasetID).Scan(&checksum)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", nil // No previous partition
		}
		return "", fmt.Errorf("get last checksum: %w", err)
	}
	return checksum, nil
}

// GetCoverageGaps finds gaps in the lineage for a dataset.
// Returns pairs of (missing_start, missing_end) ranges.
func (w *PostgresWriter) GetCoverageGaps(ctx context.Context, datasetID int64, fromLedger, toLedger uint32) ([][2]uint32, error) {
	query := `
		WITH ranges AS (
			SELECT ledger_start, ledger_end
			FROM _meta_lineage
			WHERE dataset_id = $1
			  AND ledger_start >= $2
			  AND ledger_end <= $3
			ORDER BY ledger_start
		),
		gaps AS (
			SELECT
				LAG(ledger_end) OVER (ORDER BY ledger_start) + 1 AS gap_start,
				ledger_start - 1 AS gap_end
			FROM ranges
		)
		SELECT gap_start, gap_end
		FROM gaps
		WHERE gap_start <= gap_end
	`

	rows, err := w.pool.Query(ctx, query, datasetID, int64(fromLedger), int64(toLedger))
	if err != nil {
		return nil, fmt.Errorf("query gaps: %w", err)
	}
	defer rows.Close()

	var gaps [][2]uint32
	for rows.Next() {
		var start, end int64
		if err := rows.Scan(&start, &end); err != nil {
			return nil, fmt.Errorf("scan gap: %w", err)
		}
		gaps = append(gaps, [2]uint32{uint32(start), uint32(end)})
	}

	return gaps, rows.Err()
}

// GetLastLineage returns the most recent lineage record for hash chaining.
func (w *PostgresWriter) GetLastLineage(ctx context.Context, datasetID int64) (*LineageRecord, error) {
	query := `
		SELECT ledger_start, ledger_end, row_count, byte_size, checksum,
		       COALESCE(prev_hash, ''), storage_path, COALESCE(storage_uri, ''),
		       producer_version, COALESCE(producer_git_sha, ''),
		       COALESCE(source_type, ''), COALESCE(source_location, '')
		FROM _meta_lineage
		WHERE dataset_id = $1
		ORDER BY ledger_end DESC
		LIMIT 1
	`

	var rec LineageRecord
	rec.DatasetID = datasetID

	err := w.pool.QueryRow(ctx, query, datasetID).Scan(
		&rec.LedgerStart, &rec.LedgerEnd, &rec.RowCount, &rec.ByteSize,
		&rec.Checksum, &rec.PrevHash, &rec.StoragePath, &rec.StorageURI,
		&rec.ProducerVersion, &rec.ProducerGitSHA,
		&rec.SourceType, &rec.SourceLocation,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // No previous lineage
		}
		return nil, fmt.Errorf("get last lineage: %w", err)
	}
	return &rec, nil
}

// InsertLineage records a lineage entry with prev_hash chain.
func (w *PostgresWriter) InsertLineage(ctx context.Context, rec LineageRecord) error {
	query := `
		INSERT INTO _meta_lineage (
			dataset_id, ledger_start, ledger_end, row_count, byte_size,
			checksum, prev_hash, storage_path, storage_uri,
			producer_version, producer_git_sha, source_type, source_location
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (dataset_id, ledger_start, ledger_end)
		DO UPDATE SET
			row_count = EXCLUDED.row_count,
			byte_size = EXCLUDED.byte_size,
			checksum = EXCLUDED.checksum,
			prev_hash = EXCLUDED.prev_hash,
			storage_uri = EXCLUDED.storage_uri,
			created_at = NOW()
	`

	var prevHash *string
	if rec.PrevHash != "" {
		prevHash = &rec.PrevHash
	}

	var storageURI *string
	if rec.StorageURI != "" {
		storageURI = &rec.StorageURI
	}

	_, err := w.pool.Exec(ctx, query,
		rec.DatasetID,
		int64(rec.LedgerStart),
		int64(rec.LedgerEnd),
		rec.RowCount,
		rec.ByteSize,
		rec.Checksum,
		prevHash,
		rec.StoragePath,
		storageURI,
		rec.ProducerVersion,
		rec.ProducerGitSHA,
		rec.SourceType,
		rec.SourceLocation,
	)
	if err != nil {
		return fmt.Errorf("insert lineage: %w", err)
	}

	log.Printf("[metadata] recorded lineage for range %d-%d (prev_hash=%s)",
		rec.LedgerStart, rec.LedgerEnd, rec.PrevHash)
	return nil
}

// InsertQuality records a quality validation result.
func (w *PostgresWriter) InsertQuality(ctx context.Context, rec QualityRecord) error {
	query := `
		INSERT INTO _meta_quality (dataset_id, ledger_start, ledger_end, passed, error_message)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (dataset_id, ledger_start, ledger_end)
		DO UPDATE SET
			passed = EXCLUDED.passed,
			error_message = EXCLUDED.error_message,
			created_at = NOW()
	`

	var errMsg *string
	if rec.ErrorMessage != "" {
		errMsg = &rec.ErrorMessage
	}

	_, err := w.pool.Exec(ctx, query,
		rec.DatasetID,
		int64(rec.LedgerStart),
		int64(rec.LedgerEnd),
		rec.Passed,
		errMsg,
	)
	if err != nil {
		return fmt.Errorf("insert quality: %w", err)
	}
	return nil
}

// Close releases database connections.
func (w *PostgresWriter) Close() error {
	w.pool.Close()
	return nil
}
