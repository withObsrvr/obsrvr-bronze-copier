-- Bronze Copier Catalog Schema
-- These tables track lineage and metadata for Bronze layer data

-- Dataset registry: tracks all Bronze datasets (tables x versions)
CREATE TABLE IF NOT EXISTS _meta_datasets (
    id              SERIAL PRIMARY KEY,
    domain          VARCHAR(64) NOT NULL,     -- "bronze"
    dataset         VARCHAR(128) NOT NULL,    -- "ledgers_lcm_raw"
    version         VARCHAR(32) NOT NULL,     -- "v1"
    era_id          VARCHAR(64) NOT NULL,     -- "pre_p23"
    network         VARCHAR(32) NOT NULL,     -- "pubnet", "testnet"
    schema_hash     VARCHAR(64),              -- hash of parquet schema
    description     TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(domain, dataset, version, era_id, network)
);

-- Lineage records: tracks every partition written
CREATE TABLE IF NOT EXISTS _meta_lineage (
    id                  SERIAL PRIMARY KEY,
    dataset_id          INTEGER NOT NULL REFERENCES _meta_datasets(id),
    ledger_start        BIGINT NOT NULL,
    ledger_end          BIGINT NOT NULL,
    row_count           BIGINT NOT NULL,
    byte_size           BIGINT NOT NULL,
    checksum            VARCHAR(128) NOT NULL,   -- sha256 of parquet file
    storage_path        TEXT NOT NULL,           -- full path to parquet
    producer_version    VARCHAR(64) NOT NULL,    -- "bronze-copier@v0.1.0"
    producer_git_sha    VARCHAR(64),
    source_type         VARCHAR(32),             -- "archive" | "live"
    source_location     TEXT,                    -- gs://bucket/path
    created_at          TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(dataset_id, ledger_start, ledger_end)
);

-- Index for efficient range queries
CREATE INDEX IF NOT EXISTS idx_lineage_dataset_range
    ON _meta_lineage(dataset_id, ledger_start, ledger_end);

-- Index for finding gaps in coverage
CREATE INDEX IF NOT EXISTS idx_lineage_ledger_end
    ON _meta_lineage(dataset_id, ledger_end);

-- Quality metrics per partition (for future use)
CREATE TABLE IF NOT EXISTS _meta_quality (
    id              SERIAL PRIMARY KEY,
    lineage_id      INTEGER NOT NULL REFERENCES _meta_lineage(id),
    metric_name     VARCHAR(64) NOT NULL,
    metric_value    DOUBLE PRECISION,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(lineage_id, metric_name)
);

-- Schema evolution tracking
CREATE TABLE IF NOT EXISTS _meta_changes (
    id              SERIAL PRIMARY KEY,
    dataset_id      INTEGER NOT NULL REFERENCES _meta_datasets(id),
    change_type     VARCHAR(32) NOT NULL,    -- "schema_change" | "correction" | "backfill"
    description     TEXT,
    old_version     VARCHAR(32),
    new_version     VARCHAR(32),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Helper view for coverage queries
CREATE OR REPLACE VIEW _meta_coverage AS
SELECT
    d.domain,
    d.dataset,
    d.version,
    d.era_id,
    d.network,
    MIN(l.ledger_start) as min_ledger,
    MAX(l.ledger_end) as max_ledger,
    COUNT(*) as partition_count,
    SUM(l.row_count) as total_rows,
    SUM(l.byte_size) as total_bytes
FROM _meta_datasets d
JOIN _meta_lineage l ON d.id = l.dataset_id
GROUP BY d.domain, d.dataset, d.version, d.era_id, d.network;
