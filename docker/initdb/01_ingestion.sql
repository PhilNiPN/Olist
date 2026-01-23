-- Create the schema for the olist_dw database
CREATE SCHEMA IF NOT EXISTS ingestion;
CREATE SCHEMA IF NOT EXISTS bronze;

-- 1. runs table: tracks execution of the pipeline
CREATE TABLE IF NOT EXISTS ingestion.runs (
    run_id UUID PRIMARY KEY,
    snapshot_id TEXT NOT NULL, -- Deterministic identifier for the source data
    status TEXT NOT NULL CHECK (status IN ('started', 'success', 'failed')),
    start_time TIMESTAMP DEFAULT NOW(),
    end_time TIMESTAMP
);

-- 2. file manifest: tracks what files are in a snapshot
CREATE TABLE IF NOT EXISTS ingestion.file_manifest ( 
    snapshot_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    file_hash TEXT,
    file_size_bytes BIGINT,
    row_count BIGINT,
    header_def TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (snapshot_id, filename)
);

-- 3. File loads: tracks the load status of each file per run
CREATE TABLE IF NOT EXISTS ingestion.file_loads ( 
    run_id UUID NOT NULL REFERENCES ingestion.runs(run_id),
    filename TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'loaded', 'failed')),
    rows_inserted BIGINT DEFAULT 0,
    message TEXT,
    updated_at TIMESTAMP DEFAULT NOW(), 
    PRIMARY KEY (run_id, filename)
);
