-- Create the schema for the olist_dw database
CREATE SCHEMA IF NOT EXISTS ingestion;
CREATE SCHEMA IF NOT EXISTS bronze;

-- 1. runs table: tracks execution of the pipeline
CREATE TABLE IF NOT EXISTS ingestion.runs (
    run_id UUID PRIMARY KEY,
    snapshot_id TEXT NOT NULL, -- Deterministic identifier for the source data
    layer TEXT NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
    status TEXT NOT NULL CHECK (status IN ('started', 'success', 'failed')),
    start_time TIMESTAMPTZ DEFAULT NOW(),
    end_time TIMESTAMPTZ,
    error_message TEXT
);
CREATE INDEX idx_runs_snapshot_id ON ingestion.runs(snapshot_id);

-- 2. file manifest: tracks what files are in a snapshot
CREATE TABLE IF NOT EXISTS ingestion.file_manifest ( 
    snapshot_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    file_hash TEXT,
    file_size_bytes BIGINT,
    row_count BIGINT,
    header_def TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (snapshot_id, filename)
);
CREATE INDEX idx_file_manifest_filename ON ingestion.file_manifest(filename, created_at DESC);

-- 3. File loads: tracks the load status of each file per run
CREATE TABLE IF NOT EXISTS ingestion.file_loads ( 
    run_id UUID NOT NULL REFERENCES ingestion.runs(run_id),
    filename TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'loaded', 'failed')),
    rows_inserted BIGINT DEFAULT 0,
    message TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW(), 
    PRIMARY KEY (run_id, filename)
);

-- 4. Quality checks: tracks data quality results pr. table pr. run
CREATE TABLE IF NOT EXISTS ingestion.quality_checks (
    run_id UUID NOT NULL REFERENCES ingestion.runs(run_id),
    table_name TEXT NOT NULL,
    check_name TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    details JSONB,
    checked_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (run_id, table_name, check_name)
);


-- Comments for 1. runs table:
COMMENT ON TABLE ingestion.runs IS 'Tracks each execution of the bronze ingestion pipeline.';
COMMENT ON COLUMN ingestion.runs.run_id IS 'Unique identifier for this pipeline run.';
COMMENT ON COLUMN ingestion.runs.snapshot_id IS 'Deterministic hash of the source data and links to file_manifest.';
COMMENT ON COLUMN ingestion.runs.status IS 'Pipeline state: started, success, failed.';
COMMENT ON COLUMN ingestion.runs.error_message IS 'Error details when pipeline fails, NULL for success.';

-- Comments for 2. file_manifest table:
COMMENT ON TABLE ingestion.file_manifest IS 'Tracks source file metadata pr. snapshot for change detection.';
COMMENT ON COLUMN ingestion.file_manifest.snapshot_id IS 'Deterministic hash of the source data.';
COMMENT ON COLUMN ingestion.file_manifest.file_hash IS 'SHA256 hash of the source file, used for change detection.';
COMMENT ON COLUMN ingestion.file_manifest.header_def IS 'CSV column headers, used for schema drift detection.';

-- Comments for 3. file_loads table:
COMMENT ON TABLE ingestion.file_loads IS 'Tracks the load status of each file within a pipeline run.';
COMMENT ON COLUMN ingestion.file_loads.message IS 'Error details for failed loads.';

-- Comments for 4. quality_checks table:
COMMENT ON TABLE ingestion.quality_checks IS 'Bronze layer data quality check results pr. table pr. run.';
COMMENT ON COLUMN ingestion.quality_checks.details IS ' JSON object with check-specific metrics (counts, rates, column lists).';