DO $$ BEGIN
    CREATE TYPE enrichment_status_enum AS ENUM (
        'minimal',
        'pending',
        'enriching',
        'enriched',
        'failed',
        'skipped'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE enrichment_stage_enum AS ENUM (
        'api_metadata',
        'embedding',
        'static_score',
        'link_validation'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE enrichment_result_enum AS ENUM (
        'success',
        'failed',
        'rate_limited',
        'skipped'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS datasets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_name VARCHAR(50) NOT NULL,
    external_id VARCHAR(255) NOT NULL,

    title TEXT NOT NULL,
    url VARCHAR(512) NOT NULL,

    description TEXT,
    tags TEXT[],
    license VARCHAR(100),
    file_formats TEXT[],
    total_size_bytes BIGINT,

    column_names TEXT[],
    row_count BIGINT,

    download_count BIGINT DEFAULT 0 NOT NULL,
    view_count BIGINT DEFAULT 0 NOT NULL,
    like_count BIGINT DEFAULT 0 NOT NULL,

    source_created_at TIMESTAMPTZ,
    source_updated_at TIMESTAMPTZ,

    embedding VECTOR(384),
    static_score FLOAT,

    is_active BOOLEAN DEFAULT true NOT NULL,
    enrichment_status enrichment_status_enum DEFAULT 'minimal' NOT NULL,
    enrichment_attempts INTEGER DEFAULT 0 NOT NULL,
    last_enrichment_error TEXT,
    last_enriched_at TIMESTAMPTZ,
    last_checked_at TIMESTAMPTZ,

    source_meta JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,

    CONSTRAINT unique_external_dataset UNIQUE (source_name, external_id)
);

CREATE INDEX IF NOT EXISTS idx_datasets_source_name ON datasets(source_name);
CREATE INDEX IF NOT EXISTS idx_datasets_is_active ON datasets(is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_datasets_enrichment_status ON datasets(enrichment_status);
CREATE INDEX IF NOT EXISTS idx_datasets_source_status ON datasets(source_name, enrichment_status);
CREATE INDEX IF NOT EXISTS idx_datasets_source_updated ON datasets(source_updated_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_datasets_created_at ON datasets(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_datasets_embedding ON datasets USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_datasets_title_gin ON datasets USING gin(to_tsvector('english', title));
CREATE INDEX IF NOT EXISTS idx_datasets_tags_gin ON datasets USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_datasets_column_names_gin ON datasets USING gin(column_names);

CREATE TABLE IF NOT EXISTS dataset_enrichment_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,

    stage enrichment_stage_enum NOT NULL,
    result enrichment_result_enum NOT NULL,
    attempt_number INTEGER NOT NULL,

    error_message TEXT,
    error_type VARCHAR(100),

    duration_ms INTEGER,

    worker_id VARCHAR(100),
    task_id VARCHAR(100),

    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_enrichment_logs_dataset_id ON dataset_enrichment_logs(dataset_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_logs_dataset_stage ON dataset_enrichment_logs(dataset_id, stage);
CREATE INDEX IF NOT EXISTS idx_enrichment_logs_result ON dataset_enrichment_logs(result);
CREATE INDEX IF NOT EXISTS idx_enrichment_logs_created_at ON dataset_enrichment_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_enrichment_logs_stage_result ON dataset_enrichment_logs(stage, result);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_datasets_updated_at ON datasets;
CREATE TRIGGER update_datasets_updated_at
    BEFORE UPDATE ON datasets
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_enrichment_logs_updated_at ON dataset_enrichment_logs;
CREATE TRIGGER update_enrichment_logs_updated_at
    BEFORE UPDATE ON dataset_enrichment_logs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
