CREATE TABLE IF NOT EXISTS search_logs (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE SET NULL,
    query      TEXT NOT NULL,
    filters    JSONB,
    result_count INTEGER NOT NULL DEFAULT 0,
    latency_ms FLOAT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_search_logs_user_id    ON search_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_search_logs_created_at ON search_logs(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_datasets_file_formats_gin ON datasets USING gin(file_formats);
