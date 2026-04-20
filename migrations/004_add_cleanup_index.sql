CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_active_last_checked
    ON datasets (is_active, last_checked_at NULLS FIRST)
    WHERE is_active = true;
