ALTER TABLE search_logs
    ADD COLUMN result_ids   JSONB,
    ADD COLUMN score_version VARCHAR(30) NOT NULL DEFAULT 'v1_hybrid';

CREATE INDEX IF NOT EXISTS idx_search_logs_score_version ON search_logs(score_version);

CREATE TABLE IF NOT EXISTS search_click_events (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    search_log_id UUID REFERENCES search_logs(id) ON DELETE SET NULL,
    user_id       UUID REFERENCES users(id) ON DELETE SET NULL,
    dataset_id    UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    position      INTEGER NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_click_events_search_log  ON search_click_events(search_log_id);
CREATE INDEX IF NOT EXISTS idx_click_events_dataset     ON search_click_events(dataset_id);
CREATE INDEX IF NOT EXISTS idx_click_events_created_at  ON search_click_events(created_at DESC);
