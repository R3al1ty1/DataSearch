ALTER TABLE datasets ADD COLUMN IF NOT EXISTS search_vector tsvector;

CREATE INDEX IF NOT EXISTS idx_datasets_search_vector_gin
    ON datasets USING gin(search_vector);

CREATE OR REPLACE FUNCTION datasets_search_vector_update() RETURNS trigger AS $$
BEGIN
    NEW.search_vector := to_tsvector('english',
        coalesce(NEW.title, '') || ' ' ||
        coalesce(NEW.description, '') || ' ' ||
        coalesce(array_to_string(NEW.tags, ' '), '')
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS datasets_search_vector_trig ON datasets;

CREATE TRIGGER datasets_search_vector_trig
    BEFORE INSERT OR UPDATE OF title, description, tags
    ON datasets
    FOR EACH ROW EXECUTE FUNCTION datasets_search_vector_update();

UPDATE datasets
SET search_vector = to_tsvector('english',
    coalesce(title, '') || ' ' ||
    coalesce(description, '') || ' ' ||
    coalesce(array_to_string(tags, ' '), '')
);
