ALTER TABLE datasets
    ADD COLUMN docs_score   FLOAT,
    ADD COLUMN repr_score   FLOAT,
    ADD COLUMN social_score FLOAT,
    ADD COLUMN legal_score  FLOAT;
