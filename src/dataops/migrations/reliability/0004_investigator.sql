-- Milestone 4: runbook retrieval index and investigation audit fields.
-- The `vector` extension is created by the bootstrap (superuser) in the reliability database.

CREATE TABLE reliability.runbook_documents (
    doc_id           text PRIMARY KEY,          -- runbook_id from front matter, e.g. 'RB-001'
    path             text        NOT NULL,      -- file path relative to the repository
    title            text        NOT NULL,
    service          text,
    incident_types   text[]      NOT NULL DEFAULT '{}',
    version          text        NOT NULL,
    effective_date   date,
    owner            text,
    content_sha256   text        NOT NULL,      -- fingerprint of the whole file
    embedding_model  text        NOT NULL,      -- chunks are re-embedded when the model changes
    chunk_count      integer     NOT NULL,
    indexed_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE reliability.runbook_chunks (
    chunk_id         text PRIMARY KEY,          -- '<doc_id>#<section slug>[-<n>]'
    doc_id           text        NOT NULL REFERENCES reliability.runbook_documents ON DELETE CASCADE,
    doc_version      text        NOT NULL,
    section          text        NOT NULL,
    ordinal          integer     NOT NULL,
    content          text        NOT NULL,
    content_sha256   text        NOT NULL,
    metadata         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    embedding_model  text        NOT NULL,
    embedding        vector(384) NOT NULL,
    tsv              tsvector GENERATED ALWAYS AS
                         (to_tsvector('english', section || ' ' || content)) STORED
);
CREATE INDEX runbook_chunks_tsv ON reliability.runbook_chunks USING gin (tsv);
CREATE INDEX runbook_chunks_embedding ON reliability.runbook_chunks
    USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON reliability.runbook_chunks (doc_id);

ALTER TABLE reliability.investigation_runs
    ADD COLUMN question         text,
    ADD COLUMN embedding_model  text,
    ADD COLUMN tool_call_count  integer,
    ADD COLUMN validation       jsonb;
-- 'abstained' already exists; investigations never change incident state (read-only).
