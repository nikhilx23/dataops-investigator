-- Simulation bookkeeping, deliberately outside the operational `shop` schema: it is not
-- business data, is not granted to src_reader, and is never extracted.
CREATE SCHEMA sim;

CREATE TABLE sim.clock (
    id                smallint PRIMARY KEY CHECK (id = 1),
    seed              integer     NOT NULL,
    profile           text        NOT NULL,
    dataset_checksum  text        NOT NULL,
    simulated_until   timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL DEFAULT clock_timestamp()
);
