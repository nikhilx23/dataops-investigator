-- Milestone 5: recovery control (approval, execution, verification) with an append-only audit log.

ALTER TABLE reliability.recovery_actions
    ADD COLUMN proposed_by       text,
    ADD COLUMN investigation_id  uuid REFERENCES reliability.investigation_runs,
    ADD COLUMN expected_effects  jsonb,
    ADD COLUMN execution_output  jsonb,
    ADD COLUMN approval_reason   text,
    ADD COLUMN executed_by       text,
    ADD COLUMN verified_at       timestamptz;

ALTER TABLE reliability.recovery_actions DROP CONSTRAINT recovery_actions_status_check;
ALTER TABLE reliability.recovery_actions ADD CONSTRAINT recovery_actions_status_check CHECK (status IN
    ('proposed','approved','rejected','running','succeeded','failed','verified','verification_failed'));

CREATE TABLE reliability.recovery_action_events (
    event_seq    bigserial   PRIMARY KEY,
    action_id    uuid        NOT NULL REFERENCES reliability.recovery_actions,
    from_status  text,
    to_status    text        NOT NULL,
    actor        text        NOT NULL,
    reason       text,
    details      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    at           timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON reliability.recovery_action_events (action_id, event_seq);
CREATE INDEX ON reliability.recovery_actions (incident_id);
