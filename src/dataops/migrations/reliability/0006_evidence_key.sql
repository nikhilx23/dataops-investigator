-- The same observation or lineage path can be evidence for more than one incident over time
-- (e.g. a resolved incident and a later one with the same root). Evidence ids stay stable and
-- citable; uniqueness is per incident.
ALTER TABLE reliability.incident_evidence DROP CONSTRAINT incident_evidence_pkey;
ALTER TABLE reliability.incident_evidence ADD PRIMARY KEY (incident_id, evidence_id);
CREATE INDEX ON reliability.incident_evidence (evidence_id);
