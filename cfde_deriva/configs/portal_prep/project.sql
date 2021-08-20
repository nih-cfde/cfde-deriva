-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO project (
  nid,
  id_namespace,
  creation_time,
  local_id,
  persistent_id,
  abbreviation,
  "name",
  description
)
SELECT
  p.nid,
  i.nid,
  p.creation_time,
  p.local_id,
  p.persistent_id,
  p.abbreviation,
  p."name",
  p.description
FROM submission.project p
JOIN submission.id_namespace i ON (p.id_namespace = i.id)
;
