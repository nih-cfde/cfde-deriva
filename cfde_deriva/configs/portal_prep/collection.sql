-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO collection (
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
  c.nid,
  i.nid,
  c.creation_time,
  c.local_id,
  c.persistent_id,
  c.abbreviation,
  c."name",
  c.description
FROM submission.collection c
JOIN submission.id_namespace i ON (c.id_namespace = i.id)
;
