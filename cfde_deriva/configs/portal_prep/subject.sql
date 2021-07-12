-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO subject (
  nid,
  id_namespace,
  project,
  granularity,
  creation_time,
  local_id,
  persistent_id
)
SELECT
  s.nid,
  i.nid,
  p.nid,
  sg.nid,
  s.creation_time,
  s.local_id,
  s.persistent_id
FROM submission.subject s
JOIN submission.id_namespace i ON (s.id_namespace = i.id)
JOIN submission.project p ON (s.project_id_namespace = p.id_namespace AND s.project_local_id = p.local_id)
LEFT JOIN subject_granularity sg ON (s.granularity = sg.id)
;
