-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO biosample (
  nid,
  id_namespace,
  project,
  assay_type,
  anatomy,
  creation_time,
  local_id,
  persistent_id
)
SELECT
  b.nid,
  i.nid,
  p.nid,
  "at".nid,
  a.nid,
  b.creation_time,
  b.local_id,
  b.persistent_id
FROM submission.biosample b
JOIN submission.id_namespace i ON (b.id_namespace = i.id)
JOIN submission.project p ON (b.project_id_namespace = p.id_namespace AND b.project_local_id = p.local_id)
LEFT JOIN submission.assay_type "at" ON (b.assay_type = "at".id)
LEFT JOIN submission.anatomy a ON (b.anatomy = a.id)
;
