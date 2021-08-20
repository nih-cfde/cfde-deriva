-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO file (
  nid,
  id_namespace,
  project,
  file_format,
  data_type,
  assay_type,
  mime_type,
  creation_time,
  size_in_bytes,
  uncompressed_size_in_bytes,
  sha256,
  md5,
  local_id,
  persistent_id,
  filename
)
SELECT
  f.nid,
  i.nid,
  p.nid,
  ff.nid,
  dt.nid,
  "at".nid,
  mt.nid,
  f.creation_time,
  f.size_in_bytes,
  f.uncompressed_size_in_bytes,
  f.sha256,
  f.md5,
  f.local_id,
  f.persistent_id,
  f.filename
FROM submission.file f
JOIN submission.id_namespace i ON (f.id_namespace = i.id)
JOIN submission.project p ON (f.project_id_namespace = p.id_namespace AND f.project_local_id = p.local_id)
LEFT JOIN submission.file_format ff ON (f.file_format = ff.id)
LEFT JOIN submission.data_type dt ON (f.data_type = dt.id)
LEFT JOIN submission.assay_type "at" ON (f.assay_type = "at".id)
LEFT JOIN mime_type mt ON (f.mime_type = mt.id)
;
