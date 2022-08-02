INSERT INTO ncpi_file (
  nid,
  "name",
  drs_uri,
  study_registration,
  study_id,
  participant_id,
  specimen_id,
  experimental_strategy,
  file_format
)
SELECT
  f.nid,
  f.filename,
  f.persistent_id,
  f.project_id_namespace,
  f.project_local_id,
  (SELECT s.local_id
   FROM submission.subject s
   JOIN file_describes_subject a ON (s.nid = a.subject)
   WHERE a.file = f.nid
   LIMIT 1),
  (SELECT b.local_id
   FROM submission.biosample b
   JOIN file_describes_biosample a ON (b.nid = a.biosample)
   WHERE a.file = f.nid
   LIMIT 1),
  "at".name,
  ff.name
FROM submission.file f
LEFT OUTER JOIN assay_type "at" ON (f.assay_type = "at".id)
LEFT OUTER JOIN file_format ff ON (f.file_format = ff.id)
WHERE true
ON CONFLICT DO NOTHING
;
