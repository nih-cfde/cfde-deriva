-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO subject_disease (
  subject,
  disease
)
SELECT
  s.nid,
  d.nid
FROM submission.subject_disease sd
JOIN submission.subject s ON (sd.subject_id_namespace = s.id_namespace AND sd.subject_local_id = s.local_id)
JOIN submission.disease d ON (sd.disease = d.id)
;
