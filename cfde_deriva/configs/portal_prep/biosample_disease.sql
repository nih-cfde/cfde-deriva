-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO biosample_disease (
  biosample,
  disease
)
SELECT
  b.nid,
  d.nid
FROM submission.biosample_disease bd
JOIN submission.biosample b ON (bd.biosample_id_namespace = b.id_namespace AND bd.biosample_local_id = b.local_id)
JOIN submission.disease d ON (bd.disease = d.id)
;
