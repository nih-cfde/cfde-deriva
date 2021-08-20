-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO subject_in_collection (
  subject,
  collection
)
SELECT
  s.nid,
  c.nid
FROM submission.subject_in_collection sic
JOIN submission.subject s ON (sic.subject_id_namespace = s.id_namespace AND sic.subject_local_id = s.local_id)
JOIN submission.collection c ON (sic.collection_id_namespace = c.id_namespace AND sic.collection_local_id = c.local_id)
;
