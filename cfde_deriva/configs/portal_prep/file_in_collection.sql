-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO file_in_collection (
  file,
  collection
)
SELECT
  b.nid,
  c.nid,
FROM submission.file_in_collection fic
JOIN submission.file f ON (fic.file_id_namespace = f.id_namespace AND fic.file_local_id = f.local_id)
JOIN submission.collection c ON (fic.collection_id_namespace = c.id_namespace AND fic.collection_local_id = c.local_id)
;
