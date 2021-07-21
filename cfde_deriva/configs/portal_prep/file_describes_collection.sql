INSERT INTO file_describes_collection (
  file,
  collection
)
SELECT
  f.nid,
  c.nid
FROM submission.file_describes_collection fdc
JOIN submission.file f ON (fdc.file_id_namespace = f.id_namespace AND fdc.file_local_id = f.local_id)
JOIN submission.collection c ON (fdc.collection_id_namespace = c.id_namespace AND fdc.collection_local_id = c.local_id)
;
