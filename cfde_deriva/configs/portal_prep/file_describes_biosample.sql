INSERT INTO file_describes_biosample (
  file,
  biosample
)
SELECT
  f.nid,
  b.nid
FROM submission.file_describes_biosample fdb
JOIN submission.file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
JOIN submission.biosample b ON (fdb.biosample_id_namespace = b.id_namespace AND fdb.biosample_local_id = b.local_id)
;
