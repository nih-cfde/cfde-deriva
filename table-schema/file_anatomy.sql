INSERT INTO file_anatomy (
  file_id_namespace,
  file_id,
  anatomy
)
SELECT DISTINCT
  fdb.file_id_namespace,
  fdb.file_id,
  b.anatomy
FROM file_describes_biosample fdb
JOIN biosample b
  ON (fdb.biosample_id_namespace = b.id_namespace AND fdb.biosample_id = b.id)
WHERE b.anatomy IS NOT NULL
;
