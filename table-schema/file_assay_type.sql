INSERT INTO file_assay_type (
  file_id_namespace,
  file_id,
  assay_type
)
SELECT DISTINCT
  fdb.file_id_namespace,
  fdb.file_id,
  b.assay_type
FROM file_describes_biosample fdb
JOIN biosample b
  ON (fdb.biosample_id_namespace = b.id_namespace AND fdb.biosample_id = b.id)
WHERE b.assay_type IS NOT NULL
