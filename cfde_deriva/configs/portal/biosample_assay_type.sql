INSERT INTO biosample_assay_type (
  biosample_id_namespace,
  biosample_local_id,
  assay_type
)
SELECT DISTINCT
  fdb.biosample_id_namespace,
  fdb.biosample_local_id,
  f.assay_type
FROM file_describes_biosample fdb
JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
WHERE f.assay_type IS NOT NULL
;
