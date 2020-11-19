INSERT INTO collection_assay_type (
  collection_id_namespace,
  collection_local_id,
  assay_type
)
SELECT
  cg.leader_collection_id_namespace AS collection_id_namespace,
  cg.leader_collection_local_id AS collection_local_id,
  f.assay_type
FROM collection_in_collection_transitive cg
JOIN file_in_collection fic
  ON (cg.member_collection_id_namespace = fic.collection_id_namespace AND cg.member_collection_local_id = fic.collection_local_id)
JOIN file f ON (fic.file_id_namespace = f.id_namespace AND fic.file_local_id = f.local_id)
WHERE f.assay_type IS NOT NULL

UNION

SELECT DISTINCT
  cg.leader_collection_id_namespace AS collection_id_namespace,
  cg.leader_collection_local_id AS collection_local_id,
  f.assay_type
FROM collection_in_collection_transitive cg
JOIN biosample_in_collection bic
  ON (cg.member_collection_id_namespace = bic.collection_id_namespace AND cg.member_collection_local_id = bic.collection_local_id)
JOIN file_describes_biosample fdb
  ON (bic.biosample_id_namespace = fdb.biosample_id_namespace AND bic.biosample_local_id = fdb.biosample_local_id)
JOIN file f
  ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
WHERE f.assay_type IS NOT NULL
;
