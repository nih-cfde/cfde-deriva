INSERT INTO collection_assay_type (
  collection_id_namespace,
  collection_id,
  assay_type
)
SELECT
  cg.leader_collection_id_namespace AS collection_id_namespace,
  cg.leader_collection_id AS collection_id,
  fat.assay_type
FROM collection_in_collection_transitive cg
JOIN file_in_collection fic
  ON (cg.member_collection_id_namespace = fic.collection_id_namespace AND cg.member_collection_id = fic.collection_id)
JOIN file_assay_type fat
  ON (fic.file_id_namespace = fat.file_id_namespace AND fic.file_id = fat.file_id)

UNION

SELECT DISTINCT
  cg.leader_collection_id_namespace AS collection_id_namespace,
  cg.leader_collection_id AS collection_id,
  b.assay_type
FROM collection_in_collection_transitive cg
JOIN biosample_in_collection bic
  ON (cg.member_collection_id_namespace = bic.collection_id_namespace AND cg.member_collection_id = bic.collection_id)
JOIN biosample b
  ON (bic.biosample_id_namespace = b.id_namespace AND bic.biosample_id = b.id)
WHERE b.assay_type IS NOT NULL
