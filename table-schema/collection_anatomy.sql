SELECT
  cg.leader_collection_id_namespace AS collection_id_namespace,
  cg.leader_collection_id AS collection_id,
  fa.anatomy
FROM collection_in_collection_transitive cg
JOIN file_in_collection fic
  ON (cg.member_collection_id_namespace = fic.collection_id_namespace AND cg.member_collection_id = fic.collection_id)
JOIN file_anatomy fa
  ON (fic.file_id_namespace = fa.file_id_namespace AND fic.file_id = fa.file_id)

UNION

SELECT DISTINCT
  cg.leader_collection_id_namespace AS collection_id_namespace,
  cg.leader_collection_id AS collection_id,
  b.anatomy
FROM collection_in_collection_transitive cg
JOIN biosample_in_collection bic
  ON (cg.member_collection_id_namespace = bic.collection_id_namespace AND cg.member_collection_id = bic.collection_id)
JOIN biosample b
  ON (bic.biosample_id_namespace = b.id_namespace AND bic.biosample_id = b.id)
WHERE b.anatomy IS NOT NULL
