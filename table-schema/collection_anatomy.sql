INSERT INTO collection_anatomy (
  collection_id_namespace,
  collection_local_id,
  anatomy
)
SELECT
  cg.leader_collection_id_namespace AS collection_id_namespace,
  cg.leader_collection_id AS collection_local_id,
  fa.anatomy
FROM collection_in_collection_transitive cg
JOIN file_in_collection fic
  ON (cg.member_collection_id_namespace = fic.collection_id_namespace AND cg.member_collection_local_id = fic.collection_local_id)
JOIN file_anatomy fa
  ON (fic.file_id_namespace = fa.file_id_namespace AND fic.file_local_id = fa.file_local_id)

UNION

SELECT DISTINCT
  cg.leader_collection_id_namespace AS collection_id_namespace,
  cg.leader_collection_id AS collection_local_id,
  b.anatomy
FROM collection_in_collection_transitive cg
JOIN biosample_in_collection bic
  ON (cg.member_collection_id_namespace = bic.collection_id_namespace AND cg.member_collection_local_id = bic.collection_local_id)
JOIN biosample b
  ON (bic.biosample_id_namespace = b.id_namespace AND bic.biosample_local_id = b.local_id)
WHERE b.anatomy IS NOT NULL
;
