INSERT INTO collection_biosample_creation_time (
  collection_id_namespace,
  collection_local_id,
  biosample_creation_time
)
SELECT
  cg.leader_collection_id_namespace AS collection_id_namespace,
  cg.leader_collection_local_id AS collection_local_id,
  fbct.biosample_creation_time
FROM collection_in_collection_transitive cg
JOIN file_in_collection fic
  ON (cg.member_collection_id_namespace = fic.collection_id_namespace AND cg.member_collection_local_id = fic.collection_local_id)
JOIN file_biosample_creation_time fbct
  ON (fic.file_id_namespace = fbct.file_id_namespace AND fic.file_local_id = fbct.file_local_id)

UNION

SELECT DISTINCT
  cg.leader_collection_id_namespace AS collection_id_namespace,
  cg.leader_collection_local_id AS collection_local_id,
  b.creation_time AS biosample_creation_time
FROM collection_in_collection_transitive cg
JOIN biosample_in_collection bic
  ON (cg.member_collection_id_namespace = bic.collection_id_namespace AND cg.member_collection_local_id = bic.collection_local_id)
JOIN biosample b
  ON (bic.biosample_id_namespace = b.id_namespace AND bic.biosample_local_id = b.local_id)
WHERE b.creation_time IS NOT NULL
;
