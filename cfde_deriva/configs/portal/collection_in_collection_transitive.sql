WITH RECURSIVE t AS (

  SELECT
    id_namespace AS leader_collection_id_namespace,
    local_id AS leader_collection_local_id,
    id_namespace AS member_collection_id_namespace,
    local_id AS member_collection_local_id
  FROM collection
 
  UNION

  SELECT
    t.leader_collection_id_namespace AS leader_collection_id_namespace,
    t.leader_collection_local_id AS leader_collection_local_id,
    a.subset_collection_id_namespace AS member_collection_id_namespace,
    a.subset_collection_local_id AS member_collection_local_id
  FROM collection_in_collection a
  JOIN t ON (a.superset_collection_id_namespace = t.member_collection_id_namespace AND a.superset_collection_local_id = t.member_collection_local_id)

)
INSERT INTO collection_in_collection_transitive (
    leader_collection_id_namespace,
    leader_collection_local_id,
    member_collection_id_namespace,
    member_collection_local_id
)
SELECT 
  leader_collection_id_namespace,
  leader_collection_local_id,
  member_collection_id_namespace,
  member_collection_local_id
FROM t
;
