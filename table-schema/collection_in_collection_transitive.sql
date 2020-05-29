WITH RECURSIVE t AS (

  SELECT
    id_namespace AS leader_collection_id_namespace,
    id AS leader_collection_id,
    id_namespace AS member_collection_id_namespace,
    id AS member_collection_id
  FROM collection
 
  UNION

  SELECT
    t.leader_collection_id_namespace AS leader_collection_id_namespace,
    t.leader_collection_id AS leader_collection_id,
    a.subset_collection_id_namespace AS member_collection_id_namespace,
    a.subset_collection_id AS member_collection_id
  FROM collection_in_collection a
  JOIN t ON (a.superset_collection_id_namespace = t.member_collection_id_namespace AND a.superset_collection_id = t.member_collection_id)

)
SELECT 
  leader_collection_id_namespace,
  leader_collection_id,
  member_collection_id_namespace,
  member_collection_id
FROM t;
