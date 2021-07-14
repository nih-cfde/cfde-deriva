WITH RECURSIVE t AS (

  SELECT
    nid AS leader_collection,
    nid AS member_collection
  FROM collection
 
  UNION

  SELECT
    t.leader_collection AS leader_collection,
    a.subset_collection AS member_collection
  FROM collection_in_collection a
  JOIN t ON (a.superset_collection = t.member_collection)

)
INSERT INTO collection_in_collection_transitive (
    leader_collection,
    member_collection
)
SELECT 
  leader_collection,
  member_collection
FROM t
;
