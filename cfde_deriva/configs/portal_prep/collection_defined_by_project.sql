-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO collection_defined_by_project (
  collection,
  project
)
SELECT
  c.nid,
  p.nid
FROM submission.collection_defined_by_project cdbp
JOIN submission.collection c ON (cdbp.collection_id_namespace = c.id_namespace AND cdbp.collection_local_id = c.local_id)
JOIN submission.project p ON (cdbp.project_id_namespace = p.id_namespace AND cdbp.project_local_id = p.local_id)
;
