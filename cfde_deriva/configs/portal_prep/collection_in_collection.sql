-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO collection_in_collection (
  superset_collection,
  subset_collection
)
SELECT
  p.nid,
  c.nid,
FROM submission.collection_in_collection cic
JOIN submission.collection p ON (cic.superset_collection_id_namespace = p.id_namespace AND cic.superset_collection_local_id = p.local_id)
JOIN submission.collection c ON (cic.subset_collection_id_namespace = c.id_namespace AND cic.subset_collection_local_id = c.local_id)
;
