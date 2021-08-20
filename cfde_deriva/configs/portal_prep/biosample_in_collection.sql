-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO biosample_in_collection (
  biosample,
  collection
)
SELECT
  b.nid,
  c.nid
FROM submission.biosample_in_collection bic
JOIN submission.biosample b ON (bic.biosample_id_namespace = b.id_namespace AND bic.biosample_local_id = b.local_id)
JOIN submission.collection c ON (bic.collection_id_namespace = c.id_namespace AND bic.collection_local_id = c.local_id)
;
