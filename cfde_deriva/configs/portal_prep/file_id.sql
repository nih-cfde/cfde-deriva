-- provide a single-column id for use in saved query facets etc.
UPDATE file AS s
SET id = n.id || s.local_id
FROM id_namespace n
WHERE s.id_namespace = n.nid
;
