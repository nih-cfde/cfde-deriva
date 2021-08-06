-- provide a single-column id for use in saved query facets etc.
UPDATE project AS p
SET id = n.id || p.local_id
FROM id_namespace n
WHERE p.id_namespace = n.nid
;
