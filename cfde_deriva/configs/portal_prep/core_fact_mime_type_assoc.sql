INSERT INTO core_fact_mime_type (core_fact, mime_type)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.mime_types) j
WHERE j.value IS NOT NULL
;
