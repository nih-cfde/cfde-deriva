INSERT INTO core_fact_anatomy (core_fact, anatomy)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.anatomies) j
WHERE j.value IS NOT NULL
;
