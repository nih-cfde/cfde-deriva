INSERT INTO core_fact_dcc (core_fact, dcc)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.dccs) j
WHERE j.value IS NOT NULL
;
