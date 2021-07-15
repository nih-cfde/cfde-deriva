INSERT INTO core_fact_project (core_fact, project)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.projects) j
WHERE j.value IS NOT NULL
;
