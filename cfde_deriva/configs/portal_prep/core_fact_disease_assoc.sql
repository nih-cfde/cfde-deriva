INSERT INTO core_fact_disease (core_fact, disease)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.diseases) j
WHERE j.value IS NOT NULL
;
