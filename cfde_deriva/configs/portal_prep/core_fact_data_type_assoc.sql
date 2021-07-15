INSERT INTO core_fact_data_type (core_fact, data_type)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.data_types) j
WHERE j.value IS NOT NULL
;
