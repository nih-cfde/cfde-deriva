INSERT INTO core_fact_assay_type (core_fact, assay_type)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.assay_types) j
WHERE j.value IS NOT NULL
;
