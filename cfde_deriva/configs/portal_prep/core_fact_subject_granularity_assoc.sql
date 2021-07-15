INSERT INTO core_fact_subject_granularity (core_fact, subject_granularity)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.subject_granularities) j
WHERE j.value IS NOT NULL
;
