INSERT INTO core_fact_subject_role (core_fact, subject_role)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.subject_roles) j
WHERE j.value IS NOT NULL
;
