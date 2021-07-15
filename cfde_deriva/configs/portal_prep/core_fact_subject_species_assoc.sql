INSERT INTO core_fact_subject_species (core_fact, subject_species)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.subject_species) j
WHERE j.value IS NOT NULL
;
