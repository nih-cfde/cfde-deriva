INSERT INTO core_fact_file_format (core_fact, file_format)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.file_formats) j
WHERE j.value IS NOT NULL
;
