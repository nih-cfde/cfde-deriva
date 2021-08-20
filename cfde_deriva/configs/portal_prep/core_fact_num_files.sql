UPDATE core_fact AS u
SET num_files = s.num_files,
    total_size_in_bytes = s.total_size_in_bytes
FROM (
  SELECT
    s.core_fact,
    count(*) AS num_files,
    sum(s.size_in_bytes) AS total_size_in_bytes
  FROM file s
  GROUP BY s.core_fact
) s
WHERE s.core_fact = u.nid
;
