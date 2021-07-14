UPDATE core_fact AS u
SET num_subjects = s.num_subjects
FROM (
  SELECT
    s.core_fact,
    count(*) AS num_subjects
  FROM subject s
  GROUP BY s.core_fact
) s
WHERE s.core_fact = u.nid
;
