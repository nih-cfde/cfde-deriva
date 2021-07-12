UPDATE core_fact AS u
SET num_subjects = s.num_subjects
FROM (
  SELECT s.core_fact, count(*)
  FROM subject s
  GROUP BY s.core_fact
) s(core_fact, num_subjects)
WHERE s.core_fact = cf.nid
;
