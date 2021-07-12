UPDATE core_fact AS u
SET num_biosamples = s.num_biosamples
FROM (
  SELECT s.core_fact, count(*)
  FROM biosamples s
  GROUP BY s.core_fact
) s(core_fact, num_biosamples)
WHERE s.core_fact = cf.nid
;
