UPDATE core_fact AS u
SET num_biosamples = s.num_biosamples
FROM (
  SELECT
    s.core_fact,
    count(*) AS num_biosamples
  FROM biosample s
  GROUP BY s.core_fact
) s
WHERE s.core_fact = u.nid
;
