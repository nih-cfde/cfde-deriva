UPDATE core_fact AS u
SET num_collections = s.num_collections
FROM (
  SELECT
    s.core_fact,
    count(*) AS num_collections
  FROM collection s
  GROUP BY s.core_fact
) s
WHERE s.core_fact = u.nid
;
