UPDATE core_fact AS u
SET num_collections = s.num_collections
FROM (
  SELECT s.core_fact, count(*)
  FROM collection s
  GROUP BY s.core_fact
) s(core_fact, num_collections)
WHERE s.core_fact = cf.nid
;
