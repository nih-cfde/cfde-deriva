UPDATE core_fact AS u
SET subject_granularity = s.value
FROM (
  SELECT
    s.nid,
    j.value
  FROM core_fact s
  JOIN json_each(s.subject_granularities) j
  WHERE j.value IS NOT NULL
  GROUP BY s.nid
  HAVING count(*) = 1
) s
WHERE u.nid = s.nid
;
