UPDATE core_fact AS u
SET project = s.value
FROM (
  SELECT
    s.nid,
    j.value
  FROM core_fact s
  JOIN json_each(s.projects) j
  WHERE j.value IS NOT NULL
  GROUP BY s.nid
  HAVING count(*) = 1
) s
WHERE u.nid = s.nid
;
