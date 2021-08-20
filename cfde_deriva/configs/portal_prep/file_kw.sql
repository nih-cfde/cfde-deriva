UPDATE file AS v
SET kw = s.kw
FROM (
  SELECT
    s.nid,
    cfde_keywords_merge(
       cfde_keywords(
         s.local_id,
         s.persistent_id,
         s.filename
      ),
      cf.kw
    ) AS kw
  FROM file s
  JOIN core_fact cf ON (s.core_fact = cf.nid)
) s
WHERE v.nid = s.nid
;
