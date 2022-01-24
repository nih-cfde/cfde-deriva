UPDATE collection AS v
SET kw = s.kw
FROM (
  SELECT
    s.nid,
    cfde_keywords_merge(
       cfde_keywords(
         s.local_id,
         s.persistent_id,
         s.abbreviation,
         s.name,
         s.description
      ),
      kw.kw
    ) AS kw
  FROM collection s
  JOIN core_fact cf ON (s.core_fact = cf.nid)
  JOIN core_keyword kw ON (cf.nid = kw.nid)
) s
WHERE v.nid = s.nid
;
