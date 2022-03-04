UPDATE protein_fact AS v
SET kw = s.kw
FROM (
  SELECT
    prf.nid,
    cfde_keywords_merge(
      (SELECT cfde_keywords_merge(
         cfde_keywords_agg(pr.id, pr.name, pr.description),
         cfde_keywords_merge_agg(pr.synonyms)
       )
       FROM json_each(prf.proteins) prj JOIN protein pr ON (prj.value = pr.nid))
    ) AS kw
  FROM protein_fact prf
) s
WHERE v.nid = s.nid
;
