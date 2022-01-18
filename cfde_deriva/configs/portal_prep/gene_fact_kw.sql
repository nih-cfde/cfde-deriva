UPDATE gene_fact AS v
SET kw = s.kw
FROM (
  SELECT
    gf.nid,
    cfde_keywords_merge(
      (SELECT cfde_keywords_merge(
         cfde_keywords_agg(gn.id, gn.name, gn.description),
         cfde_keywords_merge_agg(gn.synonyms)
       )
       FROM json_each(gf.genes) gnj JOIN gene gn ON (gnj.value = gn.nid))
    ) AS kw
  FROM gene_fact gf
) s
WHERE v.nid = s.nid
;
