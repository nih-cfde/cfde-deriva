UPDATE core_fact AS v
SET kw = s.kw,
-- HACK: undo usage of -1 in place of NULLs before we send to catalog
-- has nothing to do with kw but this should be done in the same column-rewriting phase
  project = CASE WHEN v.project = -1 THEN NULL ELSE v.project END,
  sex = CASE WHEN v.sex = -1 THEN NULL ELSE v.sex END,
  ethnicity = CASE WHEN v.ethnicity = -1 THEN NULL ELSE v.ethnicity END,
  subject_granularity = CASE WHEN v.subject_granularity = -1 THEN NULL ELSE v.subject_granularity END,
  anatomy = CASE WHEN v.anatomy = -1 THEN NULL ELSE v.anatomy END,
  assay_type = CASE WHEN v.assay_type = -1 THEN NULL ELSE v.assay_type END,
  analysis_type = CASE WHEN v.analysis_type = -1 THEN NULL ELSE v.analysis_type END,
  file_format = CASE WHEN v.file_format = -1 THEN NULL ELSE v.file_format END,
  compression_format = CASE WHEN v.compression_format = -1 THEN NULL ELSE v.compression_format END,
  data_type = CASE WHEN v.data_type = -1 THEN NULL ELSE v.data_type END,
  mime_type = CASE WHEN v.mime_type = -1 THEN NULL ELSE v.mime_type END
FROM (
  SELECT
    cf.nid,
    cfde_keywords_merge(
      cfde_keywords_agg(n.name, n.abbreviation),
      cfde_keywords_agg(p.name, p.abbreviation, p.description),
      cfde_keywords_agg(d.dcc_name, d.dcc_abbreviation, d.dcc_description),

      cfde_keywords_agg(pht.id, pht.name, pht.description),
      cfde_keywords_merge_agg(pht.synonyms),
      cfde_keywords_agg(dis.id, dis.name, dis.description),
      cfde_keywords_merge_agg(dis.synonyms),
      cfde_keywords_agg(subst.id, subst.name, subst.description),
      cfde_keywords_merge_agg(subst.synonyms),
      cfde_keywords_agg(gn.id, gn.name, gn.description),
      cfde_keywords_merge_agg(gn.synonyms),

      cfde_keywords_agg(sx.id, sx.name, sx.description),
      cfde_keywords_agg(rc.id, rc.name, rc.description),
      cfde_keywords_agg(eth.id, eth.name, eth.description),
      cfde_keywords_agg(sr.name, sr.description),
      cfde_keywords_agg(sg.name, sg.description),
      cfde_keywords_agg(t.id, t.name, t.description),
      cfde_keywords_merge_agg(t.synonyms),

      cfde_keywords_agg(a.id, a.name, a.description),
      cfde_keywords_merge_agg(a.synonyms),
      cfde_keywords_agg(a2.id, a2.name, a2.description),
      cfde_keywords_merge_agg(a2.synonyms),

      cfde_keywords_agg("at".id, "at".name, "at".description),
      cfde_keywords_merge_agg("at".synonyms),
      cfde_keywords_agg(ant.id, ant.name, ant.description),
      cfde_keywords_merge_agg(ant.synonyms),
      cfde_keywords_agg(ff.id, ff.name, ff.description),
      cfde_keywords_merge_agg(ff.synonyms),
      cfde_keywords_agg(dt.id, dt.name, dt.description),
      cfde_keywords_merge_agg(dt.synonyms),
      cfde_keywords_agg(mt.id, mt.name, mt.description)
    ) AS kw
  FROM core_fact cf
  JOIN id_namespace n ON (cf.id_namespace = n.nid)
  LEFT JOIN json_each(cf.projects) pj
  LEFT JOIN project p ON (pj.value = p.nid)
  LEFT JOIN json_each(cf.dccs) dj
  LEFT JOIN dcc d ON (dj.value = d.nid)

  LEFT JOIN json_each(cf.phenotypes) phtj
  LEFT JOIN phenotype pht ON (json_extract(phtj.value, '$[0]') = pht.nid)
  LEFT JOIN json_each(cf.diseases) disj
  LEFT JOIN disease dis ON (json_extract(disj.value, '$[0]') = dis.nid)
  LEFT JOIN json_each(cf.substances) substj
  LEFT JOIN substance subst ON (substj.value = subst.nid)
  LEFT JOIN json_each(cf.genes) gnj
  LEFT JOIN gene gn ON (gnj.value = gn.nid)

  LEFT JOIN json_each(cf.sexes) sxj
  LEFT JOIN sex sx ON (sxj.value = sx.nid)
  LEFT JOIN json_each(cf.races) rcj
  LEFT JOIN race rc ON (rcj.value = rc.nid)
  LEFT JOIN json_each(cf.ethnicities) ethj
  LEFT JOIN ethnicity eth ON (ethj.value = eth.nid)
  LEFT JOIN json_each(cf.subject_roles) srj
  LEFT JOIN subject_role sr ON (srj.value = sr.nid)
  LEFT JOIN json_each(cf.subject_granularities) sgj
  LEFT JOIN subject_granularity sg ON (sgj.value = sg.nid)
  LEFT JOIN json_each(cf.ncbi_taxons) tj
  LEFT JOIN ncbi_taxonomy t ON (tj.value = t.nid)

  LEFT JOIN json_each(cf.anatomies) aj
  LEFT JOIN anatomy a ON (aj.value = a.nid)
  LEFT JOIN anatomy_slim a_slim ON (aj.value = a_slim.original_term)
  LEFT JOIN anatomy a2 ON (a_slim.slim_term = a2.nid)

  LEFT JOIN json_each(cf.assay_types) atj
  LEFT JOIN assay_type "at" ON (atj.value = "at".nid)
  LEFT JOIN json_each(cf.analysis_types) antj
  LEFT JOIN analysis_type ant ON (antj.value = ant.nid)
  LEFT JOIN json_each(cf.file_formats) ffj
  LEFT JOIN file_format ff ON (ffj.value = ff.nid)
  LEFT JOIN json_each(cf.data_types) dtj
  LEFT JOIN data_type dt ON (dtj.value = dt.nid)
  LEFT JOIN json_each(cf.mime_types) mtj
  LEFT JOIN mime_type mt ON (mtj.value = mt.nid)

  GROUP BY cf.nid
) s
WHERE v.nid = s.nid
;
