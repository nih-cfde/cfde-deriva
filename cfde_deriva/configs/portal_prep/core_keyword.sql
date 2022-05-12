INSERT INTO core_keyword (nid, kw)
  SELECT
    cf.nid,
    cfde_keywords_merge(
      cfde_keywords(n.name, n.abbreviation),

      (SELECT cfde_keywords_agg(p.name, p.abbreviation, p.description)
       FROM json_each(cf.projects) pj JOIN project p ON (pj.value = p.nid)),

      (SELECT cfde_keywords_agg(d.dcc_name, d.dcc_abbreviation, d.dcc_description)
       FROM json_each(cf.dccs) dj JOIN dcc d ON (dj.value = d.nid)),

      (SELECT cfde_keywords_merge(
         cfde_keywords_agg(pht.id, pht.name, pht.description, pht.synonyms)
       )
       FROM json_each(cf.phenotypes) phtj JOIN phenotype pht ON (json_extract(phtj.value, '$[0]') = pht.nid)),

      (SELECT cfde_keywords_merge(
         cfde_keywords_agg(dis.id, dis.name, dis.description, dis.synonyms),
         cfde_keywords_agg(dis2.id, dis2.name, dis2.description, dis2.synonyms)
       )
       FROM json_each(cf.diseases) disj JOIN disease dis ON (json_extract(disj.value, '$[0]') = dis.nid)
       JOIN disease_slim dis_slim ON (disj.value = dis_slim.original_term) JOIN disease dis2 ON (dis_slim.slim_term = dis.nid)),

      (SELECT cfde_keywords_agg(sx.id, sx.name, sx.description)
       FROM json_each(cf.sexes) sxj JOIN sex sx ON (sxj.value = sx.nid)),

      (SELECT cfde_keywords_agg(rc.id, rc.name, rc.description)
       FROM json_each(cf.races) rcj JOIN race rc ON (rcj.value = rc.nid)),

      (SELECT cfde_keywords_agg(eth.id, eth.name, eth.description)
       FROM json_each(cf.ethnicities) ethj JOIN ethnicity eth ON (ethj.value = eth.nid)),

      (SELECT cfde_keywords_agg(sr.name, sr.description)
       FROM json_each(cf.subject_roles) srj JOIN subject_role sr ON (srj.value = sr.nid)),

      (SELECT cfde_keywords_agg(sg.name, sg.description)
       FROM json_each(cf.subject_granularities) sgj JOIN subject_granularity sg ON (sgj.value = sg.nid)),

      (SELECT cfde_keywords_merge(
         cfde_keywords_agg(t.id, t.name, t.description, t.synonyms)
       )
       FROM json_each(cf.ncbi_taxons) tj JOIN ncbi_taxonomy t ON (tj.value = t.nid)),

      (SELECT cfde_keywords_merge(
         cfde_keywords_agg(a.id, a.name, a.description, a.synonyms),
         cfde_keywords_agg(a2.id, a2.name, a2.description, a2.synonyms)
       )
       FROM json_each(cf.anatomies) aj JOIN anatomy a ON (aj.value = a.nid)
       JOIN anatomy_slim a_slim ON (aj.value = a_slim.original_term) JOIN anatomy a2 ON (a_slim.slim_term = a2.nid)),

      (SELECT cfde_keywords_merge(
         cfde_keywords_agg("at".id, "at".name, "at".description, "at".synonyms),
         cfde_keywords_agg("at2".id, "at2".name, "at2".description, "at2".synonyms)
       )
       FROM json_each(cf.assay_types) atj JOIN assay_type "at" ON (atj.value = "at".nid)
       JOIN assay_type_slim at_slim ON (atj.value = at_slim.original_term) JOIN assay_type at2 ON (at_slim.slim_term = at2.nid)),

      (SELECT cfde_keywords_merge(
         cfde_keywords_agg(ant.id, ant.name, ant.description, ant.synonyms)
       )
       FROM json_each(cf.analysis_types) antj JOIN analysis_type ant ON (antj.value = ant.nid)),

      (SELECT cfde_keywords_merge(
         cfde_keywords_agg(ff.id, ff.name, ff.description, ff.synonyms),
         cfde_keywords_agg(ff2.id, ff2.name, ff2.description, ff2.synonyms)
       )
       FROM json_each(cf.file_formats) ffj JOIN file_format ff ON (ffj.value = ff.nid)
       JOIN file_format_slim ff_slim ON (ffj.value = ff_slim.original_term) JOIN file_format ff2 ON (ff_slim.slim_term = ff2.nid)),

      (SELECT cfde_keywords_merge(
         cfde_keywords_agg(dt.id, dt.name, dt.description, dt.synonyms),
         cfde_keywords_agg(dt2.id, dt2.name, dt2.description, dt2.synonyms)
       )
       FROM json_each(cf.data_types) dtj JOIN data_type dt ON (dtj.value = dt.nid)
       JOIN data_type_slim dt_slim ON (dtj.value = dt_slim.original_term) JOIN data_type dt2 ON (dt_slim.slim_term = dt2.nid)),

      (SELECT cfde_keywords_agg(mt.id, mt.name, mt.description)
       FROM json_each(cf.mime_types) mtj JOIN mime_type mt ON (mtj.value = mt.nid))

    ) AS kw
  FROM core_fact cf
  JOIN id_namespace n ON (cf.id_namespace = n.nid)
;

UPDATE core_fact AS v
SET
-- HACK: undo usage of -1 in place of NULLs before we send to catalog
-- has nothing to do with kw but this should be done in the same rewrite order
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
;
