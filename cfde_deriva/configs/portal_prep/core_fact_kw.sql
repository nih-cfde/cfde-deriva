UPDATE core_fact AS v
SET kw = s.kw
FROM (
  SELECT
    cf.nid,
    cfde_keywords_merge(
      cfde_keywords_agg(n.name, n.abbreviation),
      cfde_keywords_agg(p.name, p.abbreviation, p.description),
      cfde_keywords_agg(sr.name, sr.description),
      cfde_keywords_agg(sg.name, sg.description),
      cfde_keywords_agg(t.id, t.name, t.description),
      cfde_keywords_agg(tsyn.value),
      cfde_keywords_agg(a.id, a.name, a.description),
      cfde_keywords_agg(asyn.value),
      cfde_keywords_agg("at".id, "at".name, "at".description),
      cfde_keywords_agg(atsyn.value),
      cfde_keywords_agg(ff.id, ff.name, ff.description),
      cfde_keywords_agg(ffsyn.value),
      cfde_keywords_agg(dt.id, dt.name, dt.description),
      cfde_keywords_agg(dtsyn.value),
      cfde_keywords_agg(mt.id, mt.name, mt.description)
    ) AS kw
  FROM core_fact cf
  JOIN id_namespace n ON (cf.id_namespace = n.nid)
  LEFT JOIN json_each(cf.projects) pj
  LEFT JOIN project p ON (pj.value = p.nid)
  LEFT JOIN json_each(cf.dccs) dj
  LEFT JOIN dcc d ON (dj.value = d.nid)
  LEFT JOIN json_each(cf.subject_roles) srj
  LEFT JOIN subject_role sr ON (srj.value = sr.nid)
  LEFT JOIN json_each(cf.subject_granularities) sgj
  LEFT JOIN subject_granularity sg ON (sgj.value = sg.nid)
  LEFT JOIN json_each(cf.ncbi_taxons) tj
  LEFT JOIN ncbi_taxonomy t ON (tj.value = t.nid)
  LEFT JOIN json_each(t.synonyms) tsyn
  LEFT JOIN json_each(cf.anatomies) aj
  LEFT JOIN anatomy a ON (aj.value = a.nid)
  LEFT JOIN json_each(a.synonyms) asyn
  LEFT JOIN json_each(cf.assay_types) atj
  LEFT JOIN assay_type "at" ON (atj.value = "at".nid)
  LEFT JOIN json_each("at".synonyms) atsyn
  LEFT JOIN json_each(cf.file_formats) ffj
  LEFT JOIN file_format ff ON (ffj.value = ff.nid)
  LEFT JOIN json_each(ff.synonyms) ffsyn
  LEFT JOIN json_each(cf.data_types) dtj
  LEFT JOIN data_type dt ON (dtj.value = dt.nid)
  LEFT JOIN json_each(dt.synonyms) dtsyn
  LEFT JOIN json_each(cf.mime_types) mtj
  LEFT JOIN mime_type mt ON (mtj.value = mt.nid)
  GROUP BY cf.nid
) s
WHERE v.nid = s.nid
;
