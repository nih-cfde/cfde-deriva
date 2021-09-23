CREATE TEMPORARY TABLE file_facts AS
  SELECT
    f.nid,
    f.id_namespace,
    f.bundle_collection IS NOT NULL AS is_bundle,
    json_array(f.project) AS projects,
    json_sorted(COALESCE(json_group_array(DISTINCT d.nid) FILTER (WHERE d.nid IS NOT NULL), '[]')) AS dccs,
    json_sorted(COALESCE(json_group_array(DISTINCT dis.nid) FILTER (WHERE dis.nid IS NOT NULL), '[]')) AS diseases,
    json_sorted(COALESCE(json_group_array(DISTINCT subst.nid) FILTER (WHERE subst.nid IS NOT NULL), '[]')) AS substances,
    json_sorted(COALESCE(json_group_array(DISTINCT bg.gene) FILTER (WHERE bg.gene IS NOT NULL), '[]')) AS genes,
    json_sorted(COALESCE(json_group_array(DISTINCT srt."role") FILTER (WHERE srt."role" IS NOT NULL), '[]')) AS subject_roles,
    json_sorted(COALESCE(json_group_array(DISTINCT s.granularity) FILTER (WHERE s.granularity IS NOT NULL), '[]')) AS subject_granularities,
    json_sorted(COALESCE(json_group_array(DISTINCT s.sex) FILTER (WHERE s.sex IS NOT NULL), '[]')) AS sexes,
    json_sorted(COALESCE(json_group_array(DISTINCT sr.race) FILTER (WHERE sr.race IS NOT NULL), '[]')) AS races,
    json_sorted(COALESCE(json_group_array(DISTINCT s.ethnicity) FILTER (WHERE s.ethnicity IS NOT NULL), '[]')) AS ethnicities,
    json_sorted(COALESCE(json_group_array(DISTINCT ss.species) FILTER (WHERE ss.species IS NOT NULL), '[]')) AS subject_species,
    json_sorted(COALESCE(json_group_array(DISTINCT srt.taxon) FILTER (WHERE srt.taxon IS NOT NULL), '[]')) AS ncbi_taxons,
    json_sorted(COALESCE(json_group_array(DISTINCT b.anatomy) FILTER (WHERE b.anatomy IS NOT NULL), '[]')) AS anatomies,
    json_sorted(COALESCE(json_group_array(DISTINCT "at".nid) FILTER (WHERE "at".nid IS NOT NULL), '[]')) AS assay_types,
    CASE WHEN f.file_format IS NOT NULL THEN json_array(f.file_format) ELSE '[]' END AS file_formats,
    CASE WHEN f.compression_format IS NOT NULL THEN json_array(f.compression_format) ELSE '[]' END AS compression_formats,
    CASE WHEN f.data_type   IS NOT NULL THEN json_array(f.data_type)   ELSE '[]' END AS data_types,
    CASE WHEN f.mime_type   IS NOT NULL THEN json_array(f.mime_type)   ELSE '[]' END AS mime_types
  FROM file f
  JOIN project_in_project_transitive pipt ON (f.project = pipt.member_project)
  JOIN dcc d ON (pipt.leader_project = d.project)
  LEFT JOIN (
    file_describes_biosample fdb
    JOIN biosample b ON (fdb.biosample = b.nid)
    LEFT JOIN biosample_disease bd ON (b.nid = bd.biosample)
    LEFT JOIN biosample_substance bsubst ON (bsubst.biosample = b.nid)
    LEFT JOIN biosample_gene bg ON (bg.biosample = b.nid)
    LEFT JOIN (
      biosample_from_subject bfs
      JOIN subject s ON (bfs.subject = s.nid)
      LEFT JOIN subject_race sr ON (sr.subject = s.nid)
      LEFT JOIN subject_species ss ON (ss.subject = s.nid)
      LEFT JOIN subject_role_taxonomy srt ON (s.nid = srt.subject)
      LEFT JOIN subject_disease sd ON (s.nid = sd.subject)
      LEFT JOIN subject_substance ssubst ON (ssubst.subject = s.nid)
    ) ON (b.nid = bfs.biosample)
    LEFT JOIN disease dis ON (bd.disease = dis.nid OR sd.disease = dis.nid)
    LEFT JOIN substance subst ON (ssubst.substance = subst.nid OR bsubst.substance = subst.nid)
  ) ON (f.nid = fdb.file)
  LEFT JOIN assay_type "at" ON (f.assay_type = "at".nid OR b.assay_type = "at".nid)
  GROUP BY f.nid
;
CREATE INDEX IF NOT EXISTS file_facts_combo_idx ON file_facts(
    id_namespace,
    is_bundle,
    projects,
    dccs,
    diseases,
    substances,
    genes,
    subject_roles,
    subject_granularities,
    sexes,
    races,
    ethnicities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    is_bundle,

    project,
    subject_granularity,
    sex,
    ethnicity,
    anatomy,
    assay_type,
    file_format,
    compression_format,
    data_type,
    mime_type,
    
    diseases,
    substances,
    genes,

    projects,
    dccs,
    subject_roles,
    subject_granularities,
    sexes,
    races,
    ethnicities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,
    
    id_namespace_row,
    project_row,
    subject_granularity_row,
    sex_row,
    ethnicity_row,
    anatomy_row,
    assay_type_row,
    file_format_row,
    compression_format_row,
    data_type_row,
    mime_type_row
)
SELECT
  ff.id_namespace,
  is_bundle,

  ff.project,
  subject_granularity,
  sex,
  ethnicity,
  anatomy,
  assay_type,
  file_format,
  compression_format,
  data_type,
  mime_type,
    
  diseases,
  substances,
  genes,

  projects,
  dccs,
  subject_roles,
  subject_granularities,
  sexes,
  races,
  ethnicities,
  subject_species,
  ncbi_taxons,
  anatomies,
  assay_types,
  file_formats,
  compression_formats,
  data_types,
  mime_types,
        
  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
  json_object('nid', "at".nid, 'name', "at".name, 'description', "at".description) AS assay_type_row,
  json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
  json_object('nid', cfmt.nid,'name', cfmt.name, 'description', cfmt.description) AS compression_format_row,
  json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
  json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row
FROM (
  SELECT DISTINCT
    ff.id_namespace,
    ff.is_bundle,

    (SELECT j.value FROM json_each(ff.projects) j) AS project,
    CASE WHEN json_array_length(ff.subject_granularities) = 1 THEN (SELECT j.value FROM json_each(ff.subject_granularities) j) ELSE NULL END AS subject_granularity,
    CASE WHEN json_array_length(ff.sexes) = 1 THEN (SELECT j.value FROM json_each(ff.sexes) j) ELSE NULL END AS sex,
    CASE WHEN json_array_length(ff.ethnicities) = 1 THEN (SELECT j.value FROM json_each(ff.ethnicities) j) ELSE NULL END AS ethnicity,
    CASE WHEN json_array_length(ff.anatomies) = 1 THEN (SELECT j.value FROM json_each(ff.anatomies) j) ELSE NULL END AS anatomy,
    CASE WHEN json_array_length(ff.assay_types) = 1 THEN (SELECT j.value FROM json_each(ff.assay_types) j) ELSE NULL END AS assay_type,
    CASE WHEN json_array_length(ff.file_formats) = 1 THEN (SELECT j.value FROM json_each(ff.file_formats) j) ELSE NULL END AS file_format,
    CASE WHEN json_array_length(ff.compression_formats) = 1 THEN (SELECT j.value FROM json_each(ff.compression_formats) j) ELSE NULL END AS compression_format,
    CASE WHEN json_array_length(ff.data_types) = 1 THEN (SELECT j.value FROM json_each(ff.data_types) j) ELSE NULL END AS data_type,
    CASE WHEN json_array_length(ff.mime_types) = 1 THEN (SELECT j.value FROM json_each(ff.mime_types) j) ELSE NULL END AS mime_type,

    ff.diseases,
    ff.substances,
    ff.genes,

    ff.projects,
    ff.dccs,
    ff.subject_roles,
    ff.subject_granularities,
    ff.sexes,
    ff.races,
    ff.ethnicities,
    ff.subject_species,
    ff.ncbi_taxons,
    ff.anatomies,
    ff.assay_types,
    ff.file_formats,
    ff.compression_formats,
    ff.data_types,
    ff.mime_types
  FROM file_facts ff
) ff
JOIN id_namespace n ON (ff.id_namespace = n.nid)
JOIN project p ON (ff.project = p.nid)
LEFT JOIN subject_granularity sg ON (ff.subject_granularity = sg.nid)
LEFT JOIN sex sx ON (ff.sex = sx.nid)
LEFT JOIN ethnicity eth ON (ff.ethnicity = eth.nid)
LEFT JOIN anatomy a ON (ff.anatomy = a.nid)
LEFT JOIN assay_type "at" ON (ff.assay_type = "at".nid)
LEFT JOIN file_format fmt ON (ff.file_format = fmt.nid)
LEFT JOIN file_format cfmt ON (ff.compression_format = cfmt.nid)
LEFT JOIN data_type dt ON (ff.data_type = dt.nid)
LEFT JOIN mime_type mt ON (ff.mime_type = mt.nid)
WHERE True
ON CONFLICT DO NOTHING
;

UPDATE file AS u
SET core_fact = cf.nid
FROM file_facts ff, core_fact cf
WHERE u.nid = ff.nid
  AND ff.id_namespace = cf.id_namespace
  AND ff.is_bundle = cf.is_bundle
  AND ff.projects = cf.projects
  AND ff.dccs = cf.dccs
  AND ff.diseases = cf.diseases
  AND ff.substances = cf.substances
  AND ff.genes = cf.genes
  AND ff.sexes = cf.sexes
  AND ff.races = cf.races
  AND ff.ethnicities = cf.ethnicities
  AND ff.subject_roles = cf.subject_roles
  AND ff.subject_granularities = cf.subject_granularities
  AND ff.subject_species = cf.subject_species
  AND ff.ncbi_taxons = cf.ncbi_taxons
  AND ff.anatomies = cf.anatomies
  AND ff.assay_types = cf.assay_types
  AND ff.file_formats = cf.file_formats
  AND ff.compression_formats = cf.compression_formats
  AND ff.data_types = cf.data_types
  AND ff.mime_types = cf.mime_types
;

CREATE TEMPORARY TABLE biosample_facts AS
  SELECT
    b.nid,
    b.id_namespace,
    False AS is_bundle,
    json_array(b.project) AS projects,
    json_sorted(COALESCE(json_group_array(DISTINCT d.nid) FILTER (WHERE d.nid IS NOT NULL), '[]')) AS dccs,
    json_sorted(COALESCE(json_group_array(DISTINCT dis.nid) FILTER (WHERE dis.nid IS NOT NULL), '[]')) AS diseases,
    json_sorted(COALESCE(json_group_array(DISTINCT subst.nid) FILTER (WHERE subst.nid IS NOT NULL), '[]')) AS substances,
    json_sorted(COALESCE(json_group_array(DISTINCT bg.gene) FILTER (WHERE bg.gene IS NOT NULL), '[]')) AS genes,
    json_sorted(COALESCE(json_group_array(DISTINCT srt."role") FILTER (WHERE srt."role" IS NOT NULL), '[]')) AS subject_roles,
    json_sorted(COALESCE(json_group_array(DISTINCT s.granularity) FILTER (WHERE s.granularity IS NOT NULL), '[]')) AS subject_granularities,
    json_sorted(COALESCE(json_group_array(DISTINCT s.sex) FILTER (WHERE s.sex IS NOT NULL), '[]')) AS sexes,
    json_sorted(COALESCE(json_group_array(DISTINCT sr.race) FILTER (WHERE sr.race IS NOT NULL), '[]')) AS races,
    json_sorted(COALESCE(json_group_array(DISTINCT s.ethnicity) FILTER (WHERE s.ethnicity IS NOT NULL), '[]')) AS ethnicities,
    json_sorted(COALESCE(json_group_array(DISTINCT ss.species) FILTER (WHERE ss.species IS NOT NULL), '[]')) AS subject_species,
    json_sorted(COALESCE(json_group_array(DISTINCT srt.taxon) FILTER (WHERE srt.taxon IS NOT NULL), '[]')) AS ncbi_taxons,
    CASE WHEN b.anatomy IS NOT NULL THEN json_array(b.anatomy) ELSE '[]' END AS anatomies,
    json_sorted(COALESCE(json_group_array(DISTINCT "at".nid) FILTER (WHERE "at".nid IS NOT NULL), '[]')) AS assay_types,
    json_sorted(COALESCE(json_group_array(DISTINCT f.file_format) FILTER (WHERE f.file_format IS NOT NULL), '[]')) AS file_formats,
    json_sorted(COALESCE(json_group_array(DISTINCT f.compression_format) FILTER (WHERE f.compression_format IS NOT NULL), '[]')) AS compression_formats,
    json_sorted(COALESCE(json_group_array(DISTINCT f.data_type) FILTER (WHERE f.data_type IS NOT NULL), '[]')) AS data_types,
    json_sorted(COALESCE(json_group_array(DISTINCT f.mime_type) FILTER (WHERE f.mime_type IS NOT NULL), '[]')) AS mime_types
  FROM biosample b
  JOIN project_in_project_transitive pipt ON (b.project = pipt.member_project)
  JOIN dcc d ON (pipt.leader_project = d.project)
  LEFT JOIN biosample_disease bd ON (b.nid = bd.biosample)
  LEFT JOIN biosample_substance bsubst ON (bsubst.biosample = b.nid)
  LEFT JOIN biosample_gene bg ON (bg.biosample = b.nid)
  LEFT JOIN (
    file_describes_biosample fdb
    JOIN file f ON (fdb.file = f.nid)
  ) ON (b.nid = fdb.biosample)
  LEFT JOIN assay_type "at" ON (b.assay_type = "at".nid OR f.assay_type = "at".nid)
  LEFT JOIN (
    biosample_from_subject bfs
    JOIN subject s ON (bfs.subject = s.nid)
    LEFT JOIN subject_race sr ON (sr.subject = s.nid)
    LEFT JOIN subject_species ss ON (ss.subject = s.nid)
    LEFT JOIN subject_role_taxonomy srt ON (s.nid = srt.subject)
    LEFT JOIN subject_disease sd ON (s.nid = sd.subject)
    LEFT JOIN subject_substance ssubst ON (ssubst.subject = s.nid)
  ) ON (b.nid = bfs.biosample)
  LEFT JOIN disease dis ON (bd.disease = dis.nid OR sd.disease = dis.nid)
  LEFT JOIN substance subst ON (ssubst.substance = subst.nid OR bsubst.substance = subst.nid)
  GROUP BY b.nid
;
CREATE INDEX IF NOT EXISTS biosample_facts_combo_idx ON biosample_facts(
    id_namespace,
    is_bundle,
    projects,
    dccs,
    diseases,
    substances,
    genes,
    subject_roles,
    subject_granularities,
    sexes,
    races,
    ethnicities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    is_bundle,

    project,
    subject_granularity,
    sex,
    ethnicity,
    anatomy,
    assay_type,
    file_format,
    compression_format,
    data_type,
    mime_type,
    
    diseases,
    substances,
    genes,

    projects,
    dccs,
    subject_roles,
    subject_granularities,
    sexes,
    races,
    ethnicities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,
    
    id_namespace_row,
    project_row,
    subject_granularity_row,
    sex_row,
    ethnicity_row,
    anatomy_row,
    assay_type_row,
    file_format_row,
    compression_format_row,
    data_type_row,
    mime_type_row
)
SELECT
  bf.id_namespace,
  is_bundle,

  bf.project,
  subject_granularity,
  sex,
  ethnicity,
  anatomy,
  assay_type,
  file_format,
  compression_format,
  data_type,
  mime_type,
    
  diseases,
  substances,
  genes,

  projects,
  dccs,
  subject_roles,
  subject_granularities,
  sexes,
  races,
  ethnicities,
  subject_species,
  ncbi_taxons,
  anatomies,
  assay_types,
  file_formats,
  compression_formats,
  data_types,
  mime_types,
        
  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
  json_object('nid', "at".nid, 'name', "at".name, 'description', "at".description) AS assay_type_row,
  json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
  json_object('nid', cfmt.nid,'name', cfmt.name, 'description', cfmt.description) AS compression_format_row,
  json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
  json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row
FROM (
  SELECT DISTINCT
    bf.id_namespace,
    bf.is_bundle,

    (SELECT j.value FROM json_each(bf.projects) j) AS project,
    CASE WHEN json_array_length(bf.subject_granularities) = 1 THEN (SELECT j.value FROM json_each(bf.subject_granularities) j) ELSE NULL END AS subject_granularity,
    CASE WHEN json_array_length(bf.sexes) = 1 THEN (SELECT j.value FROM json_each(bf.sexes) j) ELSE NULL END AS sex,
    CASE WHEN json_array_length(bf.ethnicities) = 1 THEN (SELECT j.value FROM json_each(bf.ethnicities) j) ELSE NULL END AS ethnicity,
    CASE WHEN json_array_length(bf.anatomies) = 1 THEN (SELECT j.value FROM json_each(bf.anatomies) j) ELSE NULL END AS anatomy,
    CASE WHEN json_array_length(bf.assay_types) = 1 THEN (SELECT j.value FROM json_each(bf.assay_types) j) ELSE NULL END AS assay_type,
    CASE WHEN json_array_length(bf.file_formats) = 1 THEN (SELECT j.value FROM json_each(bf.file_formats) j) ELSE NULL END AS file_format,
    CASE WHEN json_array_length(bf.compression_formats) = 1 THEN (SELECT j.value FROM json_each(bf.compression_formats) j) ELSE NULL END AS compression_format,
    CASE WHEN json_array_length(bf.data_types) = 1 THEN (SELECT j.value FROM json_each(bf.data_types) j) ELSE NULL END AS data_type,
    CASE WHEN json_array_length(bf.mime_types) = 1 THEN (SELECT j.value FROM json_each(bf.mime_types) j) ELSE NULL END AS mime_type,

    bf.diseases,
    bf.substances,
    bf.genes,

    bf.projects,
    bf.dccs,
    bf.subject_roles,
    bf.subject_granularities,
    bf.sexes,
    bf.races,
    bf.ethnicities,
    bf.subject_species,
    bf.ncbi_taxons,
    bf.anatomies,
    bf.assay_types,
    bf.file_formats,
    bf.compression_formats,
    bf.data_types,
    bf.mime_types
  FROM biosample_facts bf
) bf
JOIN id_namespace n ON (bf.id_namespace = n.nid)
JOIN project p ON (bf.project = p.nid)
LEFT JOIN subject_granularity sg ON (bf.subject_granularity = sg.nid)
LEFT JOIN sex sx ON (bf.sex = sx.nid)
LEFT JOIN ethnicity eth ON (bf.ethnicity = eth.nid)
LEFT JOIN anatomy a ON (bf.anatomy = a.nid)
LEFT JOIN assay_type "at" ON (bf.assay_type = "at".nid)
LEFT JOIN file_format fmt ON (bf.file_format = fmt.nid)
LEFT JOIN file_format cfmt ON (bf.compression_format = cfmt.nid)
LEFT JOIN data_type dt ON (bf.data_type = dt.nid)
LEFT JOIN mime_type mt ON (bf.mime_type = mt.nid)
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE biosample AS u
SET core_fact = cf.nid
FROM biosample_facts bf, core_fact cf
WHERE u.nid = bf.nid
  AND bf.id_namespace = cf.id_namespace
  AND bf.is_bundle = cf.is_bundle
  AND bf.projects = cf.projects
  AND bf.dccs = cf.dccs
  AND bf.diseases = cf.diseases
  AND bf.substances = cf.substances
  AND bf.genes = cf.genes
  AND bf.sexes = cf.sexes
  AND bf.races = cf.races
  AND bf.ethnicities = cf.ethnicities
  AND bf.subject_roles = cf.subject_roles
  AND bf.subject_granularities = cf.subject_granularities
  AND bf.subject_species = cf.subject_species
  AND bf.ncbi_taxons = cf.ncbi_taxons
  AND bf.anatomies = cf.anatomies
  AND bf.assay_types = cf.assay_types
  AND bf.file_formats = cf.file_formats
  AND bf.compression_formats = cf.compression_formats
  AND bf.data_types = cf.data_types
  AND bf.mime_types = cf.mime_types
;

CREATE TEMPORARY TABLE subject_facts AS
  SELECT
    s.nid,
    s.id_namespace,
    False AS is_bundle,
    json_array(s.project) AS projects,
    json_sorted(COALESCE(json_group_array(DISTINCT d.nid) FILTER (WHERE d.nid IS NOT NULL), '[]')) AS dccs,
    json_sorted(COALESCE(json_group_array(DISTINCT dis.nid) FILTER (WHERE dis.nid IS NOT NULL), '[]')) AS diseases,
    json_sorted(COALESCE(json_group_array(DISTINCT subst.nid) FILTER (WHERE subst.nid IS NOT NULL), '[]')) AS substances,
    json_sorted(COALESCE(json_group_array(DISTINCT srt."role") FILTER (WHERE srt."role" IS NOT NULL), '[]')) AS subject_roles,
    CASE WHEN s.granularity IS NOT NULL THEN json_array(s.granularity) ELSE '[]' END AS subject_granularities,
    CASE WHEN s.sex IS NOT NULL THEN json_array(s.sex) ELSE '[]' END AS sexes,
    CASE WHEN sr.race IS NOT NULL THEN json_array(sr.race) ELSE '[]' END AS races,
    CASE WHEN s.ethnicity IS NOT NULL THEN json_array(s.ethnicity) ELSE '[]' END AS ethnicities,
    json_sorted(COALESCE(json_group_array(DISTINCT ss.species) FILTER (WHERE ss.species IS NOT NULL), '[]')) AS subject_species,
    json_sorted(COALESCE(json_group_array(DISTINCT srt.taxon) FILTER (WHERE srt.taxon IS NOT NULL), '[]')) AS ncbi_taxons,
    json_sorted(COALESCE(json_group_array(DISTINCT bg.gene) FILTER (WHERE bg.gene IS NOT NULL), '[]')) AS genes,
    json_sorted(COALESCE(json_group_array(DISTINCT b.anatomy) FILTER (WHERE b.anatomy IS NOT NULL), '[]')) AS anatomies,
    json_sorted(COALESCE(json_group_array(DISTINCT "at".nid) FILTER (WHERE "at".nid IS NOT NULL), '[]')) AS assay_types,
    json_sorted(COALESCE(json_group_array(DISTINCT f.file_format) FILTER (WHERE f.file_format IS NOT NULL), '[]')) AS file_formats,
    json_sorted(COALESCE(json_group_array(DISTINCT f.compression_format) FILTER (WHERE f.compression_format IS NOT NULL), '[]')) AS compression_formats,
    json_sorted(COALESCE(json_group_array(DISTINCT f.data_type) FILTER (WHERE f.data_type IS NOT NULL), '[]')) AS data_types,
    json_sorted(COALESCE(json_group_array(DISTINCT f.mime_type) FILTER (WHERE f.mime_type IS NOT NULL), '[]')) AS mime_types
  FROM subject s
  JOIN project_in_project_transitive pipt ON (s.project = pipt.member_project)
  JOIN dcc d ON (pipt.leader_project = d.project)
  LEFT JOIN subject_race sr ON (sr.subject = s.nid)
  LEFT JOIN subject_species ss ON (ss.subject = s.nid)
  LEFT JOIN subject_role_taxonomy srt ON (s.nid = srt.subject)
  LEFT JOIN subject_disease sd ON (sd.subject = s.nid)
  LEFT JOIN subject_substance ssubst ON (ssubst.subject = s.nid)
  LEFT JOIN (
    biosample_from_subject bfs
    JOIN biosample b ON (bfs.biosample = b.nid)
    LEFT JOIN biosample_disease bd ON (bd.biosample = b.nid)
    LEFT JOIN biosample_substance bsubst ON (bsubst.biosample = b.nid)
    LEFT JOIN biosample_gene bg ON (bg.biosample = b.nid)
    LEFT JOIN (
      file_describes_biosample fdb
      JOIN file f ON (fdb.file = f.nid)
    ) ON (b.nid = fdb.biosample)
    LEFT JOIN assay_type "at" ON (b.assay_type = "at".nid OR f.assay_type = "at".nid)
  ) ON (s.nid = bfs.subject)
  LEFT JOIN disease dis ON (sd.disease = dis.nid OR bd.disease = dis.nid)
  LEFT JOIN substance subst ON (ssubst.substance = subst.nid OR bsubst.substance = subst.nid)
  GROUP BY s.nid, s.id_namespace
;
CREATE INDEX IF NOT EXISTS subject_facts_combo_idx ON subject_facts(
    id_namespace,
    is_bundle,
    projects,
    dccs,
    diseases,
    substances,
    genes,
    subject_roles,
    subject_granularities,
    sexes,
    races,
    ethnicities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    is_bundle,

    project,
    subject_granularity,
    sex,
    ethnicity,
    anatomy,
    assay_type,
    file_format,
    compression_format,
    data_type,
    mime_type,
    
    diseases,
    substances,
    genes,

    projects,
    dccs,
    subject_roles,
    subject_granularities,
    sexes,
    races,
    ethnicities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,
    
    id_namespace_row,
    project_row,
    subject_granularity_row,
    sex_row,
    ethnicity_row,
    anatomy_row,
    assay_type_row,
    file_format_row,
    compression_format_row,
    data_type_row,
    mime_type_row)
SELECT
  sf.id_namespace,
  is_bundle,

  sf.project,
  subject_granularity,
  sex,
  ethnicity,
  anatomy,
  assay_type,
  file_format,
  compression_format,
  data_type,
  mime_type,
    
  diseases,
  substances,
  genes,

  projects,
  dccs,
  subject_roles,
  subject_granularities,
  sexes,
  races,
  ethnicities,
  subject_species,
  ncbi_taxons,
  anatomies,
  assay_types,
  file_formats,
  compression_formats,
  data_types,
  mime_types,
        
  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
  json_object('nid', "at".nid, 'name', "at".name, 'description', "at".description) AS assay_type_row,
  json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
  json_object('nid', cfmt.nid,'name', cfmt.name, 'description', cfmt.description) AS compression_format_row,
  json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
  json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row
FROM (
  SELECT DISTINCT
    sf.id_namespace,
    sf.is_bundle,

    (SELECT j.value FROM json_each(sf.projects) j) AS project,
    CASE WHEN json_array_length(sf.subject_granularities) = 1 THEN (SELECT j.value FROM json_each(sf.subject_granularities) j) ELSE NULL END AS subject_granularity,
    CASE WHEN json_array_length(sf.sexes) = 1 THEN (SELECT j.value FROM json_each(sf.sexes) j) ELSE NULL END AS sex,
    CASE WHEN json_array_length(sf.ethnicities) = 1 THEN (SELECT j.value FROM json_each(sf.ethnicities) j) ELSE NULL END AS ethnicity,
    CASE WHEN json_array_length(sf.anatomies) = 1 THEN (SELECT j.value FROM json_each(sf.anatomies) j) ELSE NULL END AS anatomy,
    CASE WHEN json_array_length(sf.assay_types) = 1 THEN (SELECT j.value FROM json_each(sf.assay_types) j) ELSE NULL END AS assay_type,
    CASE WHEN json_array_length(sf.file_formats) = 1 THEN (SELECT j.value FROM json_each(sf.file_formats) j) ELSE NULL END AS file_format,
    CASE WHEN json_array_length(sf.compression_formats) = 1 THEN (SELECT j.value FROM json_each(sf.compression_formats) j) ELSE NULL END AS compression_format,
    CASE WHEN json_array_length(sf.data_types) = 1 THEN (SELECT j.value FROM json_each(sf.data_types) j) ELSE NULL END AS data_type,
    CASE WHEN json_array_length(sf.mime_types) = 1 THEN (SELECT j.value FROM json_each(sf.mime_types) j) ELSE NULL END AS mime_type,

    sf.diseases,
    sf.substances,
    sf.genes,

    sf.projects,
    sf.dccs,
    sf.subject_roles,
    sf.subject_granularities,
    sf.sexes,
    sf.races,
    sf.ethnicities,
    sf.subject_species,
    sf.ncbi_taxons,
    sf.anatomies,
    sf.assay_types,
    sf.file_formats,
    sf.compression_formats,
    sf.data_types,
    sf.mime_types
  FROM subject_facts sf
) sf
JOIN id_namespace n ON (sf.id_namespace = n.nid)
JOIN project p ON (sf.project = p.nid)
LEFT JOIN subject_granularity sg ON (sf.subject_granularity = sg.nid)
LEFT JOIN sex sx ON (sf.sex = sx.nid)
LEFT JOIN ethnicity eth ON (sf.ethnicity = eth.nid)
LEFT JOIN anatomy a ON (sf.anatomy = a.nid)
LEFT JOIN assay_type "at" ON (sf.assay_type = "at".nid)
LEFT JOIN file_format fmt ON (sf.file_format = fmt.nid)
LEFT JOIN file_format cfmt ON (sf.compression_format = cfmt.nid)
LEFT JOIN data_type dt ON (sf.data_type = dt.nid)
LEFT JOIN mime_type mt ON (sf.mime_type = mt.nid)
WHERE True
ON CONFLICT DO NOTHING
;

UPDATE subject AS u
SET core_fact = cf.nid
FROM subject_facts sf, core_fact cf
WHERE u.nid = sf.nid
  AND sf.id_namespace = cf.id_namespace
  AND sf.is_bundle = cf.is_bundle
  AND sf.projects = cf.projects
  AND sf.dccs = cf.dccs
  AND sf.diseases = cf.diseases
  AND sf.substances = cf.substances
  AND sf.genes = cf.genes
  AND sf.sexes = cf.sexes
  AND sf.races = cf.races
  AND sf.ethnicities = cf.ethnicities
  AND sf.subject_roles = cf.subject_roles
  AND sf.subject_granularities = cf.subject_granularities
  AND sf.subject_species = cf.subject_species
  AND sf.ncbi_taxons = cf.ncbi_taxons
  AND sf.anatomies = cf.anatomies
  AND sf.assay_types = cf.assay_types
  AND sf.file_formats = cf.file_formats
  AND sf.compression_formats = cf.compression_formats
  AND sf.data_types = cf.data_types
  AND sf.mime_types = cf.mime_types
;

CREATE TEMPORARY TABLE collection_facts AS
SELECT
  nid,
  id_namespace,
  is_bundle,
  projects,
  dccs,
  diseases,
  substances,
  genes,
  sexes,
  races,
  ethnicities,
  subject_roles,
  subject_granularities,
  subject_species,
  ncbi_taxons,
  anatomies,
  assay_types,
  file_formats,
  compression_formats,
  data_types,
  mime_types
FROM (
  SELECT
    col.nid,
    col.id_namespace,
    False AS is_bundle,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT cdbp.project) FILTER (WHERE cdbp.project IS NOT NULL)), '[]')
     FROM collection_defined_by_project cdbp
     WHERE cdbp.collection = col.nid) AS projects,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT d.nid) FILTER (WHERE d.nid IS NOT NULL)), '[]')
     FROM collection_defined_by_project cdbp
     JOIN project_in_project_transitive pipt ON (cdbp.project = pipt.member_project)
     JOIN dcc d ON (pipt.leader_project = d.project)
     WHERE cdbp.collection = col.nid) AS dccs,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.diseases) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.diseases) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.diseases) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS diseases,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.substances) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.substances) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.substances) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS substances,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.genes) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.genes) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.genes) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS genes,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.sexes) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.sexes) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.sexes) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS sexes,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.races) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.races) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.races) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS races,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.ethnicities) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.ethnicities) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.ethnicities) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS ethnicities,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.subject_roles) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.subject_roles) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.subject_roles) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS subject_roles,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.subject_granularities) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.subject_granularities) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.subject_granularities) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS subject_granularities,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.subject_species) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.subject_species) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.subject_species) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS subject_species,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.ncbi_taxons) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.ncbi_taxons) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.ncbi_taxons) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS ncbi_taxons,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.anatomies) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.anatomies) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.anatomies) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS anatomies,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.assay_types) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.assay_types) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.assay_types) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS assay_types,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.file_formats) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.file_formats) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.file_formats) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS file_formats,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.compression_formats) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.compression_formats) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.compression_formats) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS compression_formats,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.data_types) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.data_types) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.data_types) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS data_types,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value) FILTER (WHERE s.value IS NOT NULL)), '[]')
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_fact cf, json_each(cf.mime_types) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.mime_types) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.mime_types) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s) AS mime_types
  FROM collection col
) s
;
CREATE INDEX IF NOT EXISTS collection_facts_combo_idx ON collection_facts(
    id_namespace,
    is_bundle,
    projects,
    dccs,
    diseases,
    substances,
    genes,
    subject_roles,
    subject_granularities,
    sexes,
    races,
    ethnicities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    is_bundle,

    project,
    subject_granularity,
    sex,
    ethnicity,
    anatomy,
    assay_type,
    file_format,
    compression_format,
    data_type,
    mime_type,
    
    diseases,
    substances,
    genes,

    projects,
    dccs,
    subject_roles,
    subject_granularities,
    sexes,
    races,
    ethnicities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,
    
    id_namespace_row,
    project_row,
    subject_granularity_row,
    sex_row,
    ethnicity_row,
    anatomy_row,
    assay_type_row,
    file_format_row,
    compression_format_row,
    data_type_row,
    mime_type_row
)
SELECT
  colf.id_namespace,
  is_bundle,

  colf.project,
  subject_granularity,
  sex,
  ethnicity,
  anatomy,
  assay_type,
  file_format,
  compression_format,
  data_type,
  mime_type,
    
  diseases,
  substances,
  genes,

  projects,
  dccs,
  subject_roles,
  subject_granularities,
  sexes,
  races,
  ethnicities,
  subject_species,
  ncbi_taxons,
  anatomies,
  assay_types,
  file_formats,
  compression_formats,
  data_types,
  mime_types,
        
  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
  json_object('nid', "at".nid, 'name', "at".name, 'description', "at".description) AS assay_type_row,
  json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
  json_object('nid', cfmt.nid,'name', cfmt.name, 'description', cfmt.description) AS compression_format_row,
  json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
  json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row
FROM (
  SELECT DISTINCT
    colf.id_namespace,
    colf.is_bundle,

    (SELECT j.value FROM json_each(colf.projects) j) AS project,
    CASE WHEN json_array_length(colf.subject_granularities) = 1 THEN (SELECT j.value FROM json_each(colf.subject_granularities) j) ELSE NULL END AS subject_granularity,
    CASE WHEN json_array_length(colf.sexes) = 1 THEN (SELECT j.value FROM json_each(colf.sexes) j) ELSE NULL END AS sex,
    CASE WHEN json_array_length(colf.ethnicities) = 1 THEN (SELECT j.value FROM json_each(colf.ethnicities) j) ELSE NULL END AS ethnicity,
    CASE WHEN json_array_length(colf.anatomies) = 1 THEN (SELECT j.value FROM json_each(colf.anatomies) j) ELSE NULL END AS anatomy,
    CASE WHEN json_array_length(colf.assay_types) = 1 THEN (SELECT j.value FROM json_each(colf.assay_types) j) ELSE NULL END AS assay_type,
    CASE WHEN json_array_length(colf.file_formats) = 1 THEN (SELECT j.value FROM json_each(colf.file_formats) j) ELSE NULL END AS file_format,
    CASE WHEN json_array_length(colf.compression_formats) = 1 THEN (SELECT j.value FROM json_each(colf.compression_formats) j) ELSE NULL END AS compression_format,
    CASE WHEN json_array_length(colf.data_types) = 1 THEN (SELECT j.value FROM json_each(colf.data_types) j) ELSE NULL END AS data_type,
    CASE WHEN json_array_length(colf.mime_types) = 1 THEN (SELECT j.value FROM json_each(colf.mime_types) j) ELSE NULL END AS mime_type,

    colf.diseases,
    colf.substances,
    colf.genes,

    colf.projects,
    colf.dccs,
    colf.subject_roles,
    colf.subject_granularities,
    colf.sexes,
    colf.races,
    colf.ethnicities,
    colf.subject_species,
    colf.ncbi_taxons,
    colf.anatomies,
    colf.assay_types,
    colf.file_formats,
    colf.compression_formats,
    colf.data_types,
    colf.mime_types
  FROM collection_facts colf
) colf
JOIN id_namespace n ON (colf.id_namespace = n.nid)
LEFT JOIN project p ON (colf.project = p.nid)
LEFT JOIN subject_granularity sg ON (colf.subject_granularity = sg.nid)
LEFT JOIN sex sx ON (colf.sex = sx.nid)
LEFT JOIN ethnicity eth ON (colf.ethnicity = eth.nid)
LEFT JOIN anatomy a ON (colf.anatomy = a.nid)
LEFT JOIN assay_type "at" ON (colf.assay_type = "at".nid)
LEFT JOIN file_format fmt ON (colf.file_format = fmt.nid)
LEFT JOIN file_format cfmt ON (colf.compression_format = cfmt.nid)
LEFT JOIN data_type dt ON (colf.data_type = dt.nid)
LEFT JOIN mime_type mt ON (colf.mime_type = mt.nid)
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE collection AS u
SET core_fact = cf.nid
FROM collection_facts colf, core_fact cf
WHERE u.nid = colf.nid
  AND colf.id_namespace = cf.id_namespace
  AND colf.is_bundle = cf.is_bundle
  AND colf.projects = cf.projects
  AND colf.dccs = cf.dccs
  AND colf.diseases = cf.diseases
  AND colf.substances = cf.substances
  AND colf.genes = cf.genes
  AND colf.sexes = cf.sexes
  AND colf.races = cf.races
  AND colf.ethnicities = cf.ethnicities
  AND colf.subject_roles = cf.subject_roles
  AND colf.subject_granularities = cf.subject_granularities
  AND colf.subject_species = cf.subject_species
  AND colf.ncbi_taxons = cf.ncbi_taxons
  AND colf.anatomies = cf.anatomies
  AND colf.assay_types = cf.assay_types
  AND colf.file_formats = cf.file_formats
  AND colf.compression_formats = cf.compression_formats
  AND colf.data_types = cf.data_types
  AND colf.mime_types = cf.mime_types
;
