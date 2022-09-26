CREATE TEMPORARY TABLE file_facts AS
SELECT
  f.nid,

  f.id_namespace,
  f.bundle_collection IS NOT NULL AS is_bundle,
  f.persistent_id IS NOT NULL AS has_persistent_id,

  COALESCE(f.dbgap_study_id, -1) AS dbgap_study_id,
  COALESCE(f.project, -1) AS project,
  -1 AS sex,
  -1 AS ethnicity,
  -1 AS subject_granularity,
  -1 AS anatomy,
  -1 AS sample_prep_method,
  COALESCE(f.assay_type, -1) AS assay_type,
  COALESCE(f.analysis_type, -1) AS analysis_type,
  COALESCE(f.file_format, -1) AS file_format,
  COALESCE(f.compression_format, -1) AS compression_format,
  COALESCE(f.data_type, -1) AS data_type,
  COALESCE(f.mime_type, -1) AS mime_type,

  CASE WHEN f.dbgap_study_id IS NOT NULL THEN json_array(f.dbgap_study_id) ELSE '[]' END AS dbgap_study_ids,
  json_array(f.project) AS projects,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT dcc.nid))
      FROM project_in_project_transitive pipt
      JOIN dcc ON (pipt.leader_project = dcc.project)
      WHERE pipt.member_project = f.project
    ),
    '[]'
  ) AS dccs,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT json_array(a.phenotype, a.association_type)))
      FROM (
        SELECT a.phenotype, a.association_type FROM file_describes_subject fds JOIN subject_phenotype a ON (fds.subject = a.subject) WHERE fds.file = f.nid
        UNION
        SELECT a.phenotype, 0 AS association_type FROM file_in_collection fic JOIN collection_phenotype a ON (fic.collection = a.collection) WHERE fic.file = f.nid
      ) a
    ),
    '[]'
  ) AS phenotypes,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT json_array(a.slim_term, a.association_type)))
      FROM (
        SELECT sl.slim_term, a.association_type FROM file_describes_subject fds JOIN subject_disease a ON (fds.subject = a.subject) JOIN disease_slim_union sl ON (a.disease = sl.original_term) WHERE fds.file = f.nid
        UNION
        SELECT sl.slim_term, a.association_type FROM file_describes_biosample fdb JOIN biosample_disease a ON (fdb.biosample = a.biosample) JOIN disease_slim_union sl ON (a.disease = sl.original_term) WHERE fdb.file = f.nid
        UNION
        SELECT sl.slim_term, 0 AS association_type FROM file_in_collection fic JOIN collection_disease a ON (fic.collection = a.collection) JOIN disease_slim_union sl ON (a.disease = sl.original_term) WHERE fic.file = f.nid
      ) a
    ),
    '[]'
  ) AS diseases,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.sex)) FROM file_describes_subject fds JOIN subject s ON (fds.subject = s.nid) WHERE fds.file = f.nid AND s.sex IS NOT NULL
    ),
    '[]'
  ) AS sexes,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.race)) FROM file_describes_subject fds JOIN subject_race a ON (fds.subject = a.subject) WHERE fds.file = f.nid
    ),
    '[]'
  ) AS races,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.ethnicity)) FROM file_describes_subject fds JOIN subject s ON (fds.subject = s.nid) WHERE fds.file = f.nid AND s.ethnicity IS NOT NULL
    ),
    '[]'
  ) AS ethnicities,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a."role")) FROM file_describes_subject fds JOIN subject_role_taxonomy a ON (fds.subject = a.subject) WHERE fds.file = f.nid
    ),
    '[]'
  ) AS subject_roles,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.granularity)) FROM file_describes_subject fds JOIN subject s ON (fds.subject = s.nid) WHERE fds.file = f.nid AND s.granularity IS NOT NULL
    ),
    '[]'
  ) AS subject_granularities,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.species)) FROM file_describes_subject fds JOIN subject_species a ON (fds.subject = a.subject) WHERE fds.file = f.nid
    ),
    '[]'
  ) AS subject_species,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM file_describes_subject fds JOIN subject_role_taxonomy a ON (fds.subject = a.subject) JOIN ncbi_taxonomy_slim_union sl ON (a.taxon = sl.original_term) WHERE fds.file = f.nid
    ),
    '[]'
  ) AS ncbi_taxons,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM file_describes_biosample fdb JOIN biosample b ON (fdb.biosample = b.nid) JOIN anatomy_slim_union sl ON (b.anatomy = sl.original_term) WHERE fdb.file = f.nid
    ),
    '[]'
  ) AS anatomies,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT b.sample_prep_method)) FROM file_describes_biosample fdb JOIN biosample b ON (fdb.biosample = b.nid) WHERE fdb.file = f.nid AND b.sample_prep_method IS NOT NULL
    ),
    '[]'
  ) AS sample_prep_methods,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM assay_type_slim_union sl WHERE sl.original_term = f.assay_type
    ),
    '[]'
  ) AS assay_types,

  CASE WHEN f.analysis_type IS NOT NULL THEN json_array(f.analysis_type) ELSE '[]' END AS analysis_types,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM file_format_slim_union sl WHERE sl.original_term = f.file_format
    ),
    '[]'
  ) AS file_formats,
  CASE WHEN f.compression_format IS NOT NULL THEN json_array(f.compression_format) ELSE '[]' END AS compression_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM data_type_slim_union sl WHERE sl.original_term = f.data_type
    ),
    '[]'
  ) AS data_types,
  CASE WHEN f.mime_type   IS NOT NULL THEN json_array(f.mime_type)   ELSE '[]' END AS mime_types
FROM file f;
CREATE INDEX IF NOT EXISTS file_facts_combo_idx ON file_facts(
    id_namespace,
    is_bundle,
    has_persistent_id,

    dbgap_study_id,
    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    sample_prep_method,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

    dbgap_study_ids,
    projects,
    dccs,
    phenotypes,
    diseases,
    sexes,
    races,
    ethnicities,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    sample_prep_methods,
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    is_bundle,
    has_persistent_id,

    dbgap_study_id,
    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    sample_prep_method,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

    dbgap_study_ids,
    projects,
    dccs,
    phenotypes,
    diseases,
    sexes,
    races,
    ethnicities,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    sample_prep_methods,
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,
    
    phenotypes_flat,
    diseases_flat,

    id_namespace_row,
    dbgap_study_id_row,
    project_row,
    sex_row,
    ethnicity_row,
    subject_granularity_row,
    anatomy_row,
    sample_prep_method_row,
    assay_type_row,
    analysis_type_row,
    file_format_row,
    compression_format_row,
    data_type_row,
    mime_type_row
)
SELECT
  ff.id_namespace,
  ff.is_bundle,
  ff.has_persistent_id,

  ff.dbgap_study_id,
  ff.project,
  ff.sex,
  ff.ethnicity,
  ff.subject_granularity,
  ff.anatomy,
  ff.sample_prep_method,
  ff.assay_type,
  ff.analysis_type,
  ff.file_format,
  ff.compression_format,
  ff.data_type,
  ff.mime_type,

  ff.dbgap_study_ids,
  ff.projects,
  ff.dccs,
  ff.phenotypes,
  ff.diseases,
  ff.sexes,
  ff.races,
  ff.ethnicities,
  ff.subject_roles,
  ff.subject_granularities,
  ff.subject_species,
  ff.ncbi_taxons,
  ff.anatomies,
  ff.sample_prep_methods,
  ff.assay_types,

  ff.analysis_types,
  ff.file_formats,
  ff.compression_formats,
  ff.data_types,
  ff.mime_types,

  (SELECT json_sorted(json_group_array(DISTINCT json_extract(j.value, '$[0]')))
   FROM json_each(ff.phenotypes) j) AS phenotypes_flat,
  (SELECT json_sorted(json_group_array(DISTINCT json_extract(j.value, '$[0]')))
   FROM json_each(ff.diseases) j) AS diseases_flat,

  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', dbg.nid, 'id', dbg.id, 'name', dbg.name, 'description', dbg.description) AS dbgap_study_id_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
  json_object('nid', spm.nid, 'name', spm.name, 'description', spm.description) AS sample_prep_method_row,
  json_object('nid', ast.nid, 'name', ast.name, 'description', ast.description) AS assay_type_row,
  json_object('nid', ant.nid, 'name', ant.name, 'description', ant.description) AS analysis_type_row,
  json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
  json_object('nid', cfmt.nid,'name', cfmt.name, 'description', cfmt.description) AS compression_format_row,
  json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
  json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row
FROM (
  SELECT DISTINCT
    ff.id_namespace,
    ff.is_bundle,
    ff.has_persistent_id,

    ff.dbgap_study_id,
    ff.project,
    ff.sex,
    ff.ethnicity,
    ff.subject_granularity,
    ff.anatomy,
    ff.sample_prep_method,
    ff.assay_type,
    ff.analysis_type,
    ff.file_format,
    ff.compression_format,
    ff.data_type,
    ff.mime_type,

    ff.dbgap_study_ids,
    ff.projects,
    ff.dccs,
    ff.phenotypes,
    ff.diseases,
    ff.sexes,
    ff.races,
    ff.ethnicities,
    ff.subject_roles,
    ff.subject_granularities,
    ff.subject_species,
    ff.ncbi_taxons,
    ff.anatomies,
    ff.sample_prep_methods,
    ff.assay_types,

    ff.analysis_types,
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
LEFT JOIN sample_prep_method spm ON (ff.sample_prep_method = spm.nid)
LEFT JOIN assay_type ast ON (ff.assay_type = ast.nid)
LEFT JOIN analysis_type ant ON (ff.analysis_type = ant.nid)
LEFT JOIN file_format fmt ON (ff.file_format = fmt.nid)
LEFT JOIN file_format cfmt ON (ff.compression_format = cfmt.nid)
LEFT JOIN data_type dt ON (ff.data_type = dt.nid)
LEFT JOIN mime_type mt ON (ff.mime_type = mt.nid)
LEFT JOIN dbgap_study_id dbg ON (ff.dbgap_study_id = dbg.nid)
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE file AS u
SET core_fact = cf.nid,
    is_bundle = cf.is_bundle,
    has_persistent_id = cf.has_persistent_id,
    projects = cf.projects,
    dccs = cf.dccs,
    phenotypes_flat = cf.phenotypes_flat,
    diseases_flat = cf.diseases_flat,
    sexes = cf.sexes,
    races = cf.races,
    ethnicities = cf.ethnicities,
    subject_roles = cf.subject_roles,
    subject_granularities = cf.subject_granularities,
    subject_species = cf.subject_species,
    ncbi_taxons = cf.ncbi_taxons,
    anatomies = cf.anatomies,
    sample_prep_methods = cf.sample_prep_methods,
    assay_types = cf.assay_types,
    analysis_types = cf.analysis_types,
    file_formats = cf.file_formats,
    compression_formats = cf.compression_formats,
    data_types = cf.data_types,
    mime_types = cf.mime_types,
    dbgap_study_ids = cf.dbgap_study_ids
FROM file_facts ff, core_fact cf
WHERE u.nid = ff.nid
  AND ff.id_namespace = cf.id_namespace
  AND ff.is_bundle = cf.is_bundle
  AND ff.has_persistent_id = cf.has_persistent_id

  AND ff.dbgap_study_id = cf.dbgap_study_id
  AND ff.project = cf.project
  AND ff.sex = cf.sex
  AND ff.ethnicity = cf.ethnicity
  AND ff.subject_granularity = cf.subject_granularity
  AND ff.anatomy = cf.anatomy
  AND ff.sample_prep_method = cf.sample_prep_method
  AND ff.assay_type = cf.assay_type
  AND ff.analysis_type = cf.analysis_type
  AND ff.file_format = cf.file_format
  AND ff.compression_format = cf.compression_format
  AND ff.data_type = cf.data_type
  AND ff.mime_type = cf.mime_type

  AND ff.dbgap_study_ids = cf.dbgap_study_ids
  AND ff.projects = cf.projects
  AND ff.dccs = cf.dccs
  AND ff.phenotypes = cf.phenotypes
  AND ff.diseases = cf.diseases
  AND ff.sexes = cf.sexes
  AND ff.races = cf.races
  AND ff.ethnicities = cf.ethnicities
  AND ff.subject_roles = cf.subject_roles
  AND ff.subject_granularities = cf.subject_granularities
  AND ff.subject_species = cf.subject_species
  AND ff.ncbi_taxons = cf.ncbi_taxons
  AND ff.anatomies = cf.anatomies
  AND ff.sample_prep_methods = cf.sample_prep_methods
  AND ff.assay_types = cf.assay_types

  AND ff.analysis_types = cf.analysis_types
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
  b.persistent_id IS NOT NULL AS has_persistent_id,

  -1 AS dbgap_study_id,
  COALESCE(b.project, -1) AS project,
  -1 AS sex,
  -1 AS ethnicity,
  -1 AS subject_granularity,
  COALESCE(b.anatomy, -1) AS anatomy,
  COALESCE(b.sample_prep_method, -1) AS sample_prep_method,
  -1 AS assay_type,
  -1 AS analysis_type,
  -1 AS file_format,
  -1 AS compression_format,
  -1 AS data_type,
  -1 AS mime_type,

  '[]' AS dbgap_study_ids,
  json_array(b.project) AS projects,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT dcc.nid))
      FROM project_in_project_transitive pipt
      JOIN dcc ON (pipt.leader_project = dcc.project)
      WHERE pipt.member_project = b.project
    ),
    '[]'
  ) AS dccs,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT json_array(a.phenotype, a.association_type)))
      FROM (
        SELECT a.phenotype, a.association_type FROM biosample_from_subject bfs JOIN subject_phenotype a ON (bfs.subject = a.subject) WHERE bfs.biosample = b.nid
        UNION
        SELECT a.phenotype, 0 AS association_type FROM biosample_in_collection bic JOIN collection_phenotype a ON (bic.collection = a.collection) WHERE bic.biosample = b.nid
      ) a
    ),
    '[]'
  ) AS phenotypes,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT json_array(a.slim_term, a.association_type)))
      FROM (
        SELECT sl.slim_term, a.association_type FROM biosample_from_subject bfs JOIN subject_disease a ON (bfs.subject = a.subject) JOIN disease_slim_union sl ON (a.disease = sl.original_term) WHERE bfs.biosample = b.nid
        UNION
        SELECT sl.slim_term, a.association_type FROM biosample_disease a JOIN disease_slim_union sl ON (a.disease = sl.original_term) WHERE a.biosample = b.nid
        UNION
        SELECT sl.slim_term, 0 AS association_type FROM biosample_in_collection bic JOIN collection_disease a ON (bic.collection = a.collection) JOIN disease_slim_union sl ON (a.disease = sl.original_term) WHERE bic.biosample = b.nid
      ) a
    ),
    '[]'
  ) AS diseases,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.sex)) FROM biosample_from_subject bfs JOIN subject s ON (bfs.subject = s.nid) WHERE bfs.biosample = b.nid AND s.sex IS NOT NULL
    ),
    '[]'
  ) AS sexes,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.race)) FROM biosample_from_subject bfs JOIN subject_race a ON (bfs.subject = a.subject) WHERE bfs.biosample = b.nid
    ),
    '[]'
  ) AS races,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.ethnicity)) FROM biosample_from_subject bfs JOIN subject s ON (bfs.subject = s.nid) WHERE bfs.biosample = b.nid AND s.ethnicity IS NOT NULL
    ),
    '[]'
  ) AS ethnicities,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a."role")) FROM biosample_from_subject bfs JOIN subject_role_taxonomy a ON (bfs.subject = a.subject) WHERE bfs.biosample = b.nid
    ),
    '[]'
  ) AS subject_roles,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.granularity)) FROM biosample_from_subject bfs JOIN subject s ON (bfs.subject = s.nid) WHERE bfs.biosample = b.nid AND s.granularity IS NOT NULL
    ),
    '[]'
  ) AS subject_granularities,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.species)) FROM biosample_from_subject bfs JOIN subject_species a ON (bfs.subject = a.subject) WHERE bfs.biosample = b.nid
    ),
    '[]'
  ) AS subject_species,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM biosample_from_subject bfs JOIN subject_role_taxonomy a ON (bfs.subject = a.subject) JOIN ncbi_taxonomy_slim_union sl ON (a.taxon = sl.original_term) WHERE bfs.biosample = b.nid
    ),
    '[]'
  ) AS ncbi_taxons,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM anatomy_slim_union sl WHERE sl.original_term = b.anatomy
    ),
    '[]'
  ) AS anatomies,
  CASE WHEN b.sample_prep_method IS NOT NULL THEN json_array(b.sample_prep_method) ELSE '[]' END AS sample_prep_methods,  
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.slim_term))
      FROM (
        SELECT sl.slim_term FROM file_describes_biosample fdb JOIN file f ON (fdb.file = f.nid) JOIN assay_type_slim_union sl ON (f.assay_type = sl.original_term) WHERE fdb.biosample = b.nid
      ) a
    ),
    '[]'
  ) AS assay_types,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT f.analysis_type)) FROM file_describes_biosample fdb JOIN file f ON (fdb.file = f.nid) WHERE fdb.biosample = b.nid AND f.analysis_type IS NOT NULL
    ),
    '[]'
  ) AS analysis_types,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM file_describes_biosample fdb JOIN file f ON (fdb.file = f.nid) JOIN file_format_slim_union sl ON (sl.original_term = f.file_format) WHERE fdb.biosample = b.nid
    ),
    '[]'
  ) AS file_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT f.compression_format)) FROM file_describes_biosample fdb JOIN file f ON (fdb.file = f.nid) WHERE fdb.biosample = b.nid AND f.compression_format IS NOT NULL
    ),
    '[]'
  ) AS compression_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM file_describes_biosample fdb JOIN file f ON (fdb.file = f.nid) JOIN data_type_slim_union sl ON (sl.original_term = f.data_type) WHERE fdb.biosample = b.nid
    ),
    '[]'
  ) AS data_types,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT f.mime_type)) FROM file_describes_biosample fdb JOIN file f ON (fdb.file = f.nid) WHERE fdb.biosample = b.nid AND f.mime_type IS NOT NULL
    ),
    '[]'
  ) AS mime_types
FROM biosample b;
CREATE INDEX IF NOT EXISTS biosample_facts_combo_idx ON biosample_facts(
    id_namespace,
    is_bundle,
    has_persistent_id,

    dbgap_study_id,
    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    sample_prep_method,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

    dbgap_study_ids,
    projects,
    dccs,
    phenotypes,
    diseases,
    sexes,
    races,
    ethnicities,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    sample_prep_methods,
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    is_bundle,
    has_persistent_id,

    dbgap_study_id,
    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    sample_prep_method,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

    dbgap_study_ids,
    projects,
    dccs,
    phenotypes,
    diseases,
    sexes,
    races,
    ethnicities,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    sample_prep_methods,
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,

    phenotypes_flat,
    diseases_flat,

    id_namespace_row,
    project_row,
    sex_row,
    ethnicity_row,
    subject_granularity_row,
    anatomy_row,
    sample_prep_method_row,
    assay_type_row,
    analysis_type_row,
    file_format_row,
    compression_format_row,
    data_type_row,
    mime_type_row
)
SELECT
  bf.id_namespace,
  bf.is_bundle,
  bf.has_persistent_id,

  bf.dbgap_study_id,
  bf.project,
  bf.sex,
  bf.ethnicity,
  bf.subject_granularity,
  bf.anatomy,
  bf.sample_prep_method,
  bf.assay_type,
  bf.analysis_type,
  bf.file_format,
  bf.compression_format,
  bf.data_type,
  bf.mime_type,

  bf.dbgap_study_ids,
  bf.projects,
  bf.dccs,
  bf.phenotypes,
  bf.diseases,
  bf.sexes,
  bf.races,
  bf.ethnicities,
  bf.subject_roles,
  bf.subject_granularities,
  bf.subject_species,
  bf.ncbi_taxons,
  bf.anatomies,
  bf.sample_prep_methods,
  bf.assay_types,

  bf.analysis_types,
  bf.file_formats,
  bf.compression_formats,
  bf.data_types,
  bf.mime_types,

  (SELECT json_sorted(json_group_array(DISTINCT json_extract(j.value, '$[0]')))
   FROM json_each(bf.phenotypes) j) AS phenotypes_flat,
  (SELECT json_sorted(json_group_array(DISTINCT json_extract(j.value, '$[0]')))
   FROM json_each(bf.diseases) j) AS diseases_flat,

  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
  json_object('nid', spm.nid, 'name', spm.name, 'description', spm.description) AS sample_prep_method_row,
  json_object('nid', ast.nid, 'name', ast.name, 'description', ast.description) AS assay_type_row,
  json_object('nid', ant.nid, 'name', ant.name, 'description', ant.description) AS analysis_type_row,
  json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
  json_object('nid', cfmt.nid,'name', cfmt.name, 'description', cfmt.description) AS compression_format_row,
  json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
  json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row
FROM (
  SELECT DISTINCT
    bf.id_namespace,
    bf.is_bundle,
    bf.has_persistent_id,

    bf.dbgap_study_id,
    bf.project,
    bf.sex,
    bf.ethnicity,
    bf.subject_granularity,
    bf.anatomy,
    bf.sample_prep_method,
    bf.assay_type,
    bf.analysis_type,
    bf.file_format,
    bf.compression_format,
    bf.data_type,
    bf.mime_type,

    bf.dbgap_study_ids,
    bf.projects,
    bf.dccs,
    bf.phenotypes,
    bf.diseases,
    bf.sexes,
    bf.races,
    bf.ethnicities,
    bf.subject_roles,
    bf.subject_granularities,
    bf.subject_species,
    bf.ncbi_taxons,
    bf.anatomies,
    bf.sample_prep_methods,
    bf.assay_types,

    bf.analysis_types,
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
LEFT JOIN sample_prep_method spm ON (bf.sample_prep_method = spm.nid)
LEFT JOIN assay_type ast ON (bf.assay_type = ast.nid)
LEFT JOIN analysis_type ant ON (bf.analysis_type = ant.nid)
LEFT JOIN file_format fmt ON (bf.file_format = fmt.nid)
LEFT JOIN file_format cfmt ON (bf.compression_format = cfmt.nid)
LEFT JOIN data_type dt ON (bf.data_type = dt.nid)
LEFT JOIN mime_type mt ON (bf.mime_type = mt.nid)
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE biosample AS u
SET core_fact = cf.nid,
    is_bundle = cf.is_bundle,
    has_persistent_id = cf.has_persistent_id,
    projects = cf.projects,
    dccs = cf.dccs,
    phenotypes_flat = cf.phenotypes_flat,
    diseases_flat = cf.diseases_flat,
    sexes = cf.sexes,
    races = cf.races,
    ethnicities = cf.ethnicities,
    subject_roles = cf.subject_roles,
    subject_granularities = cf.subject_granularities,
    subject_species = cf.subject_species,
    ncbi_taxons = cf.ncbi_taxons,
    anatomies = cf.anatomies,
    sample_prep_methods = cf.sample_prep_methods,
    assay_types = cf.assay_types,
    analysis_types = cf.analysis_types,
    file_formats = cf.file_formats,
    compression_formats = cf.compression_formats,
    data_types = cf.data_types,
    mime_types = cf.mime_types,
    dbgap_study_ids = cf.dbgap_study_ids
FROM biosample_facts bf, core_fact cf
WHERE u.nid = bf.nid
  AND bf.id_namespace = cf.id_namespace
  AND bf.is_bundle = cf.is_bundle
  AND bf.has_persistent_id = cf.has_persistent_id

  AND bf.dbgap_study_id = cf.dbgap_study_id
  AND bf.project = cf.project
  AND bf.sex = cf.sex
  AND bf.ethnicity = cf.ethnicity
  AND bf.subject_granularity = cf.subject_granularity
  AND bf.anatomy = cf.anatomy
  AND bf.sample_prep_method = cf.sample_prep_method
  AND bf.assay_type = cf.assay_type
  AND bf.analysis_type = cf.analysis_type
  AND bf.file_format = cf.file_format
  AND bf.compression_format = cf.compression_format
  AND bf.data_type = cf.data_type
  AND bf.mime_type = cf.mime_type

  AND bf.dbgap_study_ids = cf.dbgap_study_ids
  AND bf.projects = cf.projects
  AND bf.dccs = cf.dccs
  AND bf.phenotypes = cf.phenotypes
  AND bf.diseases = cf.diseases
  AND bf.sexes = cf.sexes
  AND bf.races = cf.races
  AND bf.ethnicities = cf.ethnicities
  AND bf.subject_roles = cf.subject_roles
  AND bf.subject_granularities = cf.subject_granularities
  AND bf.subject_species = cf.subject_species
  AND bf.ncbi_taxons = cf.ncbi_taxons
  AND bf.anatomies = cf.anatomies
  AND bf.sample_prep_methods = cf.sample_prep_methods
  AND bf.assay_types = cf.assay_types

  AND bf.analysis_types = cf.analysis_types
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
  s.persistent_id IS NOT NULL AS has_persistent_id,

  -1 AS dbgap_study_id,
  COALESCE(s.project, -1) AS project,
  COALESCE(s.sex, -1) AS sex,
  COALESCE(s.ethnicity, -1) AS ethnicity,
  COALESCE(s.granularity, -1) AS subject_granularity,
  -1 AS anatomy,
  -1 AS assay_type,
  -1 AS sample_prep_method,
  -1 AS analysis_type,
  -1 AS file_format,
  -1 AS compression_format,
  -1 AS data_type,
  -1 AS mime_type,

  '[]' AS dbgap_study_ids,
  json_array(s.project) AS projects,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT dcc.nid))
      FROM project_in_project_transitive pipt
      JOIN dcc ON (pipt.leader_project = dcc.project)
      WHERE pipt.member_project = s.project
    ),
    '[]'
  ) AS dccs,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT json_array(a.phenotype, a.association_type)))
      FROM (
        SELECT a.phenotype, a.association_type FROM subject_phenotype a WHERE a.subject = s.nid
        UNION
        SELECT a.phenotype, 0 AS association_type FROM subject_in_collection sic JOIN collection_phenotype a ON (sic.collection = a.collection) WHERE sic.subject = s.nid
      ) a
    ),
    '[]'
  ) AS phenotypes,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT json_array(a.slim_term, a.association_type)))
      FROM (
        SELECT sl.slim_term, a.association_type FROM subject_disease a JOIN disease_slim_union sl ON (sl.original_term = a.disease) WHERE a.subject = s.nid
        UNION
        SELECT sl.slim_term, a.association_type FROM biosample_from_subject bfs JOIN biosample_disease a ON (bfs.biosample = a.biosample) JOIN disease_slim_union sl ON (sl.original_term = a.disease) WHERE bfs.subject = s.nid
        UNION
        SELECT sl.slim_term, 0 AS association_type FROM subject_in_collection sic JOIN collection_disease a ON (sic.collection = a.collection) JOIN disease_slim_union sl ON (sl.original_term = a.disease) WHERE sic.subject = s.nid
      ) a
    ),
    '[]'
  ) AS diseases,
  CASE WHEN s.sex IS NOT NULL THEN json_array(s.sex) ELSE '[]' END AS sexes,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.race)) FROM subject_race a WHERE a.subject = s.nid
    ),
    '[]'
  ) AS races,
  CASE WHEN s.ethnicity IS NOT NULL THEN json_array(s.ethnicity) ELSE '[]' END AS ethnicities,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a."role")) FROM subject_role_taxonomy a WHERE a.subject = s.nid
    ),
    '[]'
  ) AS subject_roles,
  CASE WHEN s.granularity IS NOT NULL THEN json_array(s.granularity) ELSE '[]' END AS subject_granularities,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.species)) FROM subject_species a WHERE a.subject = s.nid
    ),
    '[]'
  ) AS subject_species,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM subject_role_taxonomy a JOIN ncbi_taxonomy_slim_union sl ON (a.taxon = sl.original_term) WHERE a.subject = s.nid
    ),
    '[]'
  ) AS ncbi_taxons,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM biosample_from_subject bfs JOIN biosample b ON (bfs.biosample = b.nid) JOIN anatomy_slim_union sl ON (sl.original_term = b.anatomy) WHERE bfs.subject = s.nid
    ),
    '[]'
  ) AS anatomies,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT b.sample_prep_method)) FROM biosample_from_subject bfs JOIN biosample b ON (bfs.biosample = b.nid) WHERE bfs.subject = s.nid AND b.sample_prep_method IS NOT NULL
    ),
    '[]'
  ) AS sample_prep_methods,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.slim_term))
      FROM (
        SELECT sl.slim_term FROM file_describes_subject fds JOIN file f ON (fds.file = f.nid) JOIN assay_type_slim_union sl ON (sl.original_term = f.assay_type) WHERE fds.subject = s.nid
      ) a
    ),
    '[]'
  ) AS assay_types,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT f.analysis_type)) FROM file_describes_subject fds JOIN file f ON (fds.file = f.nid) WHERE fds.subject = s.nid AND f.analysis_type IS NOT NULL
    ),
    '[]'
  ) AS analysis_types,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM file_describes_subject fds JOIN file f ON (fds.file = f.nid) JOIN file_format_slim_union sl ON (sl.original_term = f.file_format) WHERE fds.subject = s.nid
    ),
    '[]'
  ) AS file_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT f.compression_format)) FROM file_describes_subject fds JOIN file f ON (fds.file = f.nid) WHERE fds.subject = s.nid AND f.compression_format IS NOT NULL
    ),
    '[]'
  ) AS compression_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT sl.slim_term)) FROM file_describes_subject fds JOIN file f ON (fds.file = f.nid) JOIN data_type_slim_union sl ON (sl.original_term = f.data_type) WHERE fds.subject = s.nid
    ),
    '[]'
  ) AS data_types,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT f.mime_type)) FROM file_describes_subject fds JOIN file f ON (fds.file = f.nid) WHERE fds.subject = s.nid AND f.mime_type IS NOT NULL
    ),
    '[]'
  ) AS mime_types
FROM subject s;
CREATE INDEX IF NOT EXISTS subject_facts_combo_idx ON subject_facts(
    id_namespace,
    is_bundle,
    has_persistent_id,

    dbgap_study_id,
    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    sample_prep_method,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

    dbgap_study_ids,
    projects,
    dccs,
    phenotypes,
    diseases,
    sexes,
    races,
    ethnicities,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    sample_prep_methods,
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    is_bundle,
    has_persistent_id,

    dbgap_study_id,
    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    sample_prep_method,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

    dbgap_study_ids,
    projects,
    dccs,
    phenotypes,
    diseases,
    sexes,
    races,
    ethnicities,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    sample_prep_methods,
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,

    phenotypes_flat,
    diseases_flat,

    id_namespace_row,
    project_row,
    sex_row,
    ethnicity_row,
    subject_granularity_row,
    anatomy_row,
    sample_prep_method_row,
    assay_type_row,
    analysis_type_row,
    file_format_row,
    compression_format_row,
    data_type_row,
    mime_type_row
)
SELECT
  sf.id_namespace,
  sf.is_bundle,
  sf.has_persistent_id,

  sf.dbgap_study_id,
  sf.project,
  sf.sex,
  sf.ethnicity,
  sf.subject_granularity,
  sf.anatomy,
  sf.sample_prep_method,
  sf.assay_type,
  sf.analysis_type,
  sf.file_format,
  sf.compression_format,
  sf.data_type,
  sf.mime_type,

  sf.dbgap_study_ids,
  sf.projects,
  sf.dccs,
  sf.phenotypes,
  sf.diseases,
  sf.sexes,
  sf.races,
  sf.ethnicities,
  sf.subject_roles,
  sf.subject_granularities,
  sf.subject_species,
  sf.ncbi_taxons,
  sf.anatomies,
  sf.sample_prep_methods,
  sf.assay_types,

  sf.analysis_types,
  sf.file_formats,
  sf.compression_formats,
  sf.data_types,
  sf.mime_types,

  (SELECT json_sorted(json_group_array(DISTINCT json_extract(j.value, '$[0]')))
   FROM json_each(sf.phenotypes) j) AS phenotypes_flat,
  (SELECT json_sorted(json_group_array(DISTINCT json_extract(j.value, '$[0]')))
   FROM json_each(sf.diseases) j) AS diseases_flat,

  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
  json_object('nid', spm.nid, 'name', spm.name, 'description', spm.description) AS sample_prep_method_row,
  json_object('nid', ast.nid, 'name', ast.name, 'description', ast.description) AS assay_type_row,
  json_object('nid', ant.nid, 'name', ant.name, 'description', ant.description) AS analysis_type_row,
  json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
  json_object('nid', cfmt.nid,'name', cfmt.name, 'description', cfmt.description) AS compression_format_row,
  json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
  json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row
FROM (
  SELECT DISTINCT
    sf.id_namespace,
    sf.is_bundle,
    sf.has_persistent_id,

    sf.dbgap_study_id,
    sf.project,
    sf.sex,
    sf.ethnicity,
    sf.subject_granularity,
    sf.anatomy,
    sf.sample_prep_method,
    sf.assay_type,
    sf.analysis_type,
    sf.file_format,
    sf.compression_format,
    sf.data_type,
    sf.mime_type,

    sf.dbgap_study_ids,
    sf.projects,
    sf.dccs,
    sf.phenotypes,
    sf.diseases,
    sf.sexes,
    sf.races,
    sf.ethnicities,
    sf.subject_roles,
    sf.subject_granularities,
    sf.subject_species,
    sf.ncbi_taxons,
    sf.anatomies,
    sf.sample_prep_methods,
    sf.assay_types,

    sf.analysis_types,
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
LEFT JOIN sample_prep_method spm ON (sf.sample_prep_method = spm.nid)
LEFT JOIN assay_type ast ON (sf.assay_type = ast.nid)
LEFT JOIN analysis_type ant ON (sf.analysis_type = ant.nid)
LEFT JOIN file_format fmt ON (sf.file_format = fmt.nid)
LEFT JOIN file_format cfmt ON (sf.compression_format = cfmt.nid)
LEFT JOIN data_type dt ON (sf.data_type = dt.nid)
LEFT JOIN mime_type mt ON (sf.mime_type = mt.nid)
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE subject AS u
SET core_fact = cf.nid,
    is_bundle = cf.is_bundle,
    has_persistent_id = cf.has_persistent_id,
    projects = cf.projects,
    dccs = cf.dccs,
    phenotypes_flat = cf.phenotypes_flat,
    diseases_flat = cf.diseases_flat,
    sexes = cf.sexes,
    races = cf.races,
    ethnicities = cf.ethnicities,
    subject_roles = cf.subject_roles,
    subject_granularities = cf.subject_granularities,
    subject_species = cf.subject_species,
    ncbi_taxons = cf.ncbi_taxons,
    anatomies = cf.anatomies,
    sample_prep_methods = cf.sample_prep_methods,
    assay_types = cf.assay_types,
    analysis_types = cf.analysis_types,
    file_formats = cf.file_formats,
    compression_formats = cf.compression_formats,
    data_types = cf.data_types,
    mime_types = cf.mime_types,
    dbgap_study_ids = cf.dbgap_study_ids
FROM subject_facts sf, core_fact cf
WHERE u.nid = sf.nid
  AND sf.id_namespace = cf.id_namespace
  AND sf.is_bundle = cf.is_bundle
  AND sf.has_persistent_id = cf.has_persistent_id

  AND sf.dbgap_study_id = cf.dbgap_study_id
  AND sf.project = cf.project
  AND sf.sex = cf.sex
  AND sf.ethnicity = cf.ethnicity
  AND sf.subject_granularity = cf.subject_granularity
  AND sf.anatomy = cf.anatomy
  AND sf.sample_prep_method = cf.sample_prep_method
  AND sf.assay_type = cf.assay_type
  AND sf.analysis_type = cf.analysis_type
  AND sf.file_format = cf.file_format
  AND sf.compression_format = cf.compression_format
  AND sf.data_type = cf.data_type
  AND sf.mime_type = cf.mime_type

  AND sf.dbgap_study_ids = cf.dbgap_study_ids
  AND sf.projects = cf.projects
  AND sf.dccs = cf.dccs
  AND sf.phenotypes = cf.phenotypes
  AND sf.diseases = cf.diseases
  AND sf.sexes = cf.sexes
  AND sf.races = cf.races
  AND sf.ethnicities = cf.ethnicities
  AND sf.subject_roles = cf.subject_roles
  AND sf.subject_granularities = cf.subject_granularities
  AND sf.subject_species = cf.subject_species
  AND sf.ncbi_taxons = cf.ncbi_taxons
  AND sf.anatomies = cf.anatomies
  AND sf.sample_prep_methods = cf.sample_prep_methods
  AND sf.assay_types = cf.assay_types

  AND sf.analysis_types = cf.analysis_types
  AND sf.file_formats = cf.file_formats
  AND sf.compression_formats = cf.compression_formats
  AND sf.data_types = cf.data_types
  AND sf.mime_types = cf.mime_types
;

CREATE TEMPORARY TABLE collection_facts AS
SELECT
  col.nid,

  col.id_namespace,
  False AS is_bundle,
  col.persistent_id IS NOT NULL AS has_persistent_id,

  -1 AS dbgap_study_id,
  -1 AS project,
  -1 AS sex,
  -1 AS ethnicity,
  -1 AS subject_granularity,
  -1 AS anatomy,
  -1 AS sample_prep_method,
  -1 AS assay_type,
  -1 AS analysis_type,
  -1 AS file_format,
  -1 AS compression_format,
  -1 AS data_type,
  -1 AS mime_type,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.dbgap_study_ids) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS dbgap_study_ids,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT cdbp.project)) FROM collection_defined_by_project cdbp WHERE cdbp.collection = col.nid
    ),
    '[]'
  ) AS projects,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT d.nid))
      FROM collection_defined_by_project cdbp
      JOIN project_in_project_transitive pipt ON (cdbp.project = pipt.member_project)
      JOIN dcc d ON (pipt.leader_project = d.project)
      WHERE cdbp.collection = col.nid
    ),
    '[]'
  ) AS dccs,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT json_array(s.phenotype, s.association_type)))
      FROM (
        SELECT a.phenotype, 0 AS association_type FROM collection_phenotype a WHERE a.collection = col.nid
        UNION
        SELECT json_extract(j.value, '$[0]') AS phenotype, json_extract(j.value, '$[1]') AS association_type
        FROM file_in_collection fic, file f, core_fact cf, json_each(cf.phenotypes) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT json_extract(j.value, '$[0]') AS phenotype, json_extract(j.value, '$[1]') AS association_type
        FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.phenotypes) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT json_extract(j.value, '$[0]') AS phenotype, json_extract(j.value, '$[1]') AS association_type
        FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.phenotypes) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS phenotypes,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT json_array(s.disease, s.association_type)))
      FROM (
        SELECT sl.slim_term AS disease, 0 AS association_type FROM collection_disease a JOIN disease_slim_union sl ON (sl.original_term = a.disease) WHERE a.collection = col.nid
        UNION
        SELECT json_extract(j.value, '$[0]') AS disease, json_extract(j.value, '$[1]') AS association_type
        FROM file_in_collection fic, file f, core_fact cf, json_each(cf.diseases) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT json_extract(j.value, '$[0]') AS disease, json_extract(j.value, '$[1]') AS association_type
        FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.diseases) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT json_extract(j.value, '$[0]') AS disease, json_extract(j.value, '$[1]') AS association_type
        FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.diseases) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS diseases,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.sexes) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.sexes) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.sexes) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS sexes,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.races) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.races) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.races) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS races,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.ethnicities) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.ethnicities) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.ethnicities) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS ethnicities,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.subject_roles) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.subject_roles) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.subject_roles) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS subject_roles,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.subject_granularities) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.subject_granularities) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.subject_granularities) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS subject_granularities,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.subject_species) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.subject_species) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.subject_species) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS subject_species,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT a.taxon AS value FROM collection_taxonomy a WHERE a.collection = col.nid
        UNION
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.ncbi_taxons) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.ncbi_taxons) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value
        FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.ncbi_taxons) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS ncbi_taxons,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT a.anatomy AS value FROM collection_anatomy a WHERE a.collection = col.nid
        UNION
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.anatomies) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.anatomies) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.anatomies) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS anatomies,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.sample_prep_methods) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.sample_prep_methods) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.sample_prep_methods) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS sample_prep_methods,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.assay_types) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.assay_types) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.assay_types) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS assay_types,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.analysis_types) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.analysis_types) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.analysis_types) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS analysis_types,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.file_formats) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.file_formats) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.file_formats) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS file_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.compression_formats) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.compression_formats) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.compression_formats) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS compression_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.data_types) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.data_types) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.data_types) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS data_types,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM file_in_collection fic, file f, core_fact cf, json_each(cf.mime_types) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, core_fact cf, json_each(cf.mime_types) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, core_fact cf, json_each(cf.mime_types) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
      ) s
    ),
    '[]'
  ) AS mime_types
FROM collection col;
CREATE INDEX IF NOT EXISTS collection_facts_combo_idx ON collection_facts(
    id_namespace,
    is_bundle,
    has_persistent_id,

    dbgap_study_id,
    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    sample_prep_method,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

    dbgap_study_ids,
    projects,
    dccs,
    phenotypes,
    diseases,
    sexes,
    races,
    ethnicities,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    sample_prep_methods,
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    is_bundle,
    has_persistent_id,

    dbgap_study_id,
    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    sample_prep_method,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

    dbgap_study_ids,
    projects,
    dccs,
    phenotypes,
    diseases,
    sexes,
    races,
    ethnicities,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    sample_prep_methods,
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,

    phenotypes_flat,
    diseases_flat,

    id_namespace_row,
    project_row,
    sex_row,
    ethnicity_row,
    subject_granularity_row,
    anatomy_row,
    sample_prep_method_row,
    assay_type_row,
    analysis_type_row,
    file_format_row,
    compression_format_row,
    data_type_row,
    mime_type_row
)
SELECT
  colf.id_namespace,
  colf.is_bundle,
  colf.has_persistent_id,

  colf.dbgap_study_id,
  colf.project,
  colf.sex,
  colf.ethnicity,
  colf.subject_granularity,
  colf.anatomy,
  colf.sample_prep_method,
  colf.assay_type,
  colf.analysis_type,
  colf.file_format,
  colf.compression_format,
  colf.data_type,
  colf.mime_type,

  colf.dbgap_study_ids,
  colf.projects,
  colf.dccs,
  colf.phenotypes,
  colf.diseases,
  colf.sexes,
  colf.races,
  colf.ethnicities,
  colf.subject_roles,
  colf.subject_granularities,
  colf.subject_species,
  colf.ncbi_taxons,
  colf.anatomies,
  colf.sample_prep_methods,
  colf.assay_types,

  colf.analysis_types,
  colf.file_formats,
  colf.compression_formats,
  colf.data_types,
  colf.mime_types,
        
  (SELECT json_sorted(json_group_array(DISTINCT json_extract(j.value, '$[0]')))
   FROM json_each(colf.phenotypes) j) AS phenotypes_flat,
  (SELECT json_sorted(json_group_array(DISTINCT json_extract(j.value, '$[0]')))
   FROM json_each(colf.diseases) j) AS diseases_flat,

  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
  json_object('nid', spm.nid, 'name', spm.name, 'description', spm.description) AS sample_prep_method_row,
  json_object('nid', ast.nid, 'name', ast.name, 'description', ast.description) AS assay_type_row,
  json_object('nid', ant.nid, 'name', ant.name, 'description', ant.description) AS analysis_type_row,
  json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
  json_object('nid', cfmt.nid,'name', cfmt.name, 'description', cfmt.description) AS compression_format_row,
  json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
  json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row
FROM (
  SELECT DISTINCT
    colf.id_namespace,
    colf.is_bundle,
    colf.has_persistent_id,

    colf.dbgap_study_id,
    colf.project,
    colf.sex,
    colf.ethnicity,
    colf.subject_granularity,
    colf.anatomy,
    colf.sample_prep_method,
    colf.assay_type,
    colf.analysis_type,
    colf.file_format,
    colf.compression_format,
    colf.data_type,
    colf.mime_type,

    colf.dbgap_study_ids,
    colf.projects,
    colf.dccs,
    colf.phenotypes,
    colf.diseases,
    colf.sexes,
    colf.races,
    colf.ethnicities,
    colf.subject_roles,
    colf.subject_granularities,
    colf.subject_species,
    colf.ncbi_taxons,
    colf.anatomies,
    colf.sample_prep_methods,
    colf.assay_types,

    colf.analysis_types,
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
LEFT JOIN sample_prep_method spm ON (colf.sample_prep_method = spm.nid)
LEFT JOIN assay_type ast ON (colf.assay_type = ast.nid)
LEFT JOIN analysis_type ant ON (colf.analysis_type = ant.nid)
LEFT JOIN file_format fmt ON (colf.file_format = fmt.nid)
LEFT JOIN file_format cfmt ON (colf.compression_format = cfmt.nid)
LEFT JOIN data_type dt ON (colf.data_type = dt.nid)
LEFT JOIN mime_type mt ON (colf.mime_type = mt.nid)
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE collection AS u
SET core_fact = cf.nid,
    is_bundle = cf.is_bundle,
    has_persistent_id = cf.has_persistent_id,
    projects = cf.projects,
    dccs = cf.dccs,
    phenotypes_flat = cf.phenotypes_flat,
    diseases_flat = cf.diseases_flat,
    sexes = cf.sexes,
    races = cf.races,
    ethnicities = cf.ethnicities,
    subject_roles = cf.subject_roles,
    subject_granularities = cf.subject_granularities,
    subject_species = cf.subject_species,
    ncbi_taxons = cf.ncbi_taxons,
    anatomies = cf.anatomies,
    sample_prep_methods = cf.sample_prep_methods,
    assay_types = cf.assay_types,
    analysis_types = cf.analysis_types,
    file_formats = cf.file_formats,
    compression_formats = cf.compression_formats,
    data_types = cf.data_types,
    mime_types = cf.mime_types,
    dbgap_study_ids = cf.dbgap_study_ids
FROM collection_facts colf, core_fact cf
WHERE u.nid = colf.nid
  AND colf.id_namespace = cf.id_namespace
  AND colf.is_bundle = cf.is_bundle
  AND colf.has_persistent_id = cf.has_persistent_id

  AND colf.dbgap_study_id = cf.dbgap_study_id
  AND colf.project = cf.project
  AND colf.sex = cf.sex
  AND colf.ethnicity = cf.ethnicity
  AND colf.subject_granularity = cf.subject_granularity
  AND colf.anatomy = cf.anatomy
  AND colf.sample_prep_method = cf.sample_prep_method
  AND colf.assay_type = cf.assay_type
  AND colf.analysis_type = cf.analysis_type
  AND colf.file_format = cf.file_format
  AND colf.compression_format = cf.compression_format
  AND colf.data_type = cf.data_type
  AND colf.mime_type = cf.mime_type

  AND colf.dbgap_study_ids = cf.dbgap_study_ids
  AND colf.projects = cf.projects
  AND colf.dccs = cf.dccs
  AND colf.phenotypes = cf.phenotypes
  AND colf.diseases = cf.diseases
  AND colf.sexes = cf.sexes
  AND colf.races = cf.races
  AND colf.ethnicities = cf.ethnicities
  AND colf.subject_roles = cf.subject_roles
  AND colf.subject_granularities = cf.subject_granularities
  AND colf.subject_species = cf.subject_species
  AND colf.ncbi_taxons = cf.ncbi_taxons
  AND colf.anatomies = cf.anatomies
  AND colf.sample_prep_methods = cf.sample_prep_methods
  AND colf.assay_types = cf.assay_types

  AND colf.analysis_types = cf.analysis_types
  AND colf.file_formats = cf.file_formats
  AND colf.compression_formats = cf.compression_formats
  AND colf.data_types = cf.data_types
  AND colf.mime_types = cf.mime_types
;

CREATE TEMPORARY TABLE corefact_kw AS
  SELECT
    cf.nid,
    cfde_keywords_merge(
      cfde_keywords(n.name, n.abbreviation),

      (SELECT cfde_keywords_agg(dbg.name, dbg.description)
       FROM json_each(cf.dbgap_study_ids) j JOIN dbgap_study_id dbg ON (j.value = dbg.nid)),

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
         cfde_keywords_agg(spm.id, spm.name, spm.description, spm.synonyms)
       )
       FROM json_each(cf.sample_prep_methods) spmj JOIN sample_prep_method spm ON (spmj.value = spm.nid)),

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

INSERT INTO keywords (kw)
SELECT kw FROM (
SELECT DISTINCT array_join(kw, ' ') AS kw FROM corefact_kw
UNION
SELECT DISTINCT array_join(cfde_keywords_merge(json_array(local_id, persistent_id, filename)), ' ') FROM file
UNION
SELECT DISTINCT array_join(cfde_keywords_merge(json_array(local_id, persistent_id)), ' ') FROM biosample
UNION
SELECT DISTINCT array_join(cfde_keywords_merge(json_array(local_id, persistent_id)), ' ') FROM subject
UNION
SELECT DISTINCT array_join(cfde_keywords_merge(json_array(local_id, persistent_id, abbreviation, name, description)), ' ') FROM collection
) AS s
WHERE kw IS NOT NULL
  AND kw != ''
;

CREATE TEMPORARY TABLE corefact_kw_map AS
SELECT
  c.nid AS core_fact,
  k.nid AS kw
FROM corefact_kw c
JOIN keywords k ON (array_join(c.kw, ' ') = k.kw)
;

INSERT INTO file_keywords (file, kw)
SELECT s.nid, k.kw
FROM file s JOIN corefact_kw_map k ON (s.core_fact = k.core_fact)
UNION
SELECT s.nid, k.nid
FROM file s JOIN keywords k ON ( array_join(cfde_keywords_merge(json_array(s.local_id, s.persistent_id, s.filename)), ' ') = k.kw)
;
INSERT INTO biosample_keywords (biosample, kw)
SELECT s.nid, k.kw
FROM biosample s JOIN corefact_kw_map k ON (s.core_fact = k.core_fact)
UNION
SELECT s.nid, k.nid
FROM biosample s JOIN keywords k ON ( array_join(cfde_keywords_merge(json_array(s.local_id, s.persistent_id)), ' ') = k.kw)
;
INSERT INTO subject_keywords (subject, kw)
SELECT s.nid, k.kw
FROM subject s JOIN corefact_kw_map k ON (s.core_fact = k.core_fact)
UNION
SELECT s.nid, k.nid
FROM subject s JOIN keywords k ON ( array_join(cfde_keywords_merge(json_array(s.local_id, s.persistent_id)), ' ') = k.kw)
;
INSERT INTO collection_keywords (collection, kw)
SELECT s.nid, k.kw
FROM collection s JOIN corefact_kw_map k ON (s.core_fact = k.core_fact)
UNION
SELECT s.nid, k.nid
FROM collection s JOIN keywords k ON ( array_join(cfde_keywords_merge(json_array(s.local_id, s.persistent_id, s.abbreviation, s.name), cfde_keywords(s.description)), ' ') = k.kw)
;

UPDATE core_fact AS v
SET
-- HACK: undo usage of -1 in place of NULLs before we send to catalog
-- has nothing to do with kw but this should be done in the same rewrite order
  dbgap_study_id = CASE WHEN v.dbgap_study_id = -1 THEN NULL ELSE v.dbgap_study_id END,
  project = CASE WHEN v.project = -1 THEN NULL ELSE v.project END,
  sex = CASE WHEN v.sex = -1 THEN NULL ELSE v.sex END,
  ethnicity = CASE WHEN v.ethnicity = -1 THEN NULL ELSE v.ethnicity END,
  subject_granularity = CASE WHEN v.subject_granularity = -1 THEN NULL ELSE v.subject_granularity END,
  anatomy = CASE WHEN v.anatomy = -1 THEN NULL ELSE v.anatomy END,
  sample_prep_method = CASE WHEN v.sample_prep_Method = -1 THEN NULL ELSE v.sample_prep_method END,
  assay_type = CASE WHEN v.assay_type = -1 THEN NULL ELSE v.assay_type END,
  analysis_type = CASE WHEN v.analysis_type = -1 THEN NULL ELSE v.analysis_type END,
  file_format = CASE WHEN v.file_format = -1 THEN NULL ELSE v.file_format END,
  compression_format = CASE WHEN v.compression_format = -1 THEN NULL ELSE v.compression_format END,
  data_type = CASE WHEN v.data_type = -1 THEN NULL ELSE v.data_type END,
  mime_type = CASE WHEN v.mime_type = -1 THEN NULL ELSE v.mime_type END
;
