CREATE TEMPORARY TABLE file_facts AS
SELECT
  f.nid,

  f.id_namespace,
  f.bundle_collection IS NOT NULL AS is_bundle,
  f.persistent_id IS NOT NULL AS has_persistent_id,

  COALESCE(f.project, -1) AS project,
  -1 AS sex,
  -1 AS ethnicity,
  -1 AS subject_granularity,
  -1 AS anatomy,
  COALESCE(f.assay_type, -1) AS assay_type,
  COALESCE(f.analysis_type, -1) AS analysis_type,
  COALESCE(f.file_format, -1) AS file_format,
  COALESCE(f.compression_format, -1) AS compression_format,
  COALESCE(f.data_type, -1) AS data_type,
  COALESCE(f.mime_type, -1) AS mime_type,

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
      SELECT json_sorted(json_group_array(DISTINCT json_array(a.disease, a.association_type)))
      FROM (
        SELECT a.disease, a.association_type FROM file_describes_subject fds JOIN subject_disease a ON (fds.subject = a.subject) WHERE fds.file = f.nid
        UNION
        SELECT a.disease, a.association_type FROM file_describes_biosample fdb JOIN biosample_disease a ON (fdb.biosample = a.biosample) WHERE fdb.file = f.nid
        UNION
        SELECT a.disease, 0 AS association_type FROM file_in_collection fic JOIN collection_disease a ON (fic.collection = a.collection) WHERE fic.file = f.nid
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
      SELECT json_sorted(json_group_array(DISTINCT a.taxon)) FROM file_describes_subject fds JOIN subject_role_taxonomy a ON (fds.subject = a.subject) WHERE fds.file = f.nid
    ),
    '[]'
  ) AS ncbi_taxons,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT b.anatomy)) FROM file_describes_biosample fdb JOIN biosample b ON (fdb.biosample = b.nid) WHERE fdb.file = f.nid AND b.anatomy IS NOT NULL
    ),
    '[]'
  ) AS anatomies,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.assay_type))
      FROM (
        SELECT b.assay_type FROM file_describes_biosample fdb JOIN biosample b ON (fdb.biosample = b.nid) WHERE fdb.file = f.nid AND b.assay_type IS NOT NULL
        UNION
        SELECT v.nid AS assay_type FROM assay_type v WHERE v.nid = f.assay_type
      ) a
    ),
    '[]'
  ) AS assay_types,

  CASE WHEN f.analysis_type IS NOT NULL THEN json_array(f.analysis_type) ELSE '[]' END AS analysis_types,
  CASE WHEN f.file_format IS NOT NULL THEN json_array(f.file_format) ELSE '[]' END AS file_formats,
  CASE WHEN f.compression_format IS NOT NULL THEN json_array(f.compression_format) ELSE '[]' END AS compression_formats,
  CASE WHEN f.data_type   IS NOT NULL THEN json_array(f.data_type)   ELSE '[]' END AS data_types,
  CASE WHEN f.mime_type   IS NOT NULL THEN json_array(f.mime_type)   ELSE '[]' END AS mime_types
FROM file f;
CREATE INDEX IF NOT EXISTS file_facts_combo_idx ON file_facts(
    id_namespace,
    is_bundle,
    has_persistent_id,

    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

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

    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

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
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,
    
    id_namespace_row,
    project_row,
    sex_row,
    ethnicity_row,
    subject_granularity_row,
    anatomy_row,
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

  ff.project,
  ff.sex,
  ff.ethnicity,
  ff.subject_granularity,
  ff.anatomy,
  ff.assay_type,
  ff.analysis_type,
  ff.file_format,
  ff.compression_format,
  ff.data_type,
  ff.mime_type,
    
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
  ff.assay_types,

  ff.analysis_types,
  ff.file_formats,
  ff.compression_formats,
  ff.data_types,
  ff.mime_types,

  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
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

    ff.project,
    ff.sex,
    ff.ethnicity,
    ff.subject_granularity,
    ff.anatomy,
    ff.assay_type,
    ff.analysis_type,
    ff.file_format,
    ff.compression_format,
    ff.data_type,
    ff.mime_type,

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
LEFT JOIN assay_type ast ON (ff.assay_type = ast.nid)
LEFT JOIN analysis_type ant ON (ff.analysis_type = ant.nid)
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
  AND ff.has_persistent_id = cf.has_persistent_id

  AND ff.project = cf.project
  AND ff.sex = cf.sex
  AND ff.ethnicity = cf.ethnicity
  AND ff.subject_granularity = cf.subject_granularity
  AND ff.anatomy = cf.anatomy
  AND ff.assay_type = cf.assay_type
  AND ff.analysis_type = cf.analysis_type
  AND ff.file_format = cf.file_format
  AND ff.compression_format = cf.compression_format
  AND ff.data_type = cf.data_type
  AND ff.mime_type = cf.mime_type

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

  COALESCE(b.project, -1) AS project,
  -1 AS sex,
  -1 AS ethnicity,
  -1 AS subject_granularity,
  COALESCE(b.anatomy, -1) AS anatomy,
  COALESCE(b.assay_type, -1) AS assay_type,
  -1 AS analysis_type,
  -1 AS file_format,
  -1 AS compression_format,
  -1 AS data_type,
  -1 AS mime_type,

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
      SELECT json_sorted(json_group_array(DISTINCT json_array(a.disease, a.association_type)))
      FROM (
        SELECT a.disease, a.association_type FROM biosample_from_subject bfs JOIN subject_disease a ON (bfs.subject = a.subject) WHERE bfs.biosample = b.nid
        UNION
        SELECT a.disease, a.association_type FROM biosample_disease a WHERE a.biosample = b.nid
        UNION
        SELECT a.disease, 0 AS association_type FROM biosample_in_collection bic JOIN collection_disease a ON (bic.collection = a.collection) WHERE bic.biosample = b.nid
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
      SELECT json_sorted(json_group_array(DISTINCT a.taxon)) FROM biosample_from_subject bfs JOIN subject_role_taxonomy a ON (bfs.subject = a.subject) WHERE bfs.biosample = b.nid
    ),
    '[]'
  ) AS ncbi_taxons,
  CASE WHEN b.anatomy IS NOT NULL THEN json_array(b.anatomy) ELSE '[]' END AS anatomies,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.assay_type))
      FROM (
        SELECT f.assay_type FROM file_describes_biosample fdb JOIN file f ON (fdb.file = f.nid) WHERE fdb.biosample = b.nid AND f.assay_type IS NOT NULL
        UNION
        SELECT v.nid AS assay_type FROM assay_type v WHERE v.nid = b.assay_type
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
      SELECT json_sorted(json_group_array(DISTINCT f.file_format)) FROM file_describes_biosample fdb JOIN file f ON (fdb.file = f.nid) WHERE fdb.biosample = b.nid AND f.file_format IS NOT NULL
    ),
    '[]'
  ) AS file_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT f.compression_format)) FROM file_describes_biosample fdb JOIN file f ON (fdb.file = f.nid) WHERE fdb.biosample = b.nid AND f.compression_format IS NOT NULL
    ),
    '[]'
  ) AS compression_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT f.data_type)) FROM file_describes_biosample fdb JOIN file f ON (fdb.file = f.nid) WHERE fdb.biosample = b.nid AND f.data_type IS NOT NULL
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

    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

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

    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

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
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,
    
    id_namespace_row,
    project_row,
    sex_row,
    ethnicity_row,
    subject_granularity_row,
    anatomy_row,
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

  bf.project,
  bf.sex,
  bf.ethnicity,
  bf.subject_granularity,
  bf.anatomy,
  bf.assay_type,
  bf.analysis_type,
  bf.file_format,
  bf.compression_format,
  bf.data_type,
  bf.mime_type,
    
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
  bf.assay_types,

  bf.analysis_types,
  bf.file_formats,
  bf.compression_formats,
  bf.data_types,
  bf.mime_types,

  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
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

    bf.project,
    bf.sex,
    bf.ethnicity,
    bf.subject_granularity,
    bf.anatomy,
    bf.assay_type,
    bf.analysis_type,
    bf.file_format,
    bf.compression_format,
    bf.data_type,
    bf.mime_type,
    
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
SET core_fact = cf.nid
FROM biosample_facts bf, core_fact cf
WHERE u.nid = bf.nid
  AND bf.id_namespace = cf.id_namespace
  AND bf.is_bundle = cf.is_bundle
  AND bf.has_persistent_id = cf.has_persistent_id

  AND bf.project = cf.project
  AND bf.sex = cf.sex
  AND bf.ethnicity = cf.ethnicity
  AND bf.subject_granularity = cf.subject_granularity
  AND bf.anatomy = cf.anatomy
  AND bf.assay_type = cf.assay_type
  AND bf.analysis_type = cf.analysis_type
  AND bf.file_format = cf.file_format
  AND bf.compression_format = cf.compression_format
  AND bf.data_type = cf.data_type
  AND bf.mime_type = cf.mime_type

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

  COALESCE(s.project, -1) AS project,
  COALESCE(s.sex, -1) AS sex,
  COALESCE(s.ethnicity, -1) AS ethnicity,
  COALESCE(s.granularity, -1) AS subject_granularity,
  -1 AS anatomy,
  -1 AS assay_type,
  -1 AS analysis_type,
  -1 AS file_format,
  -1 AS compression_format,
  -1 AS data_type,
  -1 AS mime_type,

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
      SELECT json_sorted(json_group_array(DISTINCT json_array(a.disease, a.association_type)))
      FROM (
        SELECT a.disease, a.association_type FROM subject_disease a WHERE a.subject = s.nid
        UNION
        SELECT a.disease, a.association_type FROM biosample_from_subject bfs JOIN biosample_disease a ON (bfs.biosample = a.biosample) WHERE bfs.subject = s.nid
        UNION
        SELECT a.disease, 0 AS association_type FROM subject_in_collection sic JOIN collection_disease a ON (sic.collection = a.collection) WHERE sic.subject = s.nid
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
      SELECT json_sorted(json_group_array(DISTINCT a.taxon)) FROM subject_role_taxonomy a WHERE a.subject = s.nid
    ),
    '[]'
  ) AS ncbi_taxons,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT b.anatomy)) FROM biosample_from_subject bfs JOIN biosample b ON (bfs.biosample = b.nid) WHERE bfs.subject = s.nid AND b.anatomy IS NOT NULL
    ),
    '[]'
  ) AS anatomies,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.assay_type))
      FROM (
        SELECT f.assay_type FROM file_describes_subject fds JOIN file f ON (fds.file = f.nid) WHERE fds.subject = s.nid AND f.assay_type IS NOT NULL
        UNION
        SELECT b.assay_type FROM biosample_from_subject bfs JOIN biosample b ON (bfs.biosample = b.nid) WHERE bfs.subject = s.nid AND b.assay_type IS NOT NULL
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
      SELECT json_sorted(json_group_array(DISTINCT f.file_format)) FROM file_describes_subject fds JOIN file f ON (fds.file = f.nid) WHERE fds.subject = s.nid AND f.file_format IS NOT NULL
    ),
    '[]'
  ) AS file_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT f.compression_format)) FROM file_describes_subject fds JOIN file f ON (fds.file = f.nid) WHERE fds.subject = s.nid AND f.compression_format IS NOT NULL
    ),
    '[]'
  ) AS compression_formats,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT f.data_type)) FROM file_describes_subject fds JOIN file f ON (fds.file = f.nid) WHERE fds.subject = s.nid AND f.data_type IS NOT NULL
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

    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

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

    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,
    
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
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,
    
    id_namespace_row,
    project_row,
    sex_row,
    ethnicity_row,
    subject_granularity_row,
    anatomy_row,
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

  sf.project,
  sf.sex,
  sf.ethnicity,
  sf.subject_granularity,
  sf.anatomy,
  sf.assay_type,
  sf.analysis_type,
  sf.file_format,
  sf.compression_format,
  sf.data_type,
  sf.mime_type,
    
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
  sf.assay_types,

  sf.analysis_types,
  sf.file_formats,
  sf.compression_formats,
  sf.data_types,
  sf.mime_types,

  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
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

    sf.project,
    sf.sex,
    sf.ethnicity,
    sf.subject_granularity,
    sf.anatomy,
    sf.assay_type,
    sf.analysis_type,
    sf.file_format,
    sf.compression_format,
    sf.data_type,
    sf.mime_type,

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
SET core_fact = cf.nid
FROM subject_facts sf, core_fact cf
WHERE u.nid = sf.nid
  AND sf.id_namespace = cf.id_namespace
  AND sf.is_bundle = cf.is_bundle
  AND sf.has_persistent_id = cf.has_persistent_id

  AND sf.project = cf.project
  AND sf.sex = cf.sex
  AND sf.ethnicity = cf.ethnicity
  AND sf.subject_granularity = cf.subject_granularity
  AND sf.anatomy = cf.anatomy
  AND sf.assay_type = cf.assay_type
  AND sf.analysis_type = cf.analysis_type
  AND sf.file_format = cf.file_format
  AND sf.compression_format = cf.compression_format
  AND sf.data_type = cf.data_type
  AND sf.mime_type = cf.mime_type

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

  -1 AS project,
  -1 AS sex,
  -1 AS ethnicity,
  -1 AS subject_granularity,
  -1 AS anatomy,
  -1 AS assay_type,
  -1 AS analysis_type,
  -1 AS file_format,
  -1 AS compression_format,
  -1 AS data_type,
  -1 AS mime_type,

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
        SELECT a.disease, 0 AS association_type FROM collection_disease a WHERE a.collection = col.nid
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

    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,

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

    project,
    sex,
    ethnicity,
    subject_granularity,
    anatomy,
    assay_type,
    analysis_type,
    file_format,
    compression_format,
    data_type,
    mime_type,
    
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
    assay_types,

    analysis_types,
    file_formats,
    compression_formats,
    data_types,
    mime_types,
    
    id_namespace_row,
    project_row,
    sex_row,
    ethnicity_row,
    subject_granularity_row,
    anatomy_row,
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

  colf.project,
  colf.sex,
  colf.ethnicity,
  colf.subject_granularity,
  colf.anatomy,
  colf.assay_type,
  colf.analysis_type,
  colf.file_format,
  colf.compression_format,
  colf.data_type,
  colf.mime_type,
    
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
  colf.assay_types,

  colf.analysis_types,
  colf.file_formats,
  colf.compression_formats,
  colf.data_types,
  colf.mime_types,
        
  json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
  json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
  json_object('nid', sx.nid,'name', sx.name, 'description', sx.description) AS sex_row,
  json_object('nid', eth.nid,'name', eth.name, 'description', eth.description) AS ethnicity_row,
  json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
  json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
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

    colf.project,
    colf.sex,
    colf.ethnicity,
    colf.subject_granularity,
    colf.anatomy,
    colf.assay_type,
    colf.analysis_type,
    colf.file_format,
    colf.compression_format,
    colf.data_type,
    colf.mime_type,

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
SET core_fact = cf.nid
FROM collection_facts colf, core_fact cf
WHERE u.nid = colf.nid
  AND colf.id_namespace = cf.id_namespace
  AND colf.is_bundle = cf.is_bundle
  AND colf.has_persistent_id = cf.has_persistent_id

  AND colf.project = cf.project
  AND colf.sex = cf.sex
  AND colf.ethnicity = cf.ethnicity
  AND colf.subject_granularity = cf.subject_granularity
  AND colf.anatomy = cf.anatomy
  AND colf.assay_type = cf.assay_type
  AND colf.analysis_type = cf.analysis_type
  AND colf.file_format = cf.file_format
  AND colf.compression_format = cf.compression_format
  AND colf.data_type = cf.data_type
  AND colf.mime_type = cf.mime_type

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
  AND colf.assay_types = cf.assay_types

  AND colf.analysis_types = cf.analysis_types
  AND colf.file_formats = cf.file_formats
  AND colf.compression_formats = cf.compression_formats
  AND colf.data_types = cf.data_types
  AND colf.mime_types = cf.mime_types
;
