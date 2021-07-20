CREATE TEMPORARY TABLE file_facts AS
  SELECT
    f.nid,
    f.id_namespace,
    f.project,
    CASE WHEN count(DISTINCT s.granularity) FILTER (WHERE s.granularity IS NOT NULL) = 1 THEN s.granularity ELSE NULL END AS subject_granularity,
    CASE WHEN count(DISTINCT b.anatomy) FILTER (WHERE b.anatomy IS NOT NULL) = 1 THEN b.anatomy ELSE NULL END AS anatomy,
    f.assay_type,
    f.file_format,
    f.data_type,
    f.mime_type,
    json_array(f.project) AS projects,
    json_sorted(COALESCE(json_group_array(DISTINCT d.nid) FILTER (WHERE d.nid IS NOT NULL), '[]')) AS dccs,
    json_sorted(COALESCE(json_group_array(DISTINCT srt."role") FILTER (WHERE srt."role" IS NOT NULL), '[]')) AS subject_roles,
    json_sorted(COALESCE(json_group_array(DISTINCT s.granularity) FILTER (WHERE s.granularity IS NOT NULL), '[]')) AS subject_granularities,
    json_sorted(COALESCE(json_group_array(DISTINCT ss.species) FILTER (WHERE ss.species IS NOT NULL), '[]')) AS subject_species,
    json_sorted(COALESCE(json_group_array(DISTINCT srt.taxon) FILTER (WHERE srt.taxon IS NOT NULL), '[]')) AS ncbi_taxons,
    json_sorted(COALESCE(json_group_array(DISTINCT b.anatomy) FILTER (WHERE b.anatomy IS NOT NULL), '[]')) AS anatomies,
    CASE WHEN f.assay_type  IS NOT NULL THEN json_array(f.assay_type)  ELSE '[]' END AS assay_types,
    CASE WHEN f.file_format IS NOT NULL THEN json_array(f.file_format) ELSE '[]' END AS file_formats,
    CASE WHEN f.data_type   IS NOT NULL THEN json_array(f.data_type)   ELSE '[]' END AS data_types,
    CASE WHEN f.mime_type   IS NOT NULL THEN json_array(f.mime_type)   ELSE '[]' END AS mime_types
  FROM file f
  JOIN project_in_project_transitive pipt ON (f.project = pipt.member_project)
  JOIN dcc d ON (pipt.leader_project = d.project)
  LEFT JOIN (
    file_describes_biosample fdb
    JOIN biosample b ON (fdb.biosample = b.nid)
    LEFT JOIN (
      biosample_from_subject bfs
      JOIN subject s ON (bfs.subject = s.nid)
      LEFT JOIN subject_species ss ON (ss.subject = s.nid)
      LEFT JOIN subject_role_taxonomy srt ON (s.nid = srt.subject)
    ) ON (b.nid = bfs.biosample)
  ) ON (f.nid = fdb.file)
  GROUP BY f.nid
;
CREATE INDEX IF NOT EXISTS file_facts_combo_idx ON file_facts(
    id_namespace,
    project,
    subject_granularity,
    anatomy,
    assay_type,
    file_format,
    data_type,
    mime_type,
    projects,
    dccs,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    project,
    subject_granularity,
    anatomy,
    assay_type,
    file_format,
    data_type,
    mime_type,
    id_namespace_row,
    project_row,
    subject_granularity_row,
    anatomy_row,
    assay_type_row,
    file_format_row,
    data_type_row,
    mime_type_row,
    projects,
    dccs,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    data_types,
    mime_types
)
  SELECT DISTINCT
    ff.id_namespace,
    ff.project,
    ff.subject_granularity,
    ff.anatomy,
    ff.assay_type,
    ff.file_format,
    ff.data_type,
    ff.mime_type,
    json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
    json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
    json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
    json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
    json_object('nid', "at".nid, 'name', "at".name, 'description', "at".description) AS assay_type_row,
    json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
    json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
    json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row,
    ff.projects,
    ff.dccs,
    ff.subject_roles,
    ff.subject_granularities,
    ff.subject_species,
    ff.ncbi_taxons,
    ff.anatomies,
    ff.assay_types,
    ff.file_formats,
    ff.data_types,
    ff.mime_types
  FROM file_facts ff
  JOIN id_namespace n ON (ff.id_namespace = n.nid)
  JOIN project p ON (ff.project = p.nid)
  LEFT JOIN subject_granularity sg ON (ff.subject_granularity = sg.nid)
  LEFT JOIN anatomy a ON (ff.anatomy = a.nid)
  LEFT JOIN assay_type "at" ON (ff.assay_type = "at".nid)
  LEFT JOIN file_format fmt ON (ff.file_format = fmt.nid)
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
  AND ff.project = cf.project
  -- sqlite "IS" operator for IS NOT DISTINCT FROM
  AND ff.subject_granularity IS cf.subject_granularity
  AND ff.anatomy IS cf.anatomy
  AND ff.assay_type IS cf.assay_type
  AND ff.file_format IS cf.file_format
  AND ff.data_type IS cf.data_type
  AND ff.mime_type IS cf.mime_type
  AND ff.projects = cf.projects
  AND ff.dccs = cf.dccs
  AND ff.subject_roles = cf.subject_roles
  AND ff.subject_granularities = cf.subject_granularities
  AND ff.subject_species = cf.subject_species
  AND ff.ncbi_taxons = cf.ncbi_taxons
  AND ff.anatomies = cf.anatomies
  AND ff.assay_types = cf.assay_types
  AND ff.file_formats = cf.file_formats
  AND ff.data_types = cf.data_types
  AND ff.mime_types = cf.mime_types
;

CREATE TEMPORARY TABLE biosample_facts AS
  SELECT
    b.nid,
    b.id_namespace,
    b.project,
    CASE WHEN count(DISTINCT s.granularity) FILTER (WHERE s.granularity IS NOT NULL) = 1 THEN s.granularity ELSE NULL END AS subject_granularity,
    b.anatomy,
    CASE WHEN count(DISTINCT f.assay_type) FILTER (WHERE f.assay_type IS NOT NULL) = 1 THEN f.assay_type ELSE NULL END AS assay_type,
    CASE WHEN count(DISTINCT f.file_format) FILTER (WHERE f.file_format IS NOT NULL) = 1 THEN f.file_format ELSE NULL END AS file_format,
    CASE WHEN count(DISTINCT f.data_type) FILTER (WHERE f.data_type IS NOT NULL) = 1 THEN f.data_type ELSE NULL END AS data_type,
    CASE WHEN count(DISTINCT f.mime_type) FILTER (WHERE f.mime_type IS NOT NULL) = 1 THEN f.mime_type ELSE NULL END AS mime_type,
    json_array(b.project) AS projects,
    json_sorted(COALESCE(json_group_array(DISTINCT d.nid) FILTER (WHERE d.nid IS NOT NULL), '[]')) AS dccs,
    json_sorted(COALESCE(json_group_array(DISTINCT srt."role") FILTER (WHERE srt."role" IS NOT NULL), '[]')) AS subject_roles,
    json_sorted(COALESCE(json_group_array(DISTINCT s.granularity) FILTER (WHERE s.granularity IS NOT NULL), '[]')) AS subject_granularities,
    json_sorted(COALESCE(json_group_array(DISTINCT ss.species) FILTER (WHERE ss.species IS NOT NULL), '[]')) AS subject_species,
    json_sorted(COALESCE(json_group_array(DISTINCT srt.taxon) FILTER (WHERE srt.taxon IS NOT NULL), '[]')) AS ncbi_taxons,
    CASE WHEN b.anatomy IS NOT NULL THEN json_array(b.anatomy) ELSE '[]' END AS anatomies,
    json_sorted(COALESCE(json_group_array(DISTINCT f.assay_type) FILTER (WHERE f.assay_type IS NOT NULL), '[]')) AS assay_types,
    json_sorted(COALESCE(json_group_array(DISTINCT f.file_format) FILTER (WHERE f.file_format IS NOT NULL), '[]')) AS file_formats,
    json_sorted(COALESCE(json_group_array(DISTINCT f.data_type) FILTER (WHERE f.data_type IS NOT NULL), '[]')) AS data_types,
    json_sorted(COALESCE(json_group_array(DISTINCT f.mime_type) FILTER (WHERE f.mime_type IS NOT NULL), '[]')) AS mime_types
  FROM biosample b
  JOIN project_in_project_transitive pipt ON (b.project = pipt.member_project)
  JOIN dcc d ON (pipt.leader_project = d.project)
  LEFT JOIN (
    file_describes_biosample fdb
    JOIN file f ON (fdb.file = f.nid)
  ) ON (b.nid = fdb.biosample)
  LEFT JOIN (
    biosample_from_subject bfs
    JOIN subject s ON (bfs.subject = s.nid)
    LEFT JOIN subject_species ss ON (ss.subject = s.nid)
    LEFT JOIN subject_role_taxonomy srt ON (s.nid = srt.subject)
  ) ON (b.nid = bfs.biosample)
  GROUP BY b.nid
;
CREATE INDEX IF NOT EXISTS biosample_facts_combo_idx ON biosample_facts(
    id_namespace,
    project,
    subject_granularity,
    anatomy,
    assay_type,
    file_format,
    data_type,
    mime_type,
    projects,
    dccs,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    project,
    subject_granularity,
    anatomy,
    assay_type,
    file_format,
    data_type,
    mime_type,
    id_namespace_row,
    project_row,
    subject_granularity_row,
    anatomy_row,
    assay_type_row,
    file_format_row,
    data_type_row,
    mime_type_row,
    projects,
    dccs,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    data_types,
    mime_types
)
  SELECT DISTINCT
    bf.id_namespace,
    bf.project,
    bf.subject_granularity,
    bf.anatomy,
    bf.assay_type,
    bf.file_format,
    bf.data_type,
    bf.mime_type,
    json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
    json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
    json_object('nid', sg.nid,'name', sg.name, 'description', sg.description) AS subject_granularity_row,
    json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
    json_object('nid', "at".nid, 'name', "at".name, 'description', "at".description) AS assay_type_row,
    json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
    json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
    json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row,
    bf.projects,
    bf.dccs,
    bf.subject_roles,
    bf.subject_granularities,
    bf.subject_species,
    bf.ncbi_taxons,
    bf.anatomies,
    bf.assay_types,
    bf.file_formats,
    bf.data_types,
    bf.mime_types
  FROM biosample_facts bf
  JOIN id_namespace n ON (bf.id_namespace = n.nid)
  JOIN project p ON (bf.project = p.nid)
  LEFT JOIN subject_granularity sg ON (bf.subject_granularity = sg.nid)
  LEFT JOIN anatomy a ON (bf.anatomy = a.nid)
  LEFT JOIN assay_type "at" ON (bf.assay_type = "at".nid)
  LEFT JOIN file_format fmt ON (bf.file_format = fmt.nid)
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
  AND bf.project = cf.project
  -- sqlite "IS" operator for IS NOT DISTINCT FROM
  AND bf.subject_granularity IS cf.subject_granularity
  AND bf.anatomy IS cf.anatomy
  AND bf.assay_type IS cf.assay_type
  AND bf.file_format IS cf.file_format
  AND bf.data_type IS cf.data_type
  AND bf.mime_type IS cf.mime_type
  AND bf.projects = cf.projects
  AND bf.dccs = cf.dccs
  AND bf.subject_roles = cf.subject_roles
  AND bf.subject_granularities = cf.subject_granularities
  AND bf.subject_species = cf.subject_species
  AND bf.ncbi_taxons = cf.ncbi_taxons
  AND bf.anatomies = cf.anatomies
  AND bf.assay_types = cf.assay_types
  AND bf.file_formats = cf.file_formats
  AND bf.data_types = cf.data_types
  AND bf.mime_types = cf.mime_types
;

CREATE TEMPORARY TABLE subject_facts AS
  SELECT
    s.nid,
    s.id_namespace,
    s.project,
    s.granularity AS subject_granularity,
    CASE WHEN count(DISTINCT b.anatomy) FILTER (WHERE b.anatomy IS NOT NULL) = 1 THEN b.anatomy ELSE NULL END AS anatomy,
    CASE WHEN count(DISTINCT f.assay_type) FILTER (WHERE f.assay_type IS NOT NULL) = 1 THEN f.assay_type ELSE NULL END AS assay_type,
    CASE WHEN count(DISTINCT f.file_format) FILTER (WHERE f.file_format IS NOT NULL) = 1 THEN f.file_format ELSE NULL END AS file_format,
    CASE WHEN count(DISTINCT f.data_type) FILTER (WHERE f.data_type IS NOT NULL) = 1 THEN f.data_type ELSE NULL END AS data_type,
    CASE WHEN count(DISTINCT f.mime_type) FILTER (WHERE f.mime_type IS NOT NULL) = 1 THEN f.mime_type ELSE NULL END AS mime_type,
    json_array(s.project) AS projects,
    json_sorted(COALESCE(json_group_array(DISTINCT d.nid) FILTER (WHERE d.nid IS NOT NULL), '[]')) AS dccs,
    json_sorted(COALESCE(json_group_array(DISTINCT srt."role") FILTER (WHERE srt."role" IS NOT NULL), '[]')) AS subject_roles,
    CASE WHEN s.granularity IS NOT NULL THEN json_array(s.granularity) ELSE '[]' END AS subject_granularities,
    json_sorted(COALESCE(json_group_array(DISTINCT ss.species) FILTER (WHERE ss.species IS NOT NULL), '[]')) AS subject_species,
    json_sorted(COALESCE(json_group_array(DISTINCT srt.taxon) FILTER (WHERE srt.taxon IS NOT NULL), '[]')) AS ncbi_taxons,
    json_sorted(COALESCE(json_group_array(DISTINCT b.anatomy) FILTER (WHERE b.anatomy IS NOT NULL), '[]')) AS anatomies,
    json_sorted(COALESCE(json_group_array(DISTINCT f.assay_type) FILTER (WHERE f.assay_type IS NOT NULL), '[]')) AS assay_types,
    json_sorted(COALESCE(json_group_array(DISTINCT f.file_format) FILTER (WHERE f.file_format IS NOT NULL), '[]')) AS file_formats,
    json_sorted(COALESCE(json_group_array(DISTINCT f.data_type) FILTER (WHERE f.data_type IS NOT NULL), '[]')) AS data_types,
    json_sorted(COALESCE(json_group_array(DISTINCT f.mime_type) FILTER (WHERE f.mime_type IS NOT NULL), '[]')) AS mime_types
  FROM subject s
  JOIN project_in_project_transitive pipt ON (s.project = pipt.member_project)
  JOIN dcc d ON (pipt.leader_project = d.project)
  LEFT JOIN subject_species ss ON (ss.subject = s.nid)
  LEFT JOIN subject_role_taxonomy srt ON (s.nid = srt.subject)
  LEFT JOIN (
    biosample_from_subject bfs
    JOIN biosample b ON (bfs.biosample = b.nid)
    LEFT JOIN (
      file_describes_biosample fdb
      JOIN file f ON (fdb.file = f.nid)
    ) ON (b.nid = fdb.biosample)
  ) ON (s.nid = bfs.subject)
  GROUP BY s.nid, s.id_namespace, json_array(s.project)
;
CREATE INDEX IF NOT EXISTS subject_facts_combo_idx ON subject_facts(
    id_namespace,
    project,
    subject_granularity,
    anatomy,
    assay_type,
    file_format,
    data_type,
    mime_type,
    projects,
    dccs,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    project,
    subject_granularity,
    anatomy,
    assay_type,
    file_format,
    data_type,
    mime_type,
    id_namespace_row,
    project_row,
    subject_granularity_row,
    anatomy_row,
    assay_type_row,
    file_format_row,
    data_type_row,
    mime_type_row,
    projects,
    dccs,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    data_types,
    mime_types
)
  SELECT DISTINCT
    sf.id_namespace,
    sf.project,
    sf.subject_granularity,
    sf.anatomy,
    sf.assay_type,
    sf.file_format,
    sf.data_type,
    sf.mime_type,
    json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
    json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
    json_object('nid', sg.nid, 'name', sg.name, 'description', sg.description) AS subject_granularity_row,
    json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
    json_object('nid', "at".nid, 'name', "at".name, 'description', "at".description) AS assay_type_row,
    json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
    json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
    json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row,
    sf.projects,
    sf.dccs,
    sf.subject_roles,
    sf.subject_granularities,
    sf.subject_species,
    sf.ncbi_taxons,
    sf.anatomies,
    sf.assay_types,
    sf.file_formats,
    sf.data_types,
    sf.mime_types
  FROM subject_facts sf
  JOIN id_namespace n ON (sf.id_namespace = n.nid)
  JOIN project p ON (sf.project = p.nid)
  LEFT JOIN subject_granularity sg ON (sf.subject_granularity = sg.nid)
  LEFT JOIN anatomy a ON (sf.anatomy = a.nid)
  LEFT JOIN assay_type "at" ON (sf.assay_type = "at".nid)
  LEFT JOIN file_format fmt ON (sf.file_format = fmt.nid)
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
  AND sf.project = cf.project
  -- sqlite "IS" operator for IS NOT DISTINCT FROM
  AND sf.subject_granularity IS cf.subject_granularity
  AND sf.projects = cf.projects
  AND sf.dccs = cf.dccs
  AND sf.anatomy IS cf.anatomy
  AND sf.assay_type IS cf.assay_type
  AND sf.file_format IS cf.file_format
  AND sf.data_type IS cf.data_type
  AND sf.mime_type IS cf.mime_type
  AND sf.subject_roles = cf.subject_roles
  AND sf.subject_granularities = cf.subject_granularities
  AND sf.subject_species = cf.subject_species
  AND sf.ncbi_taxons = cf.ncbi_taxons
  AND sf.anatomies = cf.anatomies
  AND sf.assay_types = cf.assay_types
  AND sf.file_formats = cf.file_formats
  AND sf.data_types = cf.data_types
  AND sf.mime_types = cf.mime_types
;

CREATE TEMPORARY TABLE collection_facts AS
SELECT
  nid,
  id_namespace,
  CASE WHEN (SELECT count(*) FROM json_each(s.projects)) = 1 THEN (SELECT j.value FROM json_each(s.projects) j) ELSE NULL END AS project,
  CASE WHEN (SELECT count(*) FROM json_each(s.subject_granularities)) = 1 THEN (SELECT j.value FROM json_each(s.subject_granularities) j) ELSE NULL END AS subject_granularity,
  CASE WHEN (SELECT count(*) FROM json_each(s.anatomies)) = 1 THEN (SELECT j.value FROM json_each(s.anatomies) j) ELSE NULL END AS anatomy,
  CASE WHEN (SELECT count(*) FROM json_each(s.assay_types)) = 1 THEN (SELECT j.value FROM json_each(s.assay_types) j) ELSE NULL END AS assay_type,
  CASE WHEN (SELECT count(*) FROM json_each(s.file_formats)) = 1 THEN (SELECT j.value FROM json_each(s.file_formats) j) ELSE NULL END AS file_format,
  CASE WHEN (SELECT count(*) FROM json_each(s.data_types)) = 1 THEN (SELECT j.value FROM json_each(s.data_types) j) ELSE NULL END AS data_type,
  CASE WHEN (SELECT count(*) FROM json_each(s.mime_types)) = 1 THEN (SELECT j.value FROM json_each(s.mime_types) j) ELSE NULL END AS mime_type,
  projects,
  dccs,
  subject_roles,
  subject_granularities,
  subject_species,
  ncbi_taxons,
  anatomies,
  assay_types,
  file_formats,
  data_types,
  mime_types
FROM (
  SELECT
    col.nid,
    col.id_namespace,
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
    project,
    subject_granularity,
    anatomy,
    assay_type,
    file_format,
    data_type,
    mime_type,
    projects,
    dccs,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    data_types,
    mime_types
);
INSERT INTO core_fact (
    id_namespace,
    project,
    subject_granularity,
    anatomy,
    assay_type,
    file_format,
    data_type,
    mime_type,
    id_namespace_row,
    project_row,
    subject_granularity_row,
    anatomy_row,
    assay_type_row,
    file_format_row,
    data_type_row,
    mime_type_row,
    projects,
    dccs,
    subject_roles,
    subject_granularities,
    subject_species,
    ncbi_taxons,
    anatomies,
    assay_types,
    file_formats,
    data_types,
    mime_types
)
  SELECT DISTINCT
    colf.id_namespace,
    colf.project,
    colf.subject_granularity,
    colf.anatomy,
    colf.assay_type,
    colf.file_format,
    colf.data_type,
    colf.mime_type,
    json_object('nid', n.nid, 'name', n.name, 'description', n.description) AS id_namespace_row,
    json_object('nid', p.nid, 'name', p.name, 'description', p.description) AS project_row,
    json_object('nid', sg.nid, 'name', sg.name, 'description', sg.description) AS subject_granularity_row,
    json_object('nid', a.nid, 'name', a.name, 'description', a.description) AS anatomy_row,
    json_object('nid', "at".nid, 'name', "at".name, 'description', "at".description) AS assay_type_row,
    json_object('nid', fmt.nid, 'name', fmt.name, 'description', fmt.description) AS file_format_row,
    json_object('nid', dt.nid, 'name', dt.name, 'description', dt.description) AS data_type_row,
    json_object('nid', mt.nid, 'name', mt.name, 'description', mt.description) AS mime_type_row,
    colf.projects,
    colf.dccs,
    colf.subject_roles,
    colf.subject_granularities,
    colf.subject_species,
    colf.ncbi_taxons,
    colf.anatomies,
    colf.assay_types,
    colf.file_formats,
    colf.data_types,
    colf.mime_types
  FROM collection_facts colf
  JOIN id_namespace n ON (colf.id_namespace = n.nid)
  LEFT JOIN project p ON (colf.project = p.nid)
  LEFT JOIN subject_granularity sg ON (colf.subject_granularity = sg.nid)
  LEFT JOIN anatomy a ON (colf.anatomy = a.nid)
  LEFT JOIN assay_type "at" ON (colf.assay_type = "at".nid)
  LEFT JOIN file_format fmt ON (colf.file_format = fmt.nid)
  LEFT JOIN data_type dt ON (colf.data_type = dt.nid)
  LEFT JOIN mime_type mt ON (colf.mime_type = mt.nid)  WHERE True
  ON CONFLICT DO NOTHING
;
UPDATE collection AS u
SET core_fact = cf.nid
FROM collection_facts colf, core_fact cf
WHERE u.nid = colf.nid
  AND colf.id_namespace = cf.id_namespace
  -- sqlite "IS" operator for IS NOT DISTINCT FROM
  AND colf.project IS cf.project
  AND colf.subject_granularity IS cf.subject_granularity
  AND colf.anatomy IS cf.anatomy
  AND colf.assay_type IS cf.assay_type
  AND colf.file_format IS cf.file_format
  AND colf.data_type IS cf.data_type
  AND colf.mime_type IS cf.mime_type
  AND colf.projects = cf.projects
  AND colf.dccs = cf.dccs
  AND colf.subject_roles = cf.subject_roles
  AND colf.subject_granularities = cf.subject_granularities
  AND colf.subject_species = cf.subject_species
  AND colf.ncbi_taxons = cf.ncbi_taxons
  AND colf.anatomies = cf.anatomies
  AND colf.assay_types = cf.assay_types
  AND colf.file_formats = cf.file_formats
  AND colf.data_types = cf.data_types
  AND colf.mime_types = cf.mime_types
;
