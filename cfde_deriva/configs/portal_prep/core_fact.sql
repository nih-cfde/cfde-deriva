CREATE TEMPORARY TABLE file_facts AS
  SELECT
    f.nid,
    f.id_namespace,
    json_array(f.project) AS projects,
    json_sorted(json_group_array(DISTINCT d.project)) AS dccs,
    json_sorted(json_group_array(DISTINCT srt."role")) AS subject_roles,
    json_sorted(json_group_array(DISTINCT s.granularity)) AS subject_granularities,
    json_sorted(json_group_array(DISTINCT ss.species)) AS subject_species,
    json_sorted(json_group_array(DISTINCT srt.taxon)) AS ncbi_taxons,
    json_sorted(json_group_array(DISTINCT b.anatomy)) AS anatomies,
    json_sorted(json_group_array(DISTINCT f.assay_type)) AS assay_types,
    json_sorted(json_group_array(DISTINCT f.file_format)) AS file_formats,
    json_sorted(json_group_array(DISTINCT f.data_type)) AS data_types,
    json_sorted(json_group_array(DISTINCT f.mime_type)) AS mime_types
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
  GROUP BY f.nid, f.id_namespace, json_array(f.project)
;
CREATE INDEX IF NOT EXISTS file_facts_combo_idx ON file_facts(
    id_namespace,
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
    id_namespace,
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
  FROM file_facts
  WHERE True
  ON CONFLICT DO NOTHING
;
UPDATE file AS u
SET core_fact = cf.nid
FROM file_facts ff, core_fact cf
WHERE u.nid = ff.nid
  AND ff.id_namespace = cf.id_namespace
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
    json_array(b.project) AS projects,
    json_sorted(json_group_array(DISTINCT d.project)) AS dccs,
    json_sorted(json_group_array(DISTINCT srt."role")) AS subject_roles,
    json_sorted(json_group_array(DISTINCT s.granularity)) AS subject_granularities,
    json_sorted(json_group_array(DISTINCT ss.species)) AS subject_species,
    json_sorted(json_group_array(DISTINCT srt.taxon)) AS ncbi_taxons,
    json_sorted(json_group_array(DISTINCT b.anatomy)) AS anatomies,
    json_sorted(json_group_array(DISTINCT f.assay_type)) AS assay_types,
    json_sorted(json_group_array(DISTINCT f.file_format)) AS file_formats,
    json_sorted(json_group_array(DISTINCT f.data_type)) AS data_types,
    json_sorted(json_group_array(DISTINCT f.mime_type)) AS mime_types
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
  GROUP BY b.nid, b.id_namespace, json_array(b.project)
;
CREATE INDEX IF NOT EXISTS biosample_facts_combo_idx ON biosample_facts(
    id_namespace,
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
    id_namespace,
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
  FROM biosample_facts
  WHERE True
  ON CONFLICT DO NOTHING
;
UPDATE biosample AS u
SET core_fact = cf.nid
FROM biosample_facts bf, core_fact cf
WHERE u.nid = bf.nid
  AND bf.id_namespace = cf.id_namespace
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
    json_array(s.project) AS projects,
    json_sorted(json_group_array(DISTINCT d.project)) AS dccs,
    json_sorted(json_group_array(DISTINCT srt."role")) AS subject_roles,
    json_sorted(json_group_array(DISTINCT s.granularity)) AS subject_granularities,
    json_sorted(json_group_array(DISTINCT ss.species)) AS subject_species,
    json_sorted(json_group_array(DISTINCT srt.taxon)) AS ncbi_taxons,
    json_sorted(json_group_array(DISTINCT b.anatomy)) AS anatomies,
    json_sorted(json_group_array(DISTINCT f.assay_type)) AS assay_types,
    json_sorted(json_group_array(DISTINCT f.file_format)) AS file_formats,
    json_sorted(json_group_array(DISTINCT f.data_type)) AS data_types,
    json_sorted(json_group_array(DISTINCT f.mime_type)) AS mime_types
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
    id_namespace,
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
  FROM subject_facts
  WHERE True
  ON CONFLICT DO NOTHING
;
UPDATE subject AS u
SET core_fact = cf.nid
FROM subject_facts sf, core_fact cf
WHERE u.nid = sf.nid
  AND sf.id_namespace = cf.id_namespace
  AND sf.projects = cf.projects
  AND sf.dccs = cf.dccs
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
    col.nid,
    col.id_namespace,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT cdbp.project)), '[]')
     FROM collection_defined_by_project cdbp
     WHERE cdbp.collection = col.nid) AS projects,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT cdbp.project)), '[]')
     FROM collection_defined_by_project cdbp
     JOIN project_in_project_transitive pipt ON (cdbp.project = pipt.member_project)
     JOIN dcc d ON (pipt.leader_project = d.project)
     WHERE cdbp.collection = col.nid) AS dccs,
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value)), '[]')
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
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value)), '[]')
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
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value)), '[]')
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
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value)), '[]')
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
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value)), '[]')
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
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value)), '[]')
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
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value)), '[]')
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
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value)), '[]')
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
    (SELECT COALESCE(json_sorted(json_group_array(DISTINCT s.value)), '[]')
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
;
CREATE INDEX IF NOT EXISTS collection_facts_combo_idx ON collection_facts(
    id_namespace,
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
    id_namespace,
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
  FROM collection_facts
  WHERE True
  ON CONFLICT DO NOTHING
;
UPDATE collection AS u
SET core_fact = cf.nid
FROM collection_facts colf, core_fact cf
WHERE u.nid = colf.nid
  AND colf.id_namespace = cf.id_namespace
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
