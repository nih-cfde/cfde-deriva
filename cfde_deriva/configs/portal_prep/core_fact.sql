WITH file_facts(
  nid,
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
) MATERIALIZED AS (
  SELECT
    f.nid,
    f.id_namespace,
    json_group_array(DISTINCT f.project ORDER BY f.project),
    json_group_array(DISTINCT d.project ORDER BY d.project),
    json_group_array(DISTINCT srt."role" ORDER BY srt."role"),
    json_group_array(DISTINCT s.granularity ORDER BY s.granularity),
    json_group_array(DISTINCT ss.species ORDER BY ss.species),
    json_group_array(DISTINCT srt.taxonomy ORDER BY srt.taxon),
    json_group_array(DISTINCT b.anatomy ORDER BY b.anatomy),
    json_group_array(DISTINCT f.assay_type ORDER BY f.assay_type),
    json_group_array(DISTINCT f.file_format ORDER BY f.file_format),
    json_group_array(DISTINCT f.data_type ORDER BY f.data_type),
    json_group_array(DISTINCT f.mime_type ORDER BY m.mime_type)
  FROM file f
  JOIN project_in_project_transitive pipt ON (f.project = pipt.member_project)
  JOIN dccs d ON (pipt.leader_project = d.project)
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
  GROUP BY f.nid, f.id_namespace
), new_facts AS (
  INSERT INTO core_fact
  SELECT DISTINCT
    nid,
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
  ON CONFLICT DO NOTHING
)
UPDATE file u
SET core_fact = cf.nid
FROM file_facts ff, core_facts cf
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

WITH biosample_facts(
  nid,
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
) MATERIALIZED AS (
  SELECT
    b.nid,
    b.id_namespace,
    json_group_array(DISTINCT b.project ORDER BY b.project),
    json_group_array(DISTINCT d.project ORDER BY d.project),
    json_group_array(DISTINCT srt."role" ORDER BY srt."role"),
    json_group_array(DISTINCT s.granularity ORDER BY s.granularity),
    json_group_array(DISTINCT ss.species ORDER BY ss.species),
    json_group_array(DISTINCT srt.taxonomy ORDER BY srt.taxon),
    json_group_array(DISTINCT b.anatomy ORDER BY b.anatomy),
    json_group_array(DISTINCT f.assay_type ORDER BY f.assay_type),
    json_group_array(DISTINCT f.file_format ORDER BY f.file_format),
    json_group_array(DISTINCT f.data_type ORDER BY f.data_type),
    json_group_array(DISTINCT f.mime_type ORDER BY m.mime_type)
  FROM biosample b
  JOIN project_in_project_transitive pipt ON (b.project = pipt.member_project)
  JOIN dccs d ON (pipt.leader_project = d.project)
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
  GROUP BY b.nid, b.id_namespace
), new_facts AS (
  INSERT INTO core_fact
  SELECT DISTINCT
    nid,
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
  ON CONFLICT DO NOTHING
)
UPDATE biosample u
SET core_fact = cf.nid
FROM biosample_facts bf, core_facts cf
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

WITH subject_facts(
  nid,
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
) MATERIALIZED AS (
  SELECT
    s.nid,
    s.id_namespace,
    json_group_array(DISTINCT s.project ORDER BY s.project),
    json_group_array(DISTINCT d.project ORDER BY d.project),
    json_group_array(DISTINCT srt."role" ORDER BY srt."role"),
    json_group_array(DISTINCT s.granularity ORDER BY s.granularity),
    json_group_array(DISTINCT ss.species ORDER BY ss.species),
    json_group_array(DISTINCT srt.taxonomy ORDER BY srt.taxon),
    json_group_array(DISTINCT b.anatomy ORDER BY b.anatomy),
    json_group_array(DISTINCT f.assay_type ORDER BY f.assay_type),
    json_group_array(DISTINCT f.file_format ORDER BY f.file_format),
    json_group_array(DISTINCT f.data_type ORDER BY f.data_type),
    json_group_array(DISTINCT f.mime_type ORDER BY m.mime_type)
  FROM subject s
  JOIN project_in_project_transitive pipt ON (s.project = pipt.member_project)
  JOIN dccs d ON (pipt.leader_project = d.project)
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
), new_facts AS (
  INSERT INTO core_fact
  SELECT DISTINCT
    nid,
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
  ON CONFLICT DO NOTHING
)
UPDATE subject u
SET core_fact = cf.nid
FROM subject_facts sf, core_facts cf
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

WITH collection_facts(
  nid,
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
) AS MATERIALIZED (
  SELECT
    col.nid,
    col.id_namespace,
    (SELECT json_group_array(DISTINCT cdbp.project ORDER BY cdbp.project)
     FROM collection_defined_by_project cdbp
     WHERE cdbp.collection = col.nid),
    (SELECT json_group_array(DISTINCT cdbp.project ORDER BY cdbp.project)
     FROM collection_defined_by_project cdbp
     JOIN project_in_project_transitive pipt ON (cdbp.project = pipt.member_project)
     JOIN dccs d ON (pipt.leader_project = d.project)
     WHERE cdbp.collection = col.nid),
    (SELECT json_group_array(DISTINCT s.e ORDER BY s.e)
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_facts cf, json_each(cf.subject_roles) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_facts cf, json_each(cf.subject_roles) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_facts cf, json_each(cf.subject_roles) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s(e)),
    (SELECT json_group_array(DISTINCT s.e ORDER BY s.e)
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_facts cf, json_each(cf.subject_granularities) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_facts cf, json_each(cf.subject_granularities) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_facts cf, json_each(cf.subject_granularities) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s(e)),
    (SELECT json_group_array(DISTINCT s.e ORDER BY s.e)
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_facts cf, json_each(cf.subject_species) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_facts cf, json_each(cf.subject_species) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_facts cf, json_each(cf.subject_species) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s(e)),
    (SELECT json_group_array(DISTINCT s.e ORDER BY s.e)
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_facts cf, json_each(cf.ncbi_taxons) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_facts cf, json_each(cf.ncbi_taxons) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_facts cf, json_each(cf.ncbi_taxons) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s(e)),
    (SELECT json_group_array(DISTINCT s.e ORDER BY s.e)
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_facts cf, json_each(cf.anatomies) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_facts cf, json_each(cf.anatomies) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_facts cf, json_each(cf.anatomies) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s(e)),
    (SELECT json_group_array(DISTINCT s.e ORDER BY s.e)
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_facts cf, json_each(cf.assay_types) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_facts cf, json_each(cf.assay_types) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_facts cf, json_each(cf.assay_types) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s(e)),
    (SELECT json_group_array(DISTINCT s.e ORDER BY s.e)
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_facts cf, json_each(cf.file_formats) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_facts cf, json_each(cf.file_formats) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_facts cf, json_each(cf.file_formats) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s(e)),
    (SELECT json_group_array(DISTINCT s.e ORDER BY s.e)
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_facts cf, json_each(cf.data_types) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_facts cf, json_each(cf.data_types) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_facts cf, json_each(cf.data_types) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s(e)),
    (SELECT json_group_array(DISTINCT s.e ORDER BY s.e)
     FROM (
       SELECT j.value
       FROM file_in_collection fic, file f, core_facts cf, json_each(cf.mime_types) j
       WHERE fic.collection = col.nid AND fic.file = f.nid AND f.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM biosample_in_collection bic, biosample b, core_facts cf, json_each(cf.mime_types) j
       WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.core_fact = cf.nid
     UNION
       SELECT j.value
       FROM subject_in_collection sic, subject s, core_facts cf, json_each(cf.mime_types) j
       WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.core_fact = cf.nid
     ) s(e))
  FROM collection col
), new_facts AS (
  INSERT INTO core_fact
  SELECT DISTINCT
    nid,
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
  ON CONFLICT DO NOTHING
)
UPDATE collection u
SET core_fact = cf.nid
FROM collection_facts colf, core_facts cf
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
