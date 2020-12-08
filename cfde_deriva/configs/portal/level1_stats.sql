-- sub-queries for each summarized entity-type (file, biosample, subject) to get stats
-- awkward outer joins and remapping to merge combinations with null metadata fields
-- map NULLs as '' for merge, '' back to NULL for output
-- deal with all possible null core entity combos for shared cols
WITH fstats AS (
  -- file stats need file table rows, find optional remote metadata
  SELECT
    root_project_id_namespace,
    root_project_local_id,
    project_id_namespace,
    project_local_id,
    COALESCE(assay_type_id, '') AS assay_type_id,
    COALESCE(data_type_id, '') AS data_type_id,
    COALESCE(file_format_id, '') AS file_format_id,
    COALESCE(anatomy_id, '') AS anatomy_id,
    COALESCE(granularity_id, '') AS granularity_id,
    COALESCE(species_id, '') AS species_id,
    count(*) AS num_files,
    sum(num_bytes) AS num_bytes
  FROM (
    SELECT DISTINCT
      pr.project_id_namespace AS root_project_id_namespace,
      pr.project_local_id AS root_project_local_id,
      f.project_id_namespace AS project_id_namespace,
      f.project_local_id AS project_local_id,
      f.assay_type assay_type_id,
      f.data_type AS data_type_id,
      f.file_format AS file_format_id,
      b.anatomy AS anatomy_id,
      s.granularity AS granularity_id,
      ss.species AS species_id,
      f."RID" AS files_rid,
      f.size_in_bytes AS num_bytes
    FROM file f
    JOIN project_in_project_transitive pipt
      ON (    f.project_id_namespace = pipt.member_project_id_namespace
          AND f.project_locaL_id = pipt.member_project_local_id)
    JOIN project_root pr
      ON (    pipt.leader_project_id_namespace = pr.project_id_namespace
          AND pipt.leader_project_local_id = pr.project_local_id)
    LEFT JOIN file_describes_biosample fdb
      ON (    f.id_namespace = fdb.file_id_namespace
          AND f.local_id = fdb.file_local_id)
    LEFT JOIN biosample b
      ON (    fdb.biosample_id_namespace = b.id_namespace
          AND fdb.biosample_local_id = b.local_id)
    LEFT JOIN biosample_from_subject bfs
      ON (    bfs.biosample_id_namespace = b.id_namespace
          AND bfs.biosample_local_id = b.local_id)
    LEFT JOIN subject s
      ON (    bfs.subject_id_namespace = s.id_namespace
          AND bfs.subject_local_id = s.local_id)
    LEFT JOIN subject_species ss
      ON (    s.id_namespace = ss.subject_id_namespace
          AND s.local_id = ss.subject_local_id)
    ) s
  GROUP BY
    root_project_id_namespace,
    root_project_local_id,
    project_id_namespace,
    project_local_id,
    assay_type_id,
    data_type_id,
    file_format_id,
    anatomy_id,
    granularity_id,
    species_id
), bstats AS (
  -- biosample stats need biosample table rows, find optional remote metadata
  SELECT
    pr.project_id_namespace AS root_project_id_namespace,
    pr.project_local_id AS root_project_local_id,
    b.project_id_namespace AS project_id_namespace,
    b.project_local_id AS project_local_id,
    COALESCE(f.assay_type, '') AS assay_type_id,
    COALESCE(f.data_type, '') AS data_type_id,
    COALESCE(f.file_format, '') AS file_format_id,
    COALESCE(b.anatomy, '') AS anatomy_id,
    COALESCE(s.granularity, '') AS granularity_id,
    COALESCE(ss.species, '') AS species_id,
    count(DISTINCT b."RID") AS num_biosamples,
    sum(f.size_in_bytes) AS num_bytes
  FROM biosample b
  JOIN project_in_project_transitive pipt
    ON (    b.project_id_namespace = pipt.member_project_id_namespace
        AND b.project_locaL_id = pipt.member_project_local_id)
  JOIN project_root pr
    ON (    pipt.leader_project_id_namespace = pr.project_id_namespace
        AND pipt.leader_project_local_id = pr.project_local_id)
  LEFT JOIN biosample_from_subject bfs
    ON (    bfs.biosample_id_namespace = b.id_namespace
        AND bfs.biosample_local_id = b.local_id)
  LEFT JOIN subject s
    ON (    bfs.subject_id_namespace = s.id_namespace
        AND bfs.subject_local_id = s.local_id)
  LEFT JOIN subject_species ss
    ON (    s.id_namespace = ss.subject_id_namespace
        AND s.local_id = ss.subject_local_id)
  LEFT JOIN file_describes_biosample fdb
      ON (    fdb.biosample_id_namespace = b.id_namespace
        AND fdb.biosample_local_id = b.local_id)
  LEFT JOIN file f
    ON (    f.id_namespace = fdb.file_id_namespace
        AND f.local_id = fdb.file_local_id)
  GROUP BY
    pr.project_id_namespace,
    pr.project_local_id,
    b.project_id_namespace,
    b.project_local_id,
    f.assay_type,
    f.data_type,
    f.file_format,
    b.anatomy,
    s.granularity,
    ss.species
), sstats AS (
  -- subject stats need subject table rows, find optional remote metadata
  SELECT
    pr.project_id_namespace AS root_project_id_namespace,
    pr.project_local_id AS root_project_local_id,
    s.project_id_namespace AS project_id_namespace,
    s.project_local_id AS project_local_id,
    COALESCE(f.assay_type, '') AS assay_type_id,
    COALESCE(f.data_type, '') AS data_type_id,
    COALESCE(f.file_format, '') AS file_format_id,
    COALESCE(b.anatomy, '') AS anatomy_id,
    COALESCE(s.granularity, '') AS granularity_id,
    COALESCE(ss.species, '') AS species_id,
    count(DISTINCT s."RID") AS num_subjects,
    sum(f.size_in_bytes) AS num_bytes
  FROM subject s
  JOIN project_in_project_transitive pipt
    ON (    s.project_id_namespace = pipt.member_project_id_namespace
        AND s.project_locaL_id = pipt.member_project_local_id)
  JOIN project_root pr
    ON (    pipt.leader_project_id_namespace = pr.project_id_namespace
        AND pipt.leader_project_local_id = pr.project_local_id)
  LEFT JOIN subject_species ss
    ON (    s.id_namespace = ss.subject_id_namespace
        AND s.local_id = ss.subject_local_id)
  LEFT JOIN biosample_from_subject bfs
    ON (    bfs.subject_id_namespace = s.id_namespace
        AND bfs.subject_local_id = s.local_id)
  LEFT JOIN biosample b
    ON (    bfs.biosample_id_namespace = b.id_namespace
        AND bfs.biosample_local_id = b.local_id)
  LEFT JOIN file_describes_biosample fdb
    ON (    fdb.biosample_id_namespace = b.id_namespace
        AND fdb.biosample_local_id = b.local_id)
  LEFT JOIN file f
    ON (    f.id_namespace = fdb.file_id_namespace
        AND f.local_id = fdb.file_local_id)
  GROUP BY
    pr.project_id_namespace,
    pr.project_local_id,
    s.project_id_namespace,
    s.project_local_id,
    f.assay_type,
    f.data_type,
    f.file_format,
    b.anatomy,
    s.granularity,
    ss.species
), allcombos AS (
  SELECT
    root_project_id_namespace,
    root_project_local_id,
    project_id_namespace,
    project_local_id,
    assay_type_id,
    data_type_id,
    file_format_id,
    anatomy_id,
    granularity_id,
    species_id
  FROM fstats

  UNION

  SELECT
    root_project_id_namespace,
    root_project_local_id,
    project_id_namespace,
    project_local_id,
    assay_type_id,
    data_type_id,
    file_format_id,
    anatomy_id,
    granularity_id,
    species_id
  FROM bstats

  UNION

  SELECT
    root_project_id_namespace,
    root_project_local_id,
    project_id_namespace,
    project_local_id,
    assay_type_id,
    data_type_id,
    file_format_id,
    anatomy_id,
    granularity_id,
    species_id
  FROM sstats
)
INSERT INTO level1_stats (
  root_project_id_namespace,
  root_project_local_id,
  project_id_namespace,
  project_local_id,
  assay_type_id,
  data_type_id,
  file_format_id,
  anatomy_id,
  granularity_id,
  species_id,
  num_files,
  num_bytes,
  num_biosamples,
  num_subjects
)
SELECT
  a.root_project_id_namespace,
  a.root_project_local_id,
  a.project_id_namespace,
  a.project_local_id,
  CASE WHEN a.assay_type_id = '' THEN NULL ELSE a.assay_type_id END,
  CASE WHEN a.data_type_id = '' THEN NULL ELSE a.data_type_id END,
  CASE WHEN a.file_format_id = '' THEN NULL ELSE a.file_format_id END,
  CASE WHEN a.anatomy_id = '' THEN NULL ELSE a.anatomy_id END,
  CASE WHEN a.granularity_id = '' THEN NULL ELSE a.granularity_id END,
  CASE WHEN a.species_id = '' THEN NULL ELSE a.species_id END,
  fstats.num_files,
  fstats.num_bytes,
  bstats.num_biosamples,
  sstats.num_subjects
FROM allcombos a
LEFT JOIN fstats USING (
  project_id_namespace,
  project_local_id,
  assay_type_id,
  data_type_id,
  file_format_id,
  anatomy_id,
  granularity_id,
  species_id
)
LEFT JOIN bstats USING (
  project_id_namespace,
  project_local_id,
  assay_type_id,
  data_type_id,
  file_format_id,
  anatomy_id,
  granularity_id,
  species_id
)
LEFT JOIN sstats USING (
  project_id_namespace,
  project_local_id,
  assay_type_id,
  data_type_id,
  file_format_id,
  anatomy_id,
  granularity_id,
  species_id
)
;
