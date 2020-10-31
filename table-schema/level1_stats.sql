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
-- sub-queries for each summarized entity-type (file, biosample, subject) to get stats
-- awkward outer joins and remapping to merge combinations with null metadata fields
-- map NULLs as '' for merge, '' back to NULL for output
-- deal with all possible null core entity combos for shared cols
SELECT
  COALESCE(
    fstats.root_project_id_namespace,
    bstats.root_project_id_namespace,
    sstats.root_project_id_namespace
  ) AS root_project_id_namespace,
  COALESCE(
    fstats.root_project_local_id,
    bstats.root_project_local_id,
    sstats.root_project_local_id
  ) AS root_project_local_id,
  COALESCE(
    fstats.project_id_namespace,
    bstats.project_id_namespace,
    sstats.project_id_namespace
  ) AS project_id_namespace,
  COALESCE(
    fstats.project_local_id,
    bstats.project_local_id,
    sstats.project_local_id
  ) AS project_local_id,
  COALESCE(
    CASE WHEN fstats.assay_type_id = '' THEN NULL ELSE fstats.assay_type_id END,
    CASE WHEN bstats.assay_type_id = '' THEN NULL ELSE bstats.assay_type_id END,
    CASE WHEN sstats.assay_type_id = '' THEN NULL ELSE sstats.assay_type_id END
  ) AS assay_type_id,
  COALESCE(
    CASE WHEN fstats.data_type_id = '' THEN NULL ELSE fstats.data_type_id END,
    CASE WHEN bstats.data_type_id = '' THEN NULL ELSE bstats.data_type_id END,
    CASE WHEN sstats.data_type_id = '' THEN NULL ELSE sstats.data_type_id END
  ) AS data_type_id,
  COALESCE(
    CASE WHEN fstats.file_format_id = '' THEN NULL ELSE fstats.file_format_id END,
    CASE WHEN bstats.file_format_id = '' THEN NULL ELSE bstats.file_format_id END,
    CASE WHEN sstats.file_format_id = '' THEN NULL ELSE sstats.file_format_id END
  ) AS file_format_id,
  COALESCE(
    CASE WHEN fstats.anatomy_id = '' THEN NULL ELSE fstats.anatomy_id END,
    CASE WHEN bstats.anatomy_id = '' THEN NULL ELSE bstats.anatomy_id END,
    CASE WHEN sstats.anatomy_id = '' THEN NULL ELSE sstats.anatomy_id END
  ) AS anatomy_id,
  COALESCE(
    CASE WHEN fstats.granularity_id = '' THEN NULL ELSE fstats.granularity_id END,
    CASE WHEN bstats.granularity_id = '' THEN NULL ELSE bstats.granularity_id END,
    CASE WHEN sstats.granularity_id = '' THEN NULL ELSE sstats.granularity_id END
  ) AS granularity_id,
  COALESCE(
    CASE WHEN fstats.species_id = '' THEN NULL ELSE fstats.species_id END,
    CASE WHEN bstats.species_id = '' THEN NULL ELSE bstats.species_id END,
    CASE WHEN sstats.species_id = '' THEN NULL ELSE sstats.species_id END
  ) AS species_id,
  fstats.num_files,
  fstats.num_bytes,
  bstats.num_biosamples,
  sstats.num_subjects
FROM ( (
  -- file stats need file table rows, find optional remote metadata
  SELECT
    pr.project_id_namespace AS root_project_id_namespace,
    pr.project_local_id AS root_project_local_id,
    f.project_id_namespace AS project_id_namespace,
    f.project_local_id AS project_local_id,
    COALESCE(f.assay_type, '') AS assay_type_id,
    COALESCE(f.data_type, '') AS data_type_id,
    COALESCE(f.file_format, '') AS file_format_id,
    COALESCE(b.anatomy, '') AS anatomy_id,
    COALESCE(s.granularity, '') AS granularity_id,
    COALESCE(ss.species, '') AS species_id,
    count(DISTINCT f."RID") AS num_files,
    sum(f.size_in_bytes) AS num_bytes
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
  GROUP BY
    pr.project_id_namespace,
    pr.project_local_id,
    f.project_id_namespace,
    f.project_local_id,
    f.assay_type,
    f.data_type,
    f.file_format,
    b.anatomy,
    s.granularity,
    ss.species
) fstats
FULL OUTER JOIN (
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
  FROM file f
  JOIN file_describes_biosample fdb
    ON (    f.id_namespace = fdb.file_id_namespace
        AND f.local_id = fdb.file_local_id)
  RIGHT JOIN biosample b
    ON (    fdb.biosample_id_namespace = b.id_namespace
        AND fdb.biosample_local_id = b.local_id)
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
) bstats USING (
  project_id_namespace,
  project_local_id,
  assay_type_id,
  data_type_id,
  file_format_id,
  anatomy_id,
  granularity_id,
  species_id
) )
FULL OUTER JOIN (
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
  FROM file f
  JOIN file_describes_biosample fdb
    ON (    f.id_namespace = fdb.file_id_namespace
        AND f.local_id = fdb.file_local_id)
  RIGHT JOIN biosample b
    ON (    fdb.biosample_id_namespace = b.id_namespace
        AND fdb.biosample_local_id = b.local_id)
  RIGHT JOIN biosample_from_subject bfs
    ON (    bfs.biosample_id_namespace = b.id_namespace
        AND bfs.biosample_local_id = b.local_id)
  RIGHT JOIN subject s
    ON (    bfs.subject_id_namespace = s.id_namespace
        AND bfs.subject_local_id = s.local_id)
  JOIN project_in_project_transitive pipt
    ON (    s.project_id_namespace = pipt.member_project_id_namespace
        AND s.project_locaL_id = pipt.member_project_local_id)
  JOIN project_root pr
    ON (    pipt.leader_project_id_namespace = pr.project_id_namespace
        AND pipt.leader_project_local_id = pr.project_local_id)
  LEFT JOIN subject_species ss
    ON (    s.id_namespace = ss.subject_id_namespace
        AND s.local_id = ss.subject_local_id)
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
) sstats USING (
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
