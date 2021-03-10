WITH s AS (
  SELECT
    "RID",
    json_group_array(DISTINCT kw) AS kw
  FROM (

    -- id_namespace
    SELECT "RID", id_namespace AS kw FROM biosample
    UNION

    SELECT b."RID", n."name" AS kw
    FROM biosample b
    JOIN id_namespace n ON (b.id_namespace = n.id)
    UNION

    SELECT b."RID", n.abbreviation AS kw
    FROM biosample b
    JOIN id_namespace n ON (b.id_namespace = n.id)
    WHERE n.abbreviation IS NOT NULL
    UNION

    -- local_id
    SELECT "RID", local_id AS kw FROM biosample
    UNION

    -- persistent_id
    SELECT "RID", persistent_id AS kw FROM biosample WHERE persistent_id IS NOT NULL
    UNION

    -- project_id_namespace, project_local_id
    SELECT "RID", project_id_namespace AS kw FROM biosample
    UNION

    SELECT "RID", project_local_id AS kw FROM biosample
    UNION

    SELECT b."RID", p."name" AS kw
    FROM biosample b
    JOIN project p ON (b.project_id_namespace = p.id_namespace AND b.project_local_id = p.local_id)
    UNION

    SELECT b."RID", p.description AS kw
    FROM biosample b
    JOIN project p ON (b.project_id_namespace = p.id_namespace AND b.project_local_id = p.local_id)
    WHERE p.description IS NOT NULL
    UNION

    -- ... super project facet
    SELECT DISTINCT b."RID", pip.leader_project_id_namespace AS kw
    FROM biosample b
    JOIN project_in_project_transitive pip ON (b.project_id_namespace = pip.member_project_id_namespace AND b.project_local_id = pip.member_project_local_id)
    UNION

    SELECT DISTINCT b."RID", pip.leader_project_local_id AS kw
    FROM biosample b
    JOIN project_in_project_transitive pip ON (b.project_id_namespace = pip.member_project_id_namespace AND b.project_local_id = pip.member_project_local_id)
    UNION

    SELECT DISTINCT b."RID", p."name" AS kw
    FROM biosample b
    JOIN project_in_project_transitive pip ON (b.project_id_namespace = pip.member_project_id_namespace AND b.project_local_id = pip.member_project_local_id)
    JOIN project p ON (p.id_namespace = pip.leader_project_id_namespace AND p.local_id = pip.leader_project_local_id)
    UNION

    -- anatomy
    SELECT "RID", anatomy AS kw FROM biosample WHERE anatomy IS NOT NULL
    UNION

    SELECT b."RID", t."name" AS kw
    FROM biosample b
    JOIN anatomy t ON (b.anatomy = t.id)
    UNION

    -- ... subject taxonomy facet
    SELECT b."RID", srt.taxonomy_id AS kw
    FROM biosample b
    JOIN biosample_from_subject bfs ON (b.id_namespace = bfs.biosample_id_namespace AND b.local_id = bfs.biosample_local_id)
    JOIN subject_role_taxonomy srt ON (bfs.subject_id_namespace = srt.subject_id_namespace AND bfs.subject_local_id = srt.subject_local_id)
    UNION

    SELECT b."RID", t."name" AS kw
    FROM biosample b
    JOIN biosample_from_subject bfs ON (b.id_namespace = bfs.biosample_id_namespace AND b.local_id = bfs.biosample_local_id)
    JOIN subject_role_taxonomy srt ON (bfs.subject_id_namespace = srt.subject_id_namespace AND bfs.subject_local_id = srt.subject_local_id)
    JOIN ncbi_taxonomy t ON (srt.taxonomy_id = t.id)
    UNION

    -- ... subject role facet
    SELECT b."RID", srt.role_id AS kw
    FROM biosample b
    JOIN biosample_from_subject bfs ON (b.id_namespace = bfs.biosample_id_namespace AND b.local_id = bfs.biosample_local_id)
    JOIN subject_role_taxonomy srt ON (bfs.subject_id_namespace = srt.subject_id_namespace AND bfs.subject_local_id = srt.subject_local_id)
    UNION

    SELECT b."RID", t."name" AS kw
    FROM biosample b
    JOIN biosample_from_subject bfs ON (b.id_namespace = bfs.biosample_id_namespace AND b.local_id = bfs.biosample_local_id)
    JOIN subject_role_taxonomy srt ON (bfs.subject_id_namespace = srt.subject_id_namespace AND bfs.subject_local_id = srt.subject_local_id)
    JOIN subject_role t ON (srt.role_id = t.id)
    UNION

    -- ... subject granularity facet
    SELECT b."RID", s.granularity AS kw
    FROM biosample b
    JOIN biosample_from_subject bfs ON (b.id_namespace = bfs.biosample_id_namespace AND b.local_id = bfs.biosample_local_id)
    JOIN subject s ON (bfs.subject_id_namespace = s.id_namespace AND bfs.subject_local_id = s.local_id)
    WHERE s.granularity IS NOT NULL
    UNION

    SELECT b."RID", t."name" AS kw
    FROM biosample b
    JOIN biosample_from_subject bfs ON (b.id_namespace = bfs.biosample_id_namespace AND b.local_id = bfs.biosample_local_id)
    JOIN subject s ON (bfs.subject_id_namespace = s.id_namespace AND bfs.subject_local_id = s.local_id)
    JOIN subject_granularity t ON (s.granularity = t.id)
    UNION

    -- ... file data-type facet
    SELECT b."RID", f.data_type AS kw
    FROM biosample b
    JOIN file_describes_biosample fdb ON (b.id_namespace = fdb.biosample_id_namespace AND b.local_id = fdb.biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    WHERE f.data_type IS NOT NULL
    UNION

    SELECT b."RID", t."name" AS kw
    FROM biosample b
    JOIN file_describes_biosample fdb ON (b.id_namespace = fdb.biosample_id_namespace AND b.local_id = fdb.biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    JOIN data_type t ON (f.data_type = t.id)
    UNION

    -- ... file assay-type facet
    SELECT b."RID", f.assay_type AS kw
    FROM biosample b
    JOIN file_describes_biosample fdb ON (b.id_namespace = fdb.biosample_id_namespace AND b.local_id = fdb.biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    WHERE f.assay_type IS NOT NULL
    UNION

    SELECT b."RID", t."name" AS kw
    FROM biosample b
    JOIN file_describes_biosample fdb ON (b.id_namespace = fdb.biosample_id_namespace AND b.local_id = fdb.biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    JOIN assay_type t ON (f.assay_type = t.id)
    UNION

    -- ... file file-format facet
    SELECT b."RID", f.file_format AS kw
    FROM biosample b
    JOIN file_describes_biosample fdb ON (b.id_namespace = fdb.biosample_id_namespace AND b.local_id = fdb.biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    WHERE f.file_format IS NOT NULL
    UNION

    SELECT b."RID", t."name" AS kw
    FROM biosample b
    JOIN file_describes_biosample fdb ON (b.id_namespace = fdb.biosample_id_namespace AND b.local_id = fdb.biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    JOIN file_format t ON (f.file_format = t.id)
    UNION

    -- ... part of collection facet
    SELECT DISTINCT b."RID", cic.leader_collection_id_namespace AS kw
    FROM biosample b
    JOIN biosample_in_collection bic ON (b.id_namespace = bic.biosample_id_namespace AND b.local_id = bic.biosample_local_id)
    JOIN collection_in_collection_transitive cic ON (bic.collection_id_namespace = cic.member_collection_id_namespace AND bic.collection_local_id = cic.member_collection_local_id)
    UNION

    SELECT DISTINCT b."RID", cic.leader_collection_local_id AS kw
    FROM biosample b
    JOIN biosample_in_collection bic ON (b.id_namespace = bic.biosample_id_namespace AND b.local_id = bic.biosample_local_id)
    JOIN collection_in_collection_transitive cic ON (bic.collection_id_namespace = cic.member_collection_id_namespace AND bic.collection_local_id = cic.member_collection_local_id)
    UNION

    SELECT DISTINCT b."RID", c."name" AS kw
    FROM biosample b
    JOIN biosample_in_collection bic ON (b.id_namespace = bic.biosample_id_namespace AND b.local_id = bic.biosample_local_id)
    JOIN collection_in_collection_transitive cic ON (bic.collection_id_namespace = cic.member_collection_id_namespace AND bic.collection_local_id = cic.member_collection_local_id)
    JOIN collection c ON (cic.leader_collection_id_namespace = c.id_namespace AND cic.leader_collection_local_id = c.local_id)

  ) s
  GROUP BY "RID"
)
UPDATE biosample AS v
SET kw = s.kw
FROM s
WHERE v."RID" = s."RID"
;
