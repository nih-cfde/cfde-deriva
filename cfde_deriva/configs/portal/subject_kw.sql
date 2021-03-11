WITH s AS (
  SELECT
    "RID",
    json_group_array(DISTINCT kw) AS kw
  FROM (

    -- id_namespace
    SELECT "RID", id_namespace AS kw FROM subject
    UNION

    SELECT s."RID", n."name" AS kw
    FROM subject s
    JOIN id_namespace n ON (s.id_namespace = n.id)
    UNION

    SELECT s."RID", n.abbreviation AS kw
    FROM subject s
    JOIN id_namespace n ON (s.id_namespace = n.id)
    WHERE n.abbreviation IS NOT NULL
    UNION

    -- local_id
    SELECT "RID", local_id AS kw FROM subject
    UNION

    -- persistent_id
    SELECT "RID", persistent_id AS kw FROM subject WHERE persistent_id IS NOT NULL
    UNION

    -- project_id_namespace, project_local_id
    SELECT "RID", project_id_namespace AS kw FROM subject
    UNION

    SELECT "RID", project_local_id AS kw FROM subject
    UNION

    SELECT s."RID", p."name" AS kw
    FROM subject s
    JOIN project p ON (s.project_id_namespace = p.id_namespace AND s.project_local_id = p.local_id)
    UNION

    SELECT s."RID", p.description AS kw
    FROM subject s
    JOIN project p ON (s.project_id_namespace = p.id_namespace AND s.project_local_id = p.local_id)
    WHERE p.description IS NOT NULL
    UNION

    -- ... super project facet
    SELECT DISTINCT s."RID", pip.leader_project_id_namespace AS kw
    FROM subject s
    JOIN project_in_project_transitive pip ON (s.project_id_namespace = pip.member_project_id_namespace AND s.project_local_id = pip.member_project_local_id)
    UNION

    SELECT DISTINCT s."RID", pip.leader_project_local_id AS kw
    FROM subject s
    JOIN project_in_project_transitive pip ON (s.project_id_namespace = pip.member_project_id_namespace AND s.project_local_id = pip.member_project_local_id)
    UNION

    SELECT DISTINCT s."RID", p."name" AS kw
    FROM subject s
    JOIN project_in_project_transitive pip ON (s.project_id_namespace = pip.member_project_id_namespace AND s.project_local_id = pip.member_project_local_id)
    JOIN project p ON (p.id_namespace = pip.leader_project_id_namespace AND p.local_id = pip.leader_project_local_id)
    UNION

    -- granularity
    SELECT "RID", granularity AS kw FROM subject WHERE granularity IS NOT NULL
    UNION

    SELECT s."RID", t."name" AS kw
    FROM subject s
    JOIN subject_granularity t ON (s.granularity = t.id)
    UNION

    -- ... subject taxonomy facet
    SELECT s."RID", srt.taxonomy_id AS kw
    FROM subject s
    JOIN subject_role_taxonomy srt ON (srt.subject_id_namespace = s.id_namespace AND srt.subject_local_id = s.local_id)
    UNION

    SELECT s."RID", t."name" AS kw
    FROM subject s
    JOIN subject_role_taxonomy srt ON (srt.subject_id_namespace = s.id_namespace AND srt.subject_local_id = s.local_id)
    JOIN ncbi_taxonomy t ON (srt.taxonomy_id = t.id)
    UNION

    -- ... subject role facet
    SELECT s."RID", srt.role_id AS kw
    FROM subject s
    JOIN subject_role_taxonomy srt ON (srt.subject_id_namespace = s.id_namespace AND srt.subject_local_id = s.local_id)
    UNION

    SELECT s."RID", t."name" AS kw
    FROM subject s
    JOIN subject_role_taxonomy srt ON (srt.subject_id_namespace = s.id_namespace AND srt.subject_local_id = s.local_id)
    JOIN subject_role t ON (srt.role_id = t.id)
    UNION

    -- ... file_format facet
    SELECT s."RID", file_format AS kw
    FROM subject s
    JOIN biosample_from_subject bfs ON (s.id_namespace = bfs.subject_id_namespace AND s.local_id = bfs.subject_local_id)
    JOIN file_describes_biosample fdb USING (biosample_id_namespace, biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    WHERE file_format IS NOT NULL
    UNION

    SELECT s."RID", t."name" AS kw
    FROM subject s
    JOIN biosample_from_subject bfs ON (s.id_namespace = bfs.subject_id_namespace AND s.local_id = bfs.subject_local_id)
    JOIN file_describes_biosample fdb USING (biosample_id_namespace, biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    JOIN file_format t ON (f.file_format = t.id)
    UNION

    -- ... data_type facet
    SELECT s."RID", data_type AS kw
    FROM subject s
    JOIN biosample_from_subject bfs ON (s.id_namespace = bfs.subject_id_namespace AND s.local_id = bfs.subject_local_id)
    JOIN file_describes_biosample fdb USING (biosample_id_namespace, biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    WHERE data_type IS NOT NULL
    UNION

    SELECT s."RID", t."name" AS kw
    FROM subject s
    JOIN biosample_from_subject bfs ON (s.id_namespace = bfs.subject_id_namespace AND s.local_id = bfs.subject_local_id)
    JOIN file_describes_biosample fdb USING (biosample_id_namespace, biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    JOIN data_type t ON (f.data_type = t.id)
    UNION

    -- ... assay_type facet
    SELECT s."RID", assay_type AS kw
    FROM subject s
    JOIN biosample_from_subject bfs ON (s.id_namespace = bfs.subject_id_namespace AND s.local_id = bfs.subject_local_id)
    JOIN file_describes_biosample fdb USING (biosample_id_namespace, biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    WHERE assay_type IS NOT NULL
    UNION

    SELECT s."RID", t."name" AS kw
    FROM subject s
    JOIN biosample_from_subject bfs ON (s.id_namespace = bfs.subject_id_namespace AND s.local_id = bfs.subject_local_id)
    JOIN file_describes_biosample fdb USING (biosample_id_namespace, biosample_local_id)
    JOIN file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
    JOIN assay_type t ON (f.assay_type = t.id)
    UNION

    -- ... anatomy facet
    SELECT s."RID", anatomy AS kw
    FROM subject s
    JOIN biosample_from_subject bfs ON (s.id_namespace = bfs.subject_id_namespace AND s.local_id = bfs.subject_local_id)
    JOIN biosample b ON (bfs.biosample_id_namespace = b.id_namespace AND bfs.biosample_local_id = b.local_id)
    WHERE anatomy IS NOT NULL
    UNION

    SELECT s."RID", t."name" AS kw
    FROM subject s
    JOIN biosample_from_subject bfs ON (s.id_namespace = bfs.subject_id_namespace AND s.local_id = bfs.subject_local_id)
    JOIN biosample b ON (bfs.biosample_id_namespace = b.id_namespace AND bfs.biosample_local_id = b.local_id)
    JOIN anatomy t ON (b.anatomy = t.id)
    UNION

    -- ... part of collection facet
    SELECT DISTINCT s."RID", cic.leader_collection_id_namespace AS kw
    FROM subject s
    JOIN subject_in_collection sic ON (s.id_namespace = sic.subject_id_namespace AND s.local_id = sic.subject_local_id)
    JOIN collection_in_collection_transitive cic ON (sic.collection_id_namespace = cic.member_collection_id_namespace AND sic.collection_local_id = cic.member_collection_local_id)
    UNION

    SELECT DISTINCT s."RID", cic.leader_collection_local_id AS kw
    FROM subject s
    JOIN subject_in_collection sic ON (s.id_namespace = sic.subject_id_namespace AND s.local_id = sic.subject_local_id)
    JOIN collection_in_collection_transitive cic ON (sic.collection_id_namespace = cic.member_collection_id_namespace AND sic.collection_local_id = cic.member_collection_local_id)
    UNION

    SELECT DISTINCT s."RID", c."name" AS kw
    FROM subject s
    JOIN subject_in_collection sic ON (s.id_namespace = sic.subject_id_namespace AND s.local_id = sic.subject_local_id)
    JOIN collection_in_collection_transitive cic ON (sic.collection_id_namespace = cic.member_collection_id_namespace AND sic.collection_local_id = cic.member_collection_local_id)
    JOIN collection c ON (cic.leader_collection_id_namespace = c.id_namespace AND cic.leader_collection_local_id = c.local_id)

  ) s
  GROUP BY "RID"
)
UPDATE subject AS v
SET kw = s.kw
FROM s
WHERE v."RID" = s."RID"
;
