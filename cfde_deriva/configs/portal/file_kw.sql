WITH s AS (
  SELECT
    "RID",
    json_group_array(DISTINCT kw) AS kw
  FROM (

    -- id_namespace
    SELECT "RID", id_namespace AS kw FROM file
    UNION

    SELECT f."RID", n."name" AS kw
    FROM file f
    JOIN id_namespace n ON (f.id_namespace = n.id)
    UNION

    SELECT f."RID", n.abbreviation AS kw
    FROM file f
    JOIN id_namespace n ON (f.id_namespace = n.id)
    WHERE n.abbreviation IS NOT NULL
    UNION

    -- local_id
    SELECT "RID", local_id AS kw FROM file
    UNION

    -- project_id_namespace, project_local_id
    SELECT "RID", project_id_namespace AS kw FROM file
    UNION

    SELECT "RID", project_local_id AS kw FROM file
    UNION

    SELECT f."RID", p."name" AS kw
    FROM file f
    JOIN project p ON (f.project_id_namespace = p.id_namespace AND f.project_local_id = p.local_id)
    UNION

    SELECT f."RID", p.description AS kw
    FROM file f
    JOIN project p ON (f.project_id_namespace = p.id_namespace AND f.project_local_id = p.local_id)
    WHERE p.description IS NOT NULL
    UNION

    -- file ... super project facet
    SELECT DISTINCT f."RID", pip.leader_project_id_namespace AS kw
    FROM file f
    JOIN project_in_project_transitive pip ON (f.project_id_namespace = pip.member_project_id_namespace AND f.project_local_id = pip.member_project_local_id)
    UNION

    SELECT DISTINCT f."RID", pip.leader_project_local_id AS kw
    FROM file f
    JOIN project_in_project_transitive pip ON (f.project_id_namespace = pip.member_project_id_namespace AND f.project_local_id = pip.member_project_local_id)
    UNION

    SELECT DISTINCT f."RID", p."name" AS kw
    FROM file f
    JOIN project_in_project_transitive pip ON (f.project_id_namespace = pip.member_project_id_namespace AND f.project_local_id = pip.member_project_local_id)
    JOIN project p ON (p.id_namespace = pip.leader_project_id_namespace AND p.local_id = pip.leader_project_local_id)
    UNION

    -- filename
    SELECT "RID", filename AS kw FROM file WHERE filename IS NOT NULL
    UNION

    -- file_format
    SELECT "RID", file_format AS kw FROM file WHERE file_format IS NOT NULL
    UNION

    SELECT f."RID", t."name" AS kw
    FROM file f
    JOIN file_format t ON (f.file_format = t.id)
    UNION

    -- data_type
    SELECT "RID", data_type AS kw FROM file WHERE data_type IS NOT NULL
    UNION

    SELECT f."RID", t."name" AS kw
    FROM file f
    JOIN data_type t ON (f.data_type = t.id)
    UNION

    -- assay_type
    SELECT "RID", assay_type AS kw FROM file WHERE assay_type IS NOT NULL
    UNION

    SELECT f."RID", t."name" AS kw
    FROM file f
    JOIN assay_type t ON (f.assay_type = t.id)
    UNION

    -- file ... anatomy facet
    SELECT f."RID", anatomy AS kw
    FROM file f
    JOIN file_anatomy fa ON (f.id_namespace = fa.file_id_namespace AND f.local_id = fa.file_local_id)
    UNION

    SELECT f."RID", t."name" AS kw
    FROM file f
    JOIN file_anatomy fa ON (f.id_namespace = fa.file_id_namespace AND f.local_id = fa.file_local_id)
    JOIN anatomy t ON (fa.anatomy = t.id)
    UNION

    -- file ... subject taxonomy facet
    SELECT f."RID", fsrt.subject_taxonomy_id AS kw
    FROM file f
    JOIN file_subject_role_taxonomy fsrt ON (f.id_namespace = fsrt.file_id_namespace AND f.local_id = fsrt.file_local_id)
    UNION

    SELECT f."RID", t."name" AS kw
    FROM file f
    JOIN file_subject_role_taxonomy fsrt ON (f.id_namespace = fsrt.file_id_namespace AND f.local_id = fsrt.file_local_id)
    JOIN ncbi_taxonomy t ON (fsrt.subject_taxonomy_id = t.id)
    UNION

    -- file ... subject role facet
    SELECT f."RID", fsrt.subject_role_id AS kw
    FROM file f
    JOIN file_subject_role_taxonomy fsrt ON (f.id_namespace = fsrt.file_id_namespace AND f.local_id = fsrt.file_local_id)
    UNION

    SELECT f."RID", t."name" AS kw
    FROM file f
    JOIN file_subject_role_taxonomy fsrt ON (f.id_namespace = fsrt.file_id_namespace AND f.local_id = fsrt.file_local_id)
    JOIN subject_role t ON (fsrt.subject_role_id = t.id)
    UNION

    -- file ... subject granularity facet
    SELECT f."RID", fsg.subject_granularity AS kw
    FROM file f
    JOIN file_subject_granularity fsg ON (f.id_namespace = fsg.file_id_namespace AND f.local_id = fsg.file_local_id)
    UNION

    SELECT f."RID", t."name" AS kw
    FROM file f
    JOIN file_subject_granularity fsg ON (f.id_namespace = fsg.file_id_namespace AND f.local_id = fsg.file_local_id)
    JOIN subject_granularity t ON (fsg.subject_granularity = t.id)
    UNION

    -- file ... part of collection facet
    SELECT DISTINCT f."RID", cic.leader_collection_id_namespace AS kw
    FROM file f
    JOIN file_in_collection fic ON (f.id_namespace = fic.file_id_namespace AND f.local_id = fic.file_local_id)
    JOIN collection_in_collection_transitive cic ON (fic.collection_id_namespace = cic.member_collection_id_namespace AND fic.collection_local_id = cic.member_collection_local_id)
    UNION

    SELECT DISTINCT f."RID", cic.leader_collection_local_id AS kw
    FROM file f
    JOIN file_in_collection fic ON (f.id_namespace = fic.file_id_namespace AND f.local_id = fic.file_local_id)
    JOIN collection_in_collection_transitive cic ON (fic.collection_id_namespace = cic.member_collection_id_namespace AND fic.collection_local_id = cic.member_collection_local_id)
    UNION

    SELECT DISTINCT f."RID", c."name" AS kw
    FROM file f
    JOIN file_in_collection fic ON (f.id_namespace = fic.file_id_namespace AND f.local_id = fic.file_local_id)
    JOIN collection_in_collection_transitive cic ON (fic.collection_id_namespace = cic.member_collection_id_namespace AND fic.collection_local_id = cic.member_collection_local_id)
    JOIN collection c ON (cic.leader_collection_id_namespace = c.id_namespace AND cic.leader_collection_local_id = c.local_id)

  ) s
  GROUP BY "RID"
)
UPDATE file AS v
SET kw = s.kw
FROM s
WHERE v."RID" = s."RID"
;
