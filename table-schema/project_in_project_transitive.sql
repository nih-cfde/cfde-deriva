WITH RECURSIVE t(
  leader_project_id_namespace,
  leader_project_id,
  member_project_id_namespace,
  member_project_id
) AS (

  SELECT
    id_namespace AS leader_project_id_namespace,
    id AS leader_project_id,
    id_namespace AS member_project_id_namespace,
    id AS member_project_id
  FROM project
 
  UNION

  SELECT
    t.leader_project_id_namespace,
    t.leader_project_id,
    a.child_project_id_namespace AS member_project_id_namespace,
    a.child_project_id AS member_project_id
  FROM project_in_project a
  JOIN t ON (a.parent_project_id_namespace = t.member_project_id_namespace AND a.parent_project_id = t.member_project_id)

)
SELECT 
  leader_project_id_namespace,
  leader_project_id,
  member_project_id_namespace,
  member_project_id
FROM t;
