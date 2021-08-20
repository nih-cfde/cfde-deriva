WITH RECURSIVE t AS (

  SELECT
    nid AS leader_project,
    nid AS member_project
  FROM project
 
  UNION

  SELECT
    t.leader_project,
    a.child_project AS member_project
  FROM project_in_project a
  JOIN t ON (a.parent_project = t.member_project)

)
INSERT INTO project_in_project_transitive (
  leader_project,
  member_project
)
SELECT 
  leader_project,
  member_project
FROM t
;
