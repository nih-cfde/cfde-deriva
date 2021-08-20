INSERT INTO project_root (
  project
)
SELECT leader_project
FROM project_in_project_transitive

EXCEPT

SELECT member_project
FROM project_in_project_transitive
WHERE member_project != leader_project
;
