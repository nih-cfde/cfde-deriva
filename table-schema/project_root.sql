INSERT INTO project_root (
  project_id_namespace,
  project_id
)
SELECT
  id_namespace AS project_id_namespace,
  id AS project_id
FROM project p

EXCEPT

SELECT
  child_project_id_namespace,
  child_project_id
FROM project_in_project
;
