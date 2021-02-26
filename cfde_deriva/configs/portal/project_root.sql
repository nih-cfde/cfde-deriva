INSERT INTO project_root (
  project_id_namespace,
  project_local_id
)
SELECT
  id_namespace AS project_id_namespace,
  local_id AS project_local_id
FROM project p

EXCEPT

SELECT
  child_project_id_namespace,
  child_project_local_id
FROM project_in_project
;
