-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO project_in_project (
  parent_project,
  child_project
)
SELECT
  p.nid,
  c.nid,
FROM submission.project_in_project pip
JOIN submission.project p ON (pip.parent_project_id_namespace = p.id_namespace AND pip.parent_project_local_id = p.local_id)
JOIN submission.project c ON (pip.child_project_id_namespace = c.id_namespace AND pip.child_project_local_id = c.local_id)
;
