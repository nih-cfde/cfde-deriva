INSERT INTO biosample_project_root (
  biosample_id_namespace,
  biosample_local_id,
  project_id_namespace,
  project_local_id
)
SELECT DISTINCT
  e.id_namespace,
  e.local_id,
  pr.project_id_namespace,
  pr.project_local_id
FROM biosample e
JOIN project_in_project_transitive pipt
  ON (e.project_id_namespace = pipt.member_project_id_namespace AND e.project_local_id = pipt.member_project_local_id)
JOIN project_root pr
  ON (pipt.leader_project_id_namespace = pr.project_id_namespace AND pipt.leader_project_local_id = pr.project_local_id)
;
