INSERT INTO collection_project_root (
  collection_id_namespace,
  collection_local_id,
  project_id_namespace,
  project_local_id
)
SELECT DISTINCT
  e.id_namespace,
  e.local_id,
  pr.project_id_namespace,
  pr.project_local_id
FROM collection e
JOIN collection_defined_by_project cdp
  ON (e.id_namespace = cdp.collection_id_namespace AND e.local_id = cdp.collection_local_id)
JOIN project_in_project_transitive pipt
  ON (cdp.project_id_namespace = pipt.member_project_id_namespace AND cdp.project_local_id = pipt.member_project_local_id)
JOIN project_root pr
  ON (pipt.leader_project_id_namespace = pr.project_id_namespace AND pipt.leader_project_local_id = pr.project_local_id)
;
