WITH t1 (
  subject_id_namespace,
  subject_local_id,
  project_id_namespace,
  project_local_id
) AS (
SELECT DISTINCT
  e.id_namespace,
  e.local_id,
  pr.project_id_namespace,
  pr.project_local_id
FROM subject e
JOIN project_in_project_transitive pipt
  ON (e.project_id_namespace = pipt.member_project_id_namespace AND e.project_local_id = pipt.member_project_local_id)
JOIN project_root pr
  ON (pipt.leader_project_id_namespace = pr.project_id_namespace AND pipt.leader_project_local_id = pr.project_local_id)
)
UPDATE subject AS u
SET project_root_id_namespace = t1.project_id_namespace,
    project_root_local_id = t1.project_local_id
FROM t1
WHERE u.id_namespace = t1.subject_id_namespace
  AND u.local_id = t1.subject_local_id
;
