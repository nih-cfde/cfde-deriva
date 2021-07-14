INSERT INTO subject_role (
  nid,
  id,
  "name",
  description
)
SELECT
  nid,
  id,
  "name",
  description
FROM submission.subject_role
;
