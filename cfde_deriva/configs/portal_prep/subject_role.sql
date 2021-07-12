INSERT INTO subject_role (
  nid,
  id,
  "name",
  desription
)
SELECT
  nid,
  id,
  "name",
  desription
FROM submission.subject_role
;
