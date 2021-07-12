INSERT INTO subject_granularity (
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
FROM submission.subject_granularity
;
