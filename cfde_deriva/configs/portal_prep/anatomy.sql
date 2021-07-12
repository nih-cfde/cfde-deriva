INSERT INTO anatomy (
  nid,
  id,
  "name",
  description,
  synonyms
)
SELECT
  nid,
  id,
  "name",
  description,
  synonyms
FROM submission.anatomy
;
