INSERT INTO disease (
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
FROM submission.disease
;
