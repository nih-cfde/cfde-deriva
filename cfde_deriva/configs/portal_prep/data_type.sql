INSERT INTO data_type (
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
FROM submission.data_type
;
