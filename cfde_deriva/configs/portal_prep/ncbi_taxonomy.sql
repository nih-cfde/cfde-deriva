INSERT INTO ncbi_taxonomy (
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
FROM submission.ncbi_taxonomy
;
