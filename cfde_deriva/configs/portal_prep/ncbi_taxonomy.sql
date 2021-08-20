INSERT INTO ncbi_taxonomy (
  nid,
  id,
  clade,
  "name",
  description,
  synonyms
)
SELECT
  nid,
  id,
  clade,
  "name",
  description,
  synonyms
FROM submission.ncbi_taxonomy
;
