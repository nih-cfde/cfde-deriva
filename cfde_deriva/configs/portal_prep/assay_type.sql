INSERT INTO assay_type (
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
FROM submission.assay_type
;
