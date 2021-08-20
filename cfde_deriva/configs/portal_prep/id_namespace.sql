INSERT INTO id_namespace (
  nid,
  id,
  abbreviation,
  name,
  description
)
SELECT
  nid,
  id,
  abbreviation,
  name,
  description
FROM submission.id_namespace
;
