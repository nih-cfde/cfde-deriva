-- Assume a submission db is attached at as the schema "submission"
INSERT INTO dbgap_study_id (id, name, description)
SELECT DISTINCT
  f.dbgap_study_id,
  COALESCE(v.name, 'unknown'),
  v.description
FROM submission.file f
LEFT OUTER JOIN dbgap_study_id_canonical v ON (f.dbgap_study_id = v.id)
WHERE f.dbgap_study_id IS NOT NULL
-- add missing terms to set we already have built-in
ON CONFLICT DO NOTHING
;
