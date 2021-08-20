-- Assume a submission db is attached at as the schema "submission"
INSERT INTO mime_type (id)
SELECT DISTINCT f.mime_type
FROM submission.file f
WHERE mime_type IS NOT NULL
-- allow us to pre-load some mime types in future?
ON CONFLICT DO NOTHING
;
