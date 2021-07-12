-- Assume a submission db is attached at as the schema "submission"
INSERT INTO mime_type (id)
SELECT DISTINCT mime_type
FROM submission.file f
-- use upsert so we can optionally pre-load canonical MIME types and patch 
ON CONFLICT DO NOTHING
;
