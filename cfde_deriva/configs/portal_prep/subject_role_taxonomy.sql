INSERT INTO subject_role_taxonomy (
  subject,
  role,
  taxonomy
)
SELECT
  s.nid,
  r.nid,
  t.nid
FROM submission.subject_role_taxonomy srt
JOIN submission.subject_role r ON (srt.role_id = r.id)
JOIN submission.ncbi_taxonomy t ON (srt.taxonomy_id = t.id)
;
