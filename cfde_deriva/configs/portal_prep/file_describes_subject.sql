-- translate submitted links
INSERT INTO file_describes_subject (
  nid,
  file,
  subject
)
SELECT
  fds.nid,
  f.nid,
  s.nid
FROM submission.file_describes_subject fds
JOIN submission.file f ON (fds.file_id_namespace = f.id_namespace AND fds.file_local_id = f.local_id)
JOIN submission.subject s ON (fds.subject_id_namespace = s.id_namespace AND fds.subject_local_id = s.local_id)
;

-- add any gaps implied by file -- biosample -- subject
INSERT INTO file_describes_subject (
  file,
  subject
)
SELECT
  f.nid,
  s.nid
FROM submission.file_describes_biosample fdb
JOIN submission.file f ON (fdb.file_id_namespace = f.id_namespace AND fdb.file_local_id = f.local_id)
JOIN submission.biosample_from_subject bfs USING (biosample_id_namespace, biosample_local_id)
JOIN submission.subject s ON (bfs.subject_id_namespace = s.id_namespace AND bfs.subject_local_id = s.local_id)
WHERE True
ON CONFLICT DO NOTHING
;
