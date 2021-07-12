INSERT INTO file_describes_subject (
  file,
  subject
)
SELECT
  f.nid,
  s.nid
FROM submission.file_describes_subject fds
JOIN submission.file f ON (fds.file_id_namespace = f.id_namespace AND fds.file_local_id = f.local_id)
JOIN submission.subject s ON (fds.subject_id_namespace = s.id_namespace AND fds.subject_local_id = s.local_id)
;
