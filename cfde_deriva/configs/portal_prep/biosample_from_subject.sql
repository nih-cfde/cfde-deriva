INSERT INTO biosample_from_subject (
  biosample,
  subject
)
SELECT
  b.nid,
  s.nid
FROM submission.biosample_from_subject bfs
JOIN submission.biosample b ON (bfs.biosample_id_namespace = b.id_namespace AND bfs.biosample_local_id = b.local_id)
JOIN submission.subject s ON (bfs.subject_id_namespace = s.id_namespace AND bfs.subject_local_id = s.local_id)
;
