INSERT INTO biosample_from_subject (
  biosample,
  subject,
  age_at_sampling
)
SELECT
  b.nid,
  s.nid,
  bfs.age_at_sampling
FROM submission.biosample_from_subject bfs
JOIN submission.biosample b ON (bfs.biosample_id_namespace = b.id_namespace AND bfs.biosample_local_id = b.local_id)
JOIN submission.subject s ON (bfs.subject_id_namespace = s.id_namespace AND bfs.subject_local_id = s.local_id)
;
