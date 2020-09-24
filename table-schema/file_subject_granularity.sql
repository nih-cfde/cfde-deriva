INSERT INTO file_subject_granularity (
  file_id_namespace,
  file_local_id,
  subject_granularity
)
SELECT
  fds.file_id_namespace,
  fds.file_local_id,
  s.granularity AS subject_granularity
FROM file_describes_subject fds
JOIN subject s
  ON (fds.subject_id_namespace = s.id_namespace AND fds.subject_local_id = s.local_id)

UNION

SELECT
  fdb.file_id_namespace,
  fdb.file_local_id,
  s.granularity AS subject_granularity
FROM file_describes_biosample fdb
JOIN biosample_from_subject bfs
  ON (fdb.biosample_id_namespace = bfs.biosample_id_namespace AND fdb.biosample_local_id = bfs.biosample_local_id)
JOIN subject s
  ON (bfs.subject_id_namespace = s.id_namespace AND bfs.subject_local_id = s.local_id)
;
