INSERT INTO file_subject_role_taxonomy (
  file_id_namespace,
  file_id,
  subject_role_id,
  subject_taxonomy_id
)
SELECT DISTINCT
  fds.file_id_namespace,
  fds.file_id,
  srt.role_id AS subject_role_id,
  srt.taxonomy_id AS subject_taxonomy_id
FROM file_describes_subject fds
JOIN subject_role_taxonomy srt
  ON (fds.subject_id_namespace = srt.subject_id_namespace AND fds.subject_id = srt.subject_id)

UNION

SELECT DISTINCT
  fdb.file_id_namespace,
  fdb.file_id,
  srt.role_id AS subject_role_id,
  srt.taxonomy_id AS subject_taxonomy_id
FROM file_describes_biosample fdb
JOIN biosample_from_subject bfs
  ON (fdb.biosample_id_namespace = bfs.biosample_id_namespace AND fdb.biosample_id = bfs.biosample_id)
JOIN subject_role_taxonomy srt
  ON (bfs.subject_id_namespace = srt.subject_id_namespace AND bfs.subject_id = srt.subject_id)
