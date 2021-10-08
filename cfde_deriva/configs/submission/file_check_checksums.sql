WITH bad AS (
  SELECT *
  FROM file f
  WHERE f.persistent_id IS NOT NULL
    AND f.md5 IS NULL
    AND f.sha256 IS NULL
)
SELECT
  'required checksum not present when persistent_id is present' AS description,
  (SELECT count(*) FROM bad) AS num_rows,
  nid,
  json_object(
    'persistent_id', persistent_id,
    'md5', md5,
    'sha256', sha256
  ) AS example_data
FROM bad
ORDER BY nid
LIMIT 1;

