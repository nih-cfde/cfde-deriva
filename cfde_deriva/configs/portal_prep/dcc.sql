-- Assume a submission db is attached at as the schema "submission"
-- which was provisioned with nid integer auto-increment serial columns
INSERT INTO dcc (
  project,
  id,
  dcc_name,
  dcc_abbreviation,
  dcc_description,
  contact_email,
  contact_name,
  dcc_url
)
SELECT
  p.nid,
  d.id,
  d.dcc_name,
  d.dcc_abbreviation,
  d.dcc_description,
  d.contact_email,
  d.contact_name,
  d.dcc_url
FROM submission.dcc d
JOIN submission.project p ON (d.project_id_namespace = p.id_namespace AND d.project_local_id = p.local_id)
;
