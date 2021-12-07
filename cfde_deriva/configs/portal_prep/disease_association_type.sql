INSERT INTO disease_association_type (
  nid,
  id,
  name,
  description
)
SELECT
  nid,
  id,
  name,
  description
FROM submission.disease_association_type
UNION
VALUES (
  '0',
  'cfde_disease_association_type:NS',
  'not specified',
  'association type not specified'
);
