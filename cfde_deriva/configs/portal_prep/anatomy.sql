-- get submissions term nid + id but canonical labels
INSERT INTO anatomy (
  nid,
  id,
  "name",
  description,
  synonyms
)
SELECT
  s.nid,
  s.id,
  c."name",
  c.description,
  c.synonyms
FROM submission.anatomy s
JOIN anatomy_canonical c ON (s.id = c.id)
;

-- get submissions definitions for non-canonical terms
INSERT INTO anatomy (
  nid,
  id,
  "name",
  description,
  synonyms
)
SELECT
  s.nid,
  s.id,
  s."name",
  s.description,
  s.synonyms
FROM submission.anatomy s
LEFT JOIN anatomy_canonical c ON (s.id = c.id)
WHERE c.id IS NULL
;

-- get extra canonical terms imputed by slim map and not already present
INSERT INTO anatomy (
  id,
  "name",
  description,
  synonyms
)
SELECT DISTINCT
  c.id,
  c."name",
  c.description,
  c.synonyms
FROM anatomy s
JOIN anatomy_slim_raw sm ON (s.id = sm.original_term_id)
JOIN anatomy_canonical c ON (sm.slim_term_id = c.id)
LEFT JOIN anatomy e ON (c.id = e.id)
WHERE e.id IS NULL
;

-- TEMPORARY: mock up imputed terms missing from canonical data
INSERT INTO anatomy (
  id,
  "name",
  description
)
SELECT DISTINCT
  sm.slim_term_id,
  sm.slim_term_id,
  'slim term missing from canonical anatomy.tsv'
FROM anatomy s
JOIN anatomy_slim_raw sm ON (s.id = sm.original_term_id)
LEFT JOIN anatomy e ON (sm.slim_term_id = e.id)
WHERE e.id IS NULL
;
