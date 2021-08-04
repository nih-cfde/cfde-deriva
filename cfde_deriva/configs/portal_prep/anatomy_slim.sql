-- get slim mappings for every term used
INSERT INTO anatomy_slim (
  original_term,
  slim_term
)
SELECT
  o.nid,
  s.nid
FROM anatomy_slim_raw a
JOIN anatomy o ON (a.original_term_id = o.id)
JOIN anatomy s ON (a.slim_term_id = s.id)
;

-- add identity mapping for any unmapped terms
INSERT INTO anatomy_slim (
  original_term,
  slim_term
)
SELECT
  o.nid,
  o.nid
FROM anatomy o
LEFT JOIN anatomy_slim a ON (o.nid = a.original_term)
WHERE a.original_term IS NULL
;
