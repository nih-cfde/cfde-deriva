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
