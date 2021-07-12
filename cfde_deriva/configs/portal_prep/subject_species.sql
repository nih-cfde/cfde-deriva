INSERT INTO subject_species (
  subject,
  species
)
SELECT DISTINCT
  s.nid,
  t.nid
FROM (
  -- find subjects w/ unambiguous single-organism species info
  SELECT
    s.nid
  FROM subject_role_taxonomy srt
  JOIN subject s ON (srt.subject = s.nid)
  JOIN subject_granularity sg ON (s.granularity = sg.nid)
  JOIN subject_role sr ON (srt."role" = sr.nid)
  JOIN ncbi_taxonomy t ON (srt.taxonomy = t.nid)
  WHERE sg."name" = 'single organism'
    AND sr."name" = 'single organism'
    AND t.clade = 'species'
  GROUP BY s.nid
  HAVING count(*) = 1
) s
-- now find species info
JOIN subject_role_taxonomy srt ON (srt.subject = s.nid)
JOIN subject_role sr ON (srt."role" = sr.nid)
JOIN ncbi_taxonomy t ON (srt.taxonomy = t.nid)
WHERE sr."name" = 'single organism'
  AND t.clade = 'species'
;
