INSERT INTO subject_species (
  subject_id_namespace,
  subject_local_id,
  species
)
SELECT DISTINCT
  s.id_namespace AS subject_id_namespace,
  s.local_id AS subject_local_id,
  srt.taxonomy_id AS species
FROM (
  -- find subjects w/ unambiguous single-organism species info
  SELECT
    s.id_namespace,
    s.local_id
  FROM subject_role_taxonomy srt
  JOIN subject s
    ON (    srt.subject_id_namespace = s.id_namespace
        AND srt.subject_local_id = s.local_id)
  JOIN subject_granularity sg ON (s.granularity = sg.id)
  JOIN subject_role sr ON (srt.role_id = sr.id)
  JOIN ncbi_taxonomy t ON (srt.taxonomy_id = t.id)
  WHERE sg.name = 'single organism'
    AND sr.name = 'single organism'
    AND t.clade = 'species'
  GROUP BY
    s.id_namespace,
    s.local_id
  HAVING count(*) = 1
) s
-- now find species info
JOIN subject_role_taxonomy srt
  ON (    srt.subject_id_namespace = s.id_namespace
      AND srt.subject_local_id = s.local_id)
JOIN subject_role sr ON (srt.role_id = sr.id)
JOIN ncbi_taxonomy t ON (srt.taxonomy_id = t.id)
WHERE sr.name = 'single organism'
  AND t.clade = 'species'
;
