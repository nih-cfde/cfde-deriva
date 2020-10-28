INSERT INTO subject_species (
  subject_id_namespace,
  subject_local_id,
  species
)
SELECT DISTINCT
  srt.subject_id_namespace,
  srt.subject_local_id,
  t.id AS species
FROM subject_role_taxonomy srt
JOIN subject_role sr ON (srt.role_id = sr.id)
JOIN ncbi_taxonomy t ON (srt.taxonomy_id = t.id)
WHERE sr.name = 'single organism'
  AND t.clade = 'species'
;
