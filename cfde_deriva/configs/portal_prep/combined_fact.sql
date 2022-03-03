INSERT INTO combined_fact (
  core_fact,
  gene_fact,
  protein_fact,
  pubchem_fact,
  num_collections
)
SELECT
  core_fact,
  gene_fact,
  protein_fact,
  pubchem_fact,
  count(*)
FROM collection
GROUP BY core_fact, gene_fact, protein_fact, pubchem_fact
;

INSERT INTO combined_fact (
  core_fact,
  gene_fact,
  protein_fact,
  pubchem_fact,
  num_files,
  total_size_in_bytes
)
SELECT
  core_fact,
  gene_fact,
  protein_fact,
  pubchem_fact,
  count(*),
  sum(size_in_bytes)
FROM file
GROUP BY core_fact, gene_fact, protein_fact, pubchem_fact
ON CONFLICT DO UPDATE
SET num_files = EXCLUDED.num_files,
    total_size_in_bytes = EXCLUDED.total_size_in_bytes
;

INSERT INTO combined_fact (
  core_fact,
  gene_fact,
  protein_fact,
  pubchem_fact,
  num_biosamples
)
SELECT
  core_fact,
  gene_fact,
  protein_fact,
  pubchem_fact,
  count(*)
FROM biosample
GROUP BY core_fact, gene_fact, protein_fact, pubchem_fact
ON CONFLICT DO UPDATE
SET num_biosamples = EXCLUDED.num_biosamples
;

INSERT INTO combined_fact (
  core_fact,
  gene_fact,
  protein_fact,
  pubchem_fact,
  num_subjects
)
SELECT
  core_fact,
  gene_fact,
  protein_fact,
  pubchem_fact,
  count(*)
FROM subject
GROUP BY core_fact, gene_fact, protein_fact, pubchem_fact
ON CONFLICT DO UPDATE
SET num_subjects = EXCLUDED.num_subjects
;
