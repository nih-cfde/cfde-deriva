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

UPDATE collection AS v
SET combined_fact = s.nid
FROM combined_fact s
WHERE v.core_fact = s.core_fact
  AND v.gene_fact = s.gene_fact
  AND v.pubchem_fact = s.pubchem_fact
  AND v.protein_fact = s.protein_fact
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
WHERE true
GROUP BY core_fact, gene_fact, protein_fact, pubchem_fact
ON CONFLICT (core_fact, gene_fact, protein_fact, pubchem_fact)
DO UPDATE
SET num_files = EXCLUDED.num_files,
    total_size_in_bytes = EXCLUDED.total_size_in_bytes
;

UPDATE file AS v
SET combined_fact = s.nid
FROM combined_fact s
WHERE v.core_fact = s.core_fact
  AND v.gene_fact = s.gene_fact
  AND v.pubchem_fact = s.pubchem_fact
  AND v.protein_fact = s.protein_fact
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
WHERE true
GROUP BY core_fact, gene_fact, protein_fact, pubchem_fact
ON CONFLICT (core_fact, gene_fact, protein_fact, pubchem_fact)
DO UPDATE
SET num_biosamples = EXCLUDED.num_biosamples
;

UPDATE biosample AS v
SET combined_fact = s.nid
FROM combined_fact s
WHERE v.core_fact = s.core_fact
  AND v.gene_fact = s.gene_fact
  AND v.pubchem_fact = s.pubchem_fact
  AND v.protein_fact = s.protein_fact
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
WHERE true
GROUP BY core_fact, gene_fact, protein_fact, pubchem_fact
ON CONFLICT (core_fact, gene_fact, protein_fact, pubchem_fact)
DO UPDATE
SET num_subjects = EXCLUDED.num_subjects
;

UPDATE subject AS v
SET combined_fact = s.nid
FROM combined_fact s
WHERE v.core_fact = s.core_fact
  AND v.gene_fact = s.gene_fact
  AND v.pubchem_fact = s.pubchem_fact
  AND v.protein_fact = s.protein_fact
;
