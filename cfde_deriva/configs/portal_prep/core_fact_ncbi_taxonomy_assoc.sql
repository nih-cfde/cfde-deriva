INSERT INTO core_fact_ncbi_taxonomy (core_fact, ncbi_taxon)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.ncbi_taxons) j
WHERE j.value IS NOT NULL
;
