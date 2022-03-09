UPDATE combined_fact AS v
SET kw = s.kw
FROM (
  SELECT
    s.nid,
    cfde_keywords_merge(
      ckw.kw,
      gf.kw,
      pcf.kw,
      prf.kw
    ) AS kw
  FROM combined_fact s
  JOIN core_keyword ckw ON (s.core_fact = ckw.nid)
  JOIN gene_fact gf ON (s.gene_fact = gf.nid)
  JOIN pubchem_fact pcf ON (s.pubchem_fact = pcf.nid)
  JOIN protein_fact prf ON (s.protein_fact = prf.nid)
) s
WHERE v.nid = s.nid
;
