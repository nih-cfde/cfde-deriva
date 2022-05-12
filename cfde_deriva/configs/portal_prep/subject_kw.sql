UPDATE subject AS v
SET kw = array_join(s.kw, ' ')
FROM (
  SELECT
    s.nid,
    cfde_keywords_merge(
      cfde_keywords(
        s.local_id,
        s.persistent_id
      ),
      cf.kw,
      gf.kw,
      pcf.kw,
      prf.kw
    ) AS kw
  FROM subject s
  JOIN core_fact cf ON (s.core_fact = cf.nid)
  JOIN gene_fact gf ON (s.gene_fact = gf.nid)
  JOIN pubchem_fact pcf ON (s.pubchem_fact = pcf.nid)
  JOIN protein_fact prf ON (s.protein_fact = prf.nid)
) s
WHERE v.nid = s.nid
;
