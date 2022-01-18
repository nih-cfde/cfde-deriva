UPDATE pubchem_fact AS v
SET kw = s.kw
FROM (
  SELECT
    pcf.nid,
    cfde_keywords_merge(
      -- perform split/strip/merge of subst.synonyms too...
      (SELECT cfde_keywords_agg(subst.id, subst.name, subst.description, subst.synonyms)
       FROM json_each(pcf.substances) substj JOIN substance subst ON (substj.value = subst.nid)),

      -- perform split/strip/merge of cmpd.synonyms too...
      (SELECT cfde_keywords_agg(cmpd.id, cmpd.name, cmpd.description, cmpd.synonyms)
       FROM json_each(pcf.compounds) cmpdj JOIN compound cmpd ON (cmpdj.value = cmpd.nid))
    ) AS kw
  FROM pubchem_fact pcf
) s
WHERE v.nid = s.nid
;
