INSERT INTO core_fact_phenotype (
  core_fact,
  association_type,
  phenotype
)
SELECT
  cf.nid,
  json_extract(j.value, '$[1]'),
  json_extract(j.value, '$[0]')
FROM core_fact cf, json_each(cf.phenotypes) j
WHERE true
;
