INSERT INTO core_fact_disease (
  core_fact,
  association_type,
  disease
)
SELECT
  cf.nid,
  json_extract(j.value, '$[1]'),
  json_extract(j.value, '$[0]')
FROM core_fact cf, json_each(cf.diseases) j
WHERE true
;
