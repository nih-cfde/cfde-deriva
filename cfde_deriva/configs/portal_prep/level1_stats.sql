INSERT INTO level1_stats (
  root_project_nid,
  project_nid,
  assay_type_nid,
  data_type_nid,
  file_format_nid,
  anatomy_nid,
  disease_nid,
  granularity_nid,
  species_nid,
  root_project_id_namespace,
  root_project_local_id,
  project_id_namespace,
  project_local_id,
  assay_type_id,
  data_type_id,
  file_format_id,
  anatomy_id,
  disease_id,
  granularity_id,
  species_id,
  num_files,
  num_bytes,
  num_biosamples,
  num_subjects
)
SELECT
  dcc_proj.nid AS root_project_id,
  proj.nid AS project_nid,
  "at".nid,
  dt.nid,
  ff.nid,
  anat.nid,
  dis.nid,
  sg.nid,
  ss.nid,
  dcc_proj.id_namespace,
  dcc_proj.local_id,
  proj.id_namespace,
  proj.local_id,
  "at".id,
  dt.id,
  ff.id,
  anat.id,
  dis.id,
  sg.id,
  ss.id,
  sum(cff.num_files),
  sum(cff.size_in_bytes) AS size_in_bytes,
  sum(cfb.num_biosamples),
  sum(cfs.num_subjects)
FROM core_fact cf
LEFT JOIN (
  SELECT
    cf.nid,
    count(*) AS num_files,
    sum(f.size_in_bytes) AS size_in_bytes
  FROM core_fact cf
  JOIN file f ON (f.core_fact = cf.nid)
  GROUP BY cf.nid
) cff ON (cf.nid = cff.nid)
LEFT JOIN (
  SELECT
    cf.nid,
    count(*) AS num_biosamples
  FROM core_fact cf
  JOIN biosample b ON (b.core_fact = cf.nid)
  GROUP BY cf.nid
) cfb ON (cf.nid = cfb.nid)
LEFT JOIN (
  SELECT
    cf.nid,
    count(*) AS num_subjects
  FROM core_fact cf
  JOIN subject s ON (s.core_fact = cf.nid)
  GROUP BY cf.nid
) cfs ON (cf.nid = cfs.nid)
JOIN core_fact_project cf_p ON (cf.nid = cf_p.core_fact)
JOIN project proj ON (cf_p.project = proj.nid)
JOIN core_fact_dcc cf_d ON (cf.nid = cf_d.core_fact)
JOIN dcc ON (cf_d.dcc = dcc.nid)
JOIN project dcc_proj ON (dcc.project = dcc_proj.nid)
LEFT JOIN (
  core_fact_assay_type cf_at
  JOIN assay_type "at" ON (cf_at.assay_type = "at".nid)
) ON (cf.nid = cf_at.core_fact)
LEFT JOIN (
  core_fact_data_type cf_dt
  JOIN data_type dt ON (cf_dt.data_type = dt.nid)
) ON (cf.nid = cf_dt.core_fact)
LEFT JOIN (
  core_fact_file_format cf_ff
  JOIN file_format ff ON (cf_ff.file_format = ff.nid)
) ON (cf.nid = cf_ff.core_fact)
LEFT JOIN (
  core_fact_anatomy cf_a
  JOIN anatomy anat ON (cf_a.anatomy = anat.nid)
) ON (cf.nid = cf_a.core_fact)
LEFT JOIN (
  core_fact_disease cf_dis
  JOIN disease dis ON (cf_dis.disease = dis.nid)
) ON (cf.nid = cf_dis.core_fact)
LEFT JOIN (
  core_fact_subject_granularity cf_sg
  JOIN subject_granularity sg ON (cf_sg.subject_granularity = sg.nid)
) ON (cf.nid = cf_sg.core_fact)
LEFT JOIN (
  core_fact_subject_species cf_ss
  JOIN ncbi_taxonomy ss ON (cf_ss.subject_species = ss.nid)
) ON (cf.nid = cf_ss.core_fact)
GROUP BY
  -- group by all extra cols so we can test/run this on postgres too
  proj.nid, proj.id_namespace, proj.local_id,
  dcc_proj.nid, dcc_proj.id_namespace, dcc_proj.local_id,
  "at".nid, "at".id,
  dt.nid, dt.id,
  ff.nid, ff.id,
  anat.nid, anat.id,
  dis.nid, dis.id,
  sg.nid, sg.id,
  ss.nid, ss.id
;
