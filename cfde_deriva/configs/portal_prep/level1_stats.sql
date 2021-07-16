INSERT INTO level1_stats (
  root_project_nid,
  project_nid,
  assay_type_nid,
  data_type_nid,
  file_format_nid,
  anatomy_nid,
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
  sg.id,
  ss.id,
  sum(num_files),
  sum(total_size_in_bytes),
  sum(num_biosamples),
  sum(num_subjects)
FROM core_fact cf
JOIN json_each(cf.projects) proj_j
JOIN project proj ON (proj_j.value = proj.nid)
JOIN json_each(cf.dccs) dcc_j
JOIN dcc ON (dcc_j.value = dcc.nid)
JOIN project dcc_proj ON (dcc.project = dcc_proj.nid)
LEFT JOIN json_each(cf.assay_types) at_j
LEFT JOIN assay_type "at" ON (at_j.value = "at".nid)
LEFT JOIN json_each(cf.data_types) dt_j
LEFT JOIN data_type dt ON (dt_j.value = dt.nid)
LEFT JOIN json_each(cf.file_formats) ff_j
LEFT JOIN file_format ff ON (ff_j.value = ff.nid)
LEFT JOIN json_each(cf.anatomies) anat_j
LEFT JOIN anatomy anat ON (anat_j.value = anat.nid)
LEFT JOIN json_each(cf.subject_granularities) sg_j
LEFT JOIN subject_granularity sg ON (sg_j.value = sg.nid)
LEFT JOIN json_each(cf.subject_species) ss_j
LEFT JOIN ncbi_taxonomy ss ON (ss_j.value = ss.nid)
GROUP BY proj.nid, "at".nid, dt.nid, ff.nid, anat.nid, sg.nid, ss.nid
