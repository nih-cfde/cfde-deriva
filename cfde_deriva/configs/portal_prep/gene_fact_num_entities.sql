UPDATE gene_fact AS u
SET
  num_files = (SELECT count(*) FROM file s WHERE s.gene_fact = u.nid),
  num_biosamples = (SELECT count(*) FROM biosample s WHERE s.gene_fact = u.nid),
  num_subjects = (SELECT count(*) FROM subject s WHERE s.gene_fact = u.nid),
  num_collections = (SELECT count(*) FROM collection s WHERE s.gene_fact = u.nid),
  total_size_in_bytes = (SELECT sum(size_in_bytes) FROM file s WHERE s.gene_fact = u.nid)
;
