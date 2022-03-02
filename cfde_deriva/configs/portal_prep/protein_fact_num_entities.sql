UPDATE protein_fact AS u
SET
  num_collections = (SELECT count(*) FROM collection s WHERE s.protein_fact = u.nid)
;
