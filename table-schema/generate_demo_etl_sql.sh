#!/bin/bash

cat <<EOF
BEGIN;

SET search_path = "CFDE";

-- delete in case we have previous ETL results?
DELETE FROM collection_anatomy;
DELETE FROM collection_assay_type;
DELETE FROM collection_biosample_creation_time;
DELETE FROM collection_in_collection_transitive;
DELETE FROM collection_subject_granularity;
DELETE FROM collection_subject_role_taxonomy;
DELETE FROM biosample_assay_type;
DELETE FROM file_anatomy;
DELETE FROM file_biosample_creation_time;
DELETE FROM file_subject_granularity;
DELETE FROM file_subject_role_taxonomy;
DELETE FROM project_in_project_transitive;
DELETE FROM project_root;

EOF

grep derivation c2m2-level1-portal-model.json \
    | sed -e 's/.*: "\([^"]*\)"/\1/' \
    | while read sql_fname
do
    cat <<EOF

-- begin ${sql_fname}
EOF
    cat ${sql_fname}
    cat <<EOF
-- end ${sql_fname}

EOF
   
done

cat <<EOF
COMMIT;
EOF
