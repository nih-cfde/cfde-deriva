CREATE TEMPORARY TABLE file_gfacts AS
SELECT
  f.nid,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.gene)) FROM file_describes_biosample fdb JOIN biosample_gene a ON (fdb.biosample = a.biosample) WHERE fdb.file = f.nid
    ),
    '[]'
  ) AS genes
FROM file f;
CREATE INDEX IF NOT EXISTS file_gfacts_combo_idx ON file_gfacts(
    genes
);
INSERT INTO gene_fact (
    genes
)
SELECT DISTINCT
  ff.genes
FROM file_gfacts ff
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE file AS u
SET gene_fact = gf.nid
FROM file_gfacts ff, gene_fact gf
WHERE u.nid = ff.nid
  AND ff.genes = gf.genes
;

CREATE TEMPORARY TABLE biosample_gfacts AS
SELECT
  b.nid,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.gene)) FROM biosample_gene a WHERE a.biosample = b.nid
    ),
    '[]'
  ) AS genes
FROM biosample b;
CREATE INDEX IF NOT EXISTS biosample_gfacts_combo_idx ON biosample_gfacts(
    genes
);
INSERT INTO gene_fact (
    genes
)
SELECT DISTINCT
  bf.genes
FROM biosample_gfacts bf
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE biosample AS u
SET gene_fact = gf.nid
FROM biosample_gfacts bf, gene_fact gf
WHERE u.nid = bf.nid
  AND bf.genes = gf.genes
;

CREATE TEMPORARY TABLE subject_gfacts AS
SELECT
  s.nid,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.gene)) FROM biosample_from_subject bfs JOIN biosample_gene a ON (bfs.biosample = a.biosample) WHERE bfs.subject = s.nid
    ),
    '[]'
  ) AS genes
FROM subject s;
CREATE INDEX IF NOT EXISTS subject_gfacts_combo_idx ON subject_gfacts(
    genes
);
INSERT INTO gene_fact (
    genes
)
SELECT DISTINCT
  sf.genes
FROM subject_gfacts sf
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE subject AS u
SET gene_fact = gf.nid
FROM subject_gfacts sf, gene_fact gf
WHERE u.nid = sf.nid
  AND sf.genes = gf.genes
;

CREATE TEMPORARY TABLE collection_gfacts AS
SELECT
  col.nid,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT a.gene AS value FROM collection_gene a WHERE a.collection = col.nid
        UNION
        SELECT j.value FROM file_in_collection fic, file f, gene_fact gf, json_each(gf.genes) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.gene_fact = gf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, gene_fact gf, json_each(gf.genes) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.gene_fact = gf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, gene_fact gf, json_each(gf.genes) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.gene_fact = gf.nid
      ) s
    ),
    '[]'
  ) AS genes
FROM collection col;
CREATE INDEX IF NOT EXISTS collection_gfacts_combo_idx ON collection_gfacts(
    genes
);
INSERT INTO gene_fact (
    genes
)
SELECT DISTINCT
  colf.genes
FROM collection_gfacts colf
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE collection AS u
SET gene_fact = gf.nid
FROM collection_gfacts colf, gene_fact gf
WHERE u.nid = colf.nid
  AND colf.genes = gf.genes
;

UPDATE gene_fact AS v
SET kw = s.kw
FROM (
  SELECT
    gf.nid,
    cfde_keywords_merge(
      (SELECT cfde_keywords_merge(
         cfde_keywords_agg(gn.id, gn.name, gn.description),
         cfde_keywords_merge_agg(gn.synonyms)
       )
       FROM json_each(gf.genes) gnj JOIN gene gn ON (gnj.value = gn.nid))
    ) AS kw
  FROM gene_fact gf
) s
WHERE v.nid = s.nid
;
