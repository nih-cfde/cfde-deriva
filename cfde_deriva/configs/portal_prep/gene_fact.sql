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
SET gene_fact = gf.nid,
    genes = gf.genes
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
SET gene_fact = gf.nid,
    genes = gf.genes
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
SET gene_fact = gf.nid,
    genes = gf.genes
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
SET gene_fact = gf.nid,
    genes = gf.genes
FROM collection_gfacts colf, gene_fact gf
WHERE u.nid = colf.nid
  AND colf.genes = gf.genes
;

CREATE TEMPORARY TABLE genefact_kw AS
  SELECT
    gf.nid,
    cfde_keywords_merge(
      -- perform split/strip/merge of gene.synonyms too...
      (SELECT cfde_keywords_agg(gn.id, gn.name, gn.description, gn.synonyms)
       FROM json_each(gf.genes) gnj JOIN gene gn ON (gnj.value = gn.nid))
    ) AS kw
  FROM gene_fact gf
;

INSERT INTO keywords (kw)
SELECT kw FROM (
SELECT DISTINCT array_join(kw, ' ') AS kw FROM genefact_kw
EXCEPT
SELECT kw FROM keywords
) AS s
WHERE kw IS NOT NULL
  AND kw != ''
;

CREATE TEMPORARY TABLE genefact_kw_map AS
SELECT
  c.nid AS gene_fact,
  k.nid AS kw
FROM genefact_kw c
JOIN keywords k ON (c.kw = k.kw)
;

INSERT INTO file_keywords (file, kw)
SELECT s.nid, k.kw
FROM file s JOIN genefact_kw_map k ON (s.gene_fact = k.gene_fact)
EXCEPT
SELECT file, kw FROM file_keywords
;
INSERT INTO biosample_keywords (biosample, kw)
SELECT s.nid, k.kw
FROM biosample s JOIN genefact_kw_map k ON (s.gene_fact = k.gene_fact)
EXCEPT
SELECT biosample, kw FROM biosample_keywords
;
INSERT INTO subject_keywords (subject, kw)
SELECT s.nid, k.kw
FROM subject s JOIN genefact_kw_map k ON (s.gene_fact = k.gene_fact)
EXCEPT
SELECT subject, kw FROM subject_keywords
;
INSERT INTO collection_keywords (collection, kw)
SELECT s.nid, k.kw
FROM collection s JOIN genefact_kw_map k ON (s.gene_fact = k.gene_fact)
EXCEPT
SELECT collection, kw FROM collection_keywords
;

