
INSERT INTO protein_fact (proteins)
VALUES ('[]')
ON CONFLICT DO NOTHING
;

UPDATE file AS u
SET protein_fact = (SELECT nid FROM protein_fact WHERE proteins = '[]')
;

UPDATE biosample AS u
SET protein_fact = (SELECT nid FROM protein_fact WHERE proteins = '[]')
;

UPDATE subject AS u
SET protein_fact = (SELECT nid FROM protein_fact WHERE proteins = '[]')
;

CREATE TEMPORARY TABLE collection_prfacts AS
SELECT
  col.nid,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.protein))
      FROM (
        SELECT cpr.protein FROM collection_protein cpr WHERE cpr.collection = col.nid
      ) s
    ),
    '[]'
  ) AS proteins
FROM collection col;
CREATE INDEX IF NOT EXISTS collection_prfacts_combo_idx ON collection_prfacts(
    proteins
);
INSERT INTO protein_fact (
    proteins
)
SELECT DISTINCT
  colf.proteins
FROM collection_prfacts colf
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE collection AS u
SET protein_fact = prf.nid
FROM collection_prfacts colf, protein_fact prf
WHERE u.nid = colf.nid
  AND colf.proteins = prf.proteins
;

CREATE TEMPORARY TABLE proteinfact_kw AS
  SELECT
    prf.nid,
    cfde_keywords_merge(
      -- perform split/strip/merge of protein.synonyms too...
      (SELECT cfde_keywords_agg(pr.id, pr.name, pr.description, pr.synonyms)
       FROM json_each(prf.proteins) prj JOIN protein pr ON (prj.value = pr.nid))
    ) AS kw
  FROM protein_fact prf
;

INSERT INTO keywords (kw)
SELECT kw FROM (
SELECT DISTINCT array_join(kw, ' ') AS kw FROM proteinfact_kw
EXCEPT
SELECT kw FROM keywords
) AS s
WHERE kw IS NOT NULL
  AND kw != ''
;

CREATE TEMPORARY TABLE proteinfact_kw_map AS
SELECT
  c.nid AS protein_fact,
  k.nid AS kw
FROM proteinfact_kw c
JOIN keywords k ON (array_join(c.kw, ' ') = k.kw)
;

INSERT INTO file_keywords (file, kw)
SELECT s.nid, k.kw
FROM file s JOIN proteinfact_kw_map k ON (s.protein_fact = k.protein_fact)
EXCEPT
SELECT file, kw FROM file_keywords
;
INSERT INTO biosample_keywords (biosample, kw)
SELECT s.nid, k.kw
FROM biosample s JOIN proteinfact_kw_map k ON (s.protein_fact = k.protein_fact)
EXCEPT
SELECT biosample, kw FROM biosample_keywords
;
INSERT INTO subject_keywords (subject, kw)
SELECT s.nid, k.kw
FROM subject s JOIN proteinfact_kw_map k ON (s.protein_fact = k.protein_fact)
EXCEPT
SELECT subject, kw FROM subject_keywords
;
INSERT INTO collection_keywords (collection, kw)
SELECT s.nid, k.kw
FROM collection s JOIN proteinfact_kw_map k ON (s.protein_fact = k.protein_fact)
EXCEPT
SELECT collection, kw FROM collection_keywords
;

