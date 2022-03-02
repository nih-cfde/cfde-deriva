
INSERT INTO protein_fact (proteins)
VALUES ('[]')
WHERE True
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
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT j.value FROM collection_protein cpr WHERE cpr.collection = col.nid
      )
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
