CREATE TEMPORARY TABLE file_pfacts AS
SELECT
  f.nid,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.substance))
      FROM (
        SELECT a.substance FROM file_describes_subject fds JOIN subject_substance a ON (fds.subject = a.subject) WHERE fds.file = f.nid
        UNION
        SELECT a.substance FROM file_describes_biosample fdb JOIN biosample_substance a ON (fdb.biosample = a.biosample) WHERE fdb.file = f.nid
      ) a
    ),
    '[]'
  ) AS substances,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.compound))
      FROM (
        SELECT v.compound FROM file_describes_subject fds JOIN subject_substance a ON (fds.subject = a.subject) JOIN substance v ON (a.substance = v.nid) WHERE fds.file = f.nid
        UNION
        SELECT v.compound FROM file_describes_biosample fdb JOIN biosample_substance a ON (fdb.biosample = a.biosample) JOIN substance v ON (a.substance = v.nid) WHERE fdb.file = f.nid
      ) a
    ),
    '[]'
  ) AS compounds
FROM file f;
CREATE INDEX IF NOT EXISTS file_pfacts_combo_idx ON file_pfacts(
    substances,
    compounds
);
INSERT INTO pubchem_fact (
    substances,
    compounds
)
SELECT DISTINCT
  ff.substances,
  ff.compounds
FROM file_pfacts ff
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE file AS u
SET pubchem_fact = pcf.nid,
    substances = pcf.substances,
    compounds = pcf.compounds
FROM file_pfacts ff, pubchem_fact pcf
WHERE u.nid = ff.nid
  AND ff.substances = pcf.substances
  AND ff.compounds = pcf.compounds
;

CREATE TEMPORARY TABLE biosample_pfacts AS
SELECT
  b.nid,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.substance))
      FROM (
        SELECT a.substance FROM biosample_from_subject bfs JOIN subject_substance a ON (bfs.subject = a.subject) WHERE bfs.biosample = b.nid
        UNION
        SELECT a.substance FROM biosample_substance a WHERE a.biosample = b.nid
      ) a
    ),
    '[]'
  ) AS substances,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.compound))
      FROM (
        SELECT v.compound FROM biosample_from_subject bfs JOIN subject_substance a ON (bfs.subject = a.subject) JOIN substance v ON (a.substance = v.nid) WHERE bfs.biosample = b.nid
        UNION
        SELECT v.compound FROM biosample_substance a JOIN substance v ON (a.substance = v.nid) WHERE a.biosample = b.nid
      ) a
    ),
    '[]'
  ) AS compounds
FROM biosample b;
CREATE INDEX IF NOT EXISTS biosample_pfacts_combo_idx ON biosample_pfacts(
    substances,
    compounds
);
INSERT INTO pubchem_fact (
    substances,
    compounds
)
SELECT DISTINCT
  bf.substances,
  bf.compounds
FROM biosample_pfacts bf
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE biosample AS u
SET pubchem_fact = pcf.nid,
    substances = pcf.substances,
    compounds = pcf.compounds
FROM biosample_pfacts bf, pubchem_fact pcf
WHERE u.nid = bf.nid
  AND bf.substances = pcf.substances
  AND bf.compounds = pcf.compounds
;

CREATE TEMPORARY TABLE subject_pfacts AS
SELECT
  s.nid,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.substance))
      FROM (
        SELECT a.substance FROM subject_substance a WHERE a.subject = s.nid
        UNION
        SELECT a.substance FROM biosample_from_subject bfs JOIN biosample_substance a ON (bfs.biosample = a.biosample) WHERE bfs.subject = s.nid
      ) a
    ),
    '[]'
  ) AS substances,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT a.compound))
      FROM (
        SELECT v.compound FROM subject_substance a JOIN substance v ON (a.substance = v.nid) WHERE a.subject = s.nid
        UNION
        SELECT v.compound FROM biosample_from_subject bfs JOIN biosample_substance a ON (bfs.biosample = a.biosample) JOIN substance v ON (a.substance = v.nid) WHERE bfs.subject = s.nid
      ) a
    ),
    '[]'
  ) AS compounds
FROM subject s;
CREATE INDEX IF NOT EXISTS subject_pfacts_combo_idx ON subject_pfacts(
    substances,
    compounds
);
INSERT INTO pubchem_fact (
    substances,
    compounds
)
SELECT DISTINCT
  sf.substances,
  sf.compounds
FROM subject_pfacts sf
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE subject AS u
SET pubchem_fact = pcf.nid,
    substances = pcf.substances,
    compounds = pcf.compounds
FROM subject_pfacts sf, pubchem_fact pcf
WHERE u.nid = sf.nid
  AND sf.substances = pcf.substances
  AND sf.compounds = pcf.compounds
;

CREATE TEMPORARY TABLE collection_pfacts AS
SELECT
  col.nid,

  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT a.substance AS value FROM collection_substance a WHERE a.collection = col.nid
        UNION
        SELECT j.value FROM file_in_collection fic, file f, pubchem_fact pcf, json_each(pcf.substances) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.pubchem_fact = pcf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, pubchem_fact pcf, json_each(pcf.substances) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.pubchem_fact = pcf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, pubchem_fact pcf, json_each(pcf.substances) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.pubchem_fact = pcf.nid
      ) s
    ),
    '[]'
  ) AS substances,
  COALESCE((
      SELECT json_sorted(json_group_array(DISTINCT s.value))
      FROM (
        SELECT a.compound AS value FROM collection_compound a WHERE a.collection = col.nid
        UNION
        SELECT v.compound AS value FROM collection_substance a JOIN substance v ON (a.substance = v.nid) WHERE a.collection = col.nid
        UNION
        SELECT j.value FROM file_in_collection fic, file f, pubchem_fact pcf, json_each(pcf.compounds) j WHERE fic.collection = col.nid AND fic.file = f.nid AND f.pubchem_fact = pcf.nid
        UNION
        SELECT j.value FROM biosample_in_collection bic, biosample b, pubchem_fact pcf, json_each(pcf.compounds) j WHERE bic.collection = col.nid AND bic.biosample = b.nid AND b.pubchem_fact = pcf.nid
        UNION
        SELECT j.value FROM subject_in_collection sic, subject s, pubchem_fact pcf, json_each(pcf.compounds) j WHERE sic.collection = col.nid AND sic.subject = s.nid AND s.pubchem_fact = pcf.nid
      ) s
    ),
    '[]'
  ) AS compounds
FROM collection col;
CREATE INDEX IF NOT EXISTS collection_pfacts_combo_idx ON collection_pfacts(
    substances,
    compounds
);
INSERT INTO pubchem_fact (
    substances,
    compounds
)
SELECT DISTINCT
  colf.substances,
  colf.compounds
FROM collection_pfacts colf
WHERE True
ON CONFLICT DO NOTHING
;
UPDATE collection AS u
SET pubchem_fact = pcf.nid,
    substances = pcf.substances,
    compounds = pcf.compounds
FROM collection_pfacts colf, pubchem_fact pcf
WHERE u.nid = colf.nid
  AND colf.substances = pcf.substances
  AND colf.compounds = pcf.compounds
;

CREATE TEMPORARY TABLE pubchemfact_kw AS
  SELECT
    pcf.nid,
    cfde_keywords_merge(
      -- perform split/strip/merge of subst.synonyms too...
      (SELECT cfde_keywords_agg(subst.id, subst.name, subst.description, subst.synonyms)
       FROM json_each(pcf.substances) substj JOIN substance subst ON (substj.value = subst.nid)),

      -- perform split/strip/merge of cmpd.synonyms too...
      (SELECT cfde_keywords_agg(cmpd.id, cmpd.name, cmpd.description, cmpd.synonyms)
       FROM json_each(pcf.compounds) cmpdj JOIN compound cmpd ON (cmpdj.value = cmpd.nid))
    ) AS kw
  FROM pubchem_fact pcf
;

INSERT INTO keywords (kw)
SELECT kw FROM (
SELECT DISTINCT array_join(kw, ' ') AS kw FROM pubchemfact_kw
EXCEPT
SELECT kw FROM keywords
) AS s
WHERE kw IS NOT NULL
  AND kw != ''
;

CREATE TEMPORARY TABLE pubchemfact_kw_map AS
SELECT
  c.nid AS pubchem_fact,
  k.nid AS kw
FROM pubchemfact_kw c
JOIN keywords k ON (c.kw = k.kw)
;

INSERT INTO file_keywords (file, kw)
SELECT s.nid, k.kw
FROM file s JOIN pubchemfact_kw_map k ON (s.pubchem_fact = k.pubchem_fact)
EXCEPT
SELECT file, kw FROM file_keywords
;
INSERT INTO biosample_keywords (biosample, kw)
SELECT s.nid, k.kw
FROM biosample s JOIN pubchemfact_kw_map k ON (s.pubchem_fact = k.pubchem_fact)
EXCEPT
SELECT biosample, kw FROM biosample_keywords
;
INSERT INTO subject_keywords (subject, kw)
SELECT s.nid, k.kw
FROM subject s JOIN pubchemfact_kw_map k ON (s.pubchem_fact = k.pubchem_fact)
EXCEPT
SELECT subject, kw FROM subject_keywords
;
INSERT INTO collection_keywords (collection, kw)
SELECT s.nid, k.kw
FROM collection s JOIN pubchemfact_kw_map k ON (s.pubchem_fact = k.pubchem_fact)
EXCEPT
SELECT collection, kw FROM collection_keywords
;
