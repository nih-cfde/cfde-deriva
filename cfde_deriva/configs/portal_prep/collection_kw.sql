UPDATE collection AS s
SET kw = cfde_keywords(
      s.local_id,
      s.persistent_id,
      s.abbreviation,
      s.name,
      s.description
    )
;
