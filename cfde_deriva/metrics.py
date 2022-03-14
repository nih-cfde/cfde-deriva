
"""CFDE utility library for managing and accessing metrics concepts and data.

Most functions require a registry catalog binding.  Some access rights
on the registry content may vary by client and so an appropriate
binding with client-specific credentials is required for different
access scenarios:

  from deriva.core import ErmrestCatalog
  registry_catalog = ErmrestCatalog('https', servername, 'registry', credentials)

"""

from deriva.core import urlquote


def _get_required(d, k):
    v = d[k]
    if v is None:
        raise ValueError('record field %r must have a value' % (k,))
    return v


def _get_required_number(d, k):
    v = _get_required(d, k)
    if not isinstance(v, (int, float)):
        raise ValueError('record field %r must be a number' % (k,))
    return v


def _get_optional_number(d, k):
    v = d.get(k)
    if v is not None and not isinstance(v, (int, float)):
        raise ValueError('record field %r must be a number or None' % (k,))
    return v


def _is_distinct(v1, v2):
    if v1 is None and v2 is not None:
        return True
    elif v1 is not None and v2 is None:
        return True
    elif v1 != v2:
        return True
    else:
        return False


def _is_stale(existing, goal, keys):
    return any([_is_distinct(existing.get(k), goal.get(k)) for k in keys])


def register_datapackage_metrics(registry_catalog, records):
    """Idempotently register datapackage_metric terms

    :param registry_catalog: An ErmrestCatalog instance for the registry.
    :param records: A list of dict-like term records.

    A term record has the following mandatory fields:
    - id: CURI-like global id for a metric
    - name: short human-readable name for a metric

    Optional fields will be populated with the default if omitted:
    - rank: floating point ordinal for sorting metrics
    - description: a longer human-readable explanation of a metric
    - hide: boolean "true" to suppress metric displays in portal

    This will insert new metrics not already known by the registry but
    will leave existing metrics unmodified (using existing registry
    definitions as authoritative). The administrator should curate the
    live records in the registry if changes to existing metric names,
    descriptions, or ranks are desired.

    """
    by_id = {}
    for record in records:
        curi = _get_required(record, 'id')
        name = _get_required(record, 'name')
        rank = record.get('rank', 1000.0)  # HACK, use same default as in registry schema
        hide = record.get('hide', False)   # HACK, use same default as in registry schema
        description = record.get('description')
        if curi in by_id:
            raise ValueError('records cannot share "id" field %r' % (curi,))
        by_id[curi] = dict(
            id=curi,
            name=name,
            rank=rank,
            hide=hide,
            description=description
        )

    registry_catalog.post(
        '/entity/CFDE:datapackage_metric?onconflict=skip',
        json=list(by_id.values())
    ).json()  # discard response data
    return None


def _check_submission_id(registry_catalog, submission_id):
    rows = registry_catalog.get('/entity/CFDE:datapackage/id=%s' % (urlquote(submission_id)))
    if not rows:
        raise ValueError('submission_id %r is not known by the registry' % (submission_id,))


def register_datapackage_measurements(registry_catalog, submission_id, records):
    """Idempotently register datapackage_measurement data.

    :param registry_catalog: An ErmrestCatalog instance for the registry.
    :param submission_id: The UUID-like id for a datapackage being measured
    :param records: A list of dict-like term records.

    A term record has the following mandatory fields:
    - metric: CURI-like global id for a metric
    - value: floating point measurement value (or null if not measurable)

    Optional fields will be populated with None if omitted:
    - numerator: floating point numerator for ratio-based values
    - denominator: floating point denominator for ratio-based values
    - comment: human-readable text augmenting a measurement value

    """
    _check_submission_id(registry_catalog, submission_id)

    all_metrics = {
        row['id']: row
        for row in registry_catalog.get('/entity/CFDE:datapackage_metric').json()
    }

    by_metric = {}
    for record in records:
        metric = _get_required(record, 'metric')
        value = _get_optional_number(record, 'value')
        numerator = _get_optional_number(record, 'numerator')
        denominator = _get_optional_number(record, 'denominator')
        comment = record.get('comment')
        if metric in by_metric:
            raise ValueError('metric %r cannot be set with multiple values' % (metric,))
        by_metric[metric] = dict(
            datapackage=submission_id,
            metric=metric,
            # HACK: portal uses a compound fkey to sync a copy of metric's (rank, id)
            metric_rank=all_metrics[metric]['rank'],
            value=value,
            numerator=numerator,
            denominator=denominator,
            comment=comment,
        )

    # insert missing rows first
    registry_catalog.post(
        '/entity/CFDE:datapackage_measurement?onconflict=skip',
        json=list(by_metric.values())
    ).json()  # discard response data

    # find and update existing but stale rows
    need_update = [
        by_metric[row['metric']]
        for row in registry_catalog.get(
                '/entity/CFDE:datapackage_measurement/datapackage=%s' % (urlquote(submission_id))
        ).json()
        if row['metric'] in by_metric and _is_stale(
                row,
                by_metric.get(row['metric'], {}),
                ['value', 'numerator', 'denominator', 'comment']
        )
    ]

    registry_catalog.put(
        '/attributegroup/CFDE:datapackage_measurement/datapackage,metric;value,numerator,denominator,comment',
        json=need_update,
    ).json()  # discard response data


def get_datapackage_measurements(registry_catalog, submission_id):
    """Return a sorted list of all measurement data for a given datapackage.

    Result is a list of dict-like measurements with fields:
    - datapackage: UUID-like submission id
    - metric: CURI-like metric concept id
    - metric_rank: sort ordinal configured for concept in registry
    - name: human readable metric concept name
    - description: human readable concept explanation
    - value: floating-point measurement value
    - numerator: numerator for ratio-based values
    - denominator: denominator for ratio-based values
    - comment: human readable augmentive text for value

    Results are sorted by:
    - ascending metric_rank as primary sort key
    - ascending name as secondary sort key to break ties
    """
    _check_submission_id(registry_catalog, submission_id)

    return registry_catalog.get(
        '/attributegroup/M:=CFDE:datapackage_metric/V:=(id)=(CFDE:datapackage_measurement:metric)'
        + ('/datapackage=%s' % urlquote(submission_id))
        + '/V:datapackage,V:metric;V:metric_rank,M:name,M:description'
        + ',V:value,V:numerator,V:denominator,V:comment'
        + '@sort(metric_rank,name)'
    ).json()
