
"""CFDE utility library for managing and accessing metrics concepts and data.

Most functions require a registry catalog binding.  Some access rights
on the registry content may vary by client and so an appropriate
binding with client-specific credentials is required for different
access scenarios:

  from deriva.core import ErmrestCatalog
  registry_catalog = ErmrestCatalog('https', servername, 'registry', credentials)

"""

from deriva.core import urlquote, DEFAULT_HEADERS


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


def register_datapackage_metrics(registry_catalog, records, update_existing=False, update_cols=['name', 'description']):
    """Idempotently register datapackage_metric terms

    :param registry_catalog: An ErmrestCatalog instance for the registry.
    :param records: A list of dict-like term records.
    :param update_existing: Whether to also update existing metric definitions (default False).
    :param update_cols: Which columns to update when update_existing=True (default updates only name and description).

    A term record has the following mandatory fields:
    - id: CURI-like global id for a metric
    - name: short human-readable name for a metric

    Optional fields will be populated with the default if omitted:
    - rank: floating point ordinal for sorting metrics
    - description: a longer human-readable explanation of a metric
    - hide: boolean "true" to suppress metric displays in portal

    By default, this function will insert new metrics not already
    known by the registry but will leave existing metrics unmodified
    (using existing registry definitions as authoritative). Setting
    update_existing=True will switch this and treat the input records
    as authoritative. However, only the columns named in the
    update_cols parameter will be revised.

    The administrator should coordinate activities to either curate
    live records in the registry or in an authoritative store being
    used to supply the records input to this function. The update_cols
    parameter allows the authoritative content to be split between the
    caller-provided records and the registry's live records on a field
    by field basis.

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

    if update_existing:
        if not isinstance(update_cols, list):
            raise TypeError('update_cols must be a list')

        for cname in update_cols:
            if not isinstance(cname, str):
                raise TypeError('each element of update_cols must be a string')
            allowed_update_cols = {'name', 'description', 'rank', 'hide'}
            if cname not in allowed_update_cols:
                raise ValueError('each element of update_cols must be one of %r' % allowed_update_cols)

        # do an idempotent update while minimizing mutation requests to the registry
        rows = registry_catalog.get('/entity/CFDE:datapackage_metric?limit=none').json()
        existing_by_id = { row['id']: row for row in rows }

        def need_update(existing, goal):
            for cname in update_cols:
                if existing[cname] != goal[cname]:
                    return True
            return False

        updates = [
            v
            for k,v in by_id.items()
            if need_update(existing_by_id[k], v)
        ]

        registry_catalog.put(
            # correlate by existing 'id' and write the other update cols
            '/attributegroup/CFDE:datapackage_metric/id;%s' % (
                ','.join([ urlquote(c) for c in update_cols ])
            ),
            json=updates,
        ).json() # discard response data

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
        value = _get_required_number(record, 'value')
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


def get_datapackage_measurements(registry_catalog, submission_id, headers=DEFAULT_HEADERS):
    """Return a sorted list of all measurement data for a given datapackage.

    :param registry_catalog: An ErmrestCatalog instance bound to catalog_id='registry'
    :param submission_id: The (UUID) id of a submission record in the registry_catalog
    :param headers: A dict-like HTTP request headers object to override the defaults

    The optional headers may be used to pass different credentials in
    each request.

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
        + '@sort(metric_rank,name)',
        headers=headers,
    ).json()
