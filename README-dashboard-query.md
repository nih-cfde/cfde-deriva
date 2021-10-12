
# Statistics Query Client API

This document summarizes the usage of the
`cfde_deriva.dashboard_queries` Python module to generate statistics
about a CFDE C2M2 catalog. It is part of the installable `cfde_deriva`
package.  Examples throughout assume this bit of setup:

```
from cfde_deriva.dashboard_queries import StatsQuery, DashboardQueryHelper
```

## DashboardQueryHelper

The `DashboardQueryHelper` class embodies a connection to an ERMrest
catalog instance containing CFDE C2M2 catalog content. This reusable
object is passed as a constructor argument to the `StatsQuery` class
discussed below.  It also provides a few demonstration and utility
functions of its own.

```
hostname = 'app.nih-cfde.org'
catalogid = '1'
helper = DashboardQueryHelper(hostname, catalogid)
```

This example connects to the initial CFDE deployment. It is important
to understand that in practical use, clients may wish to target other
catalogs to retrieve statistics about specific prepared sets of C2M2
content.

### Access deriva-py APIs

The `DashboardQueryHelper` instance binds to a catalog and has
several sub-objects relevant for lower-level access to the
C2M2 portal catalog:

- `catalog`: an `ErmrestCatalog` instance
- `builder`: a cached result of `catalog.getPathBuilder()`, providing deriva-py's datapath API for data access

The `builder` can be used to retrieve other supporting portal
content. For example:

```
# get a list of all DCCs known by the catalog
dccs = list(helper.builder.CFDE.dcc.path.fetch())
```

## StatsQuery2

The `StatsQuery2` class encapsulates an interface to build
multi-dimensional grouped statistics over C2M2 content. Conceptually,
the caller always makes a sequence of choices:

1. Provide an appropriate helper bound to the target catalog.
2. Specify the base _entity_ type which is being summarized.
3. Specify zero or more _dimension_ categories for grouped statistics.
4. Initiate query execution by _fetching_ results.

The results are a set of rows identified by a combination of
_coordinates_ in each _dimension_. Each coordinate is a set of zero or
more terms representing a combination of terms linked to entities for
that dimensional concept.  Likewise, the row represents a specific
combination of such coordinates across all selected dimensions. Finally,
the row has a statistical summary of the selected _entity_ type, e.g.
a `num_files` count of linked files or `total_size_in_bytes` sum of
all linked files' sizes.

### Entity to be summarized

The set of supported base entities is built into a release of the
`dashboaord_queries` module.  Each supported base entity type has its
own predfined set of statstical measures which are implicitly included
in the query result.

The base entity names are table names from C2M2. Currently these tables
and respective measures are supported:

- `file`
    - `num_files`: count of file records
    - `total_size_in_bytes`: sum of (non-null) `total_size_in_bytes` for counted file records
- `biosample`
    - `num_biosamples`: count of biosample records
- `subject`
    - `num_subjects`: count of subject records
- `collection`
    - `num_collections`: count of collection records

```
file_stats = StatsQuery2(helper).entity('file')
biosample_stats = StatsQuery2(helper).entity('biosample')
subject_stats = StatsQuery2(helper).entity('subject')
```

The preceding queries could be fetched for global statistics, or
optional grouping dimensions could be added prior to fetching in order
to get grouped statistics.

### Dimension for grouped statistics

By adding dimensions to a query, the caller can obtain grouped
statistics for each unique term in that dimension. Adding _n_
dimensions will result in a _n_ -ary grouping key for statistical
results.

The dimension names are related to C2M2 concepts. When a dimension is
added, a new output field will contain a list of linked term information
(each term represented as an object with various fields):

- `anatomy`: "slim" term mapped from original `biosample.anatomy` terms
- `assay_type`
- `compression_type`: C2M2 `file_format` terms related by `file.compression_format`
- `data_type`
- `disease`
- `dcc`: 
- `ethnicity`
- `file_format`: C2M2 `file_format` terms related by `file.file_format`
- `gene`
- `mime_type`
- `ncbi_taxonomy`
- `race`
- `sex`
- `species`: a subset of `ncbi_taxonomy` terms representing subject species
- `substance`
- `subject_granularity`
- `subject_role`

The `dcc` dimension has properties of the C2M2 `dcc` table which
deviate from other vocabulary table definitions:

- `dcc_abbreviation` has no equivalent in vocabularies
- `dcc_name` instead of `name`
- `dcc_description` instead of `description`

### Simple Examples

These examples all involve only a base entity (for counting) and some
metadata about that entity (including the attributed project which
produced the entity).

Global file statistics:
```
StatsQuery2(helper).entity("file").fetch()
```

Per-DCC file statistics:
```
StatsQuery2(helper).entity("file").dimension("dcc").fetch()
```

Per-DCC file statistics further divided by file data type:
```
StatsQuery2(helper).entity("file").dimension("dcc").dimension("data_type").fetch()
```

Global biosample counts:
```
StatsQuery(helper).entity("biosample").fetch()
```

Per-DCC biosample counts:
```
StatsQuery(helper).entity("biosample").dimension("dcc").fetch()
```

Per-DCC biosample counts further divided by biosample assay type:
```
StatsQuery(helper).entity("biosample").dimension("dcc").dimension("assay_type").fetch()
```

Per-DCC biosample counts further divided by biosample anatomy and assay type:
```
StatsQuery(helper).entity("biosample").dimension("dcc").dimension("anatomy").dimension("assay_type").fetch()
```

## StatsQuery

The `StatsQuery` class is a legacy API for accessing a
backwards-compatibility summary of C2M2 content. This summary is based
on a different warehousing approach, and uses single-term coordinates
for dimensions rather than term sets.  This leads to multiple-counting
of entities in many scenarios, e.g. a file related to more than one
biosample might then be related to more than one anatomy term and end
up counted under each term.  This legacy API also does not include support
for more recently added vocabulary concepts in C2M2, such as substance,
gene, or clinical concepts like sex or race.

