
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

## StatsQuery

The `StatsQuery` class encapsulates an interface to build
multi-dimensional grouped statistics over C2M2 content. Conceptually,
the caller always makes a sequence of choices:

1. Provide an appropriate helper bound to the target catalog.
2. Specify the base _entity_ type which is being summarized.
3. Specify zero or more _dimension_ categories for grouped statistics.
4. Initiate query execution by _fetching_ results.

### Entity to be summarized

The set of supported base entities is built into a release of the
`dashboaord_queries` module.  Each supported base entity type has its
own predfined set of statstical measures which are implicitly included
in the query result.

The base entity names are table names from C2M2. Currently these tables
and respective measures are supported:

- `file`
    - `num_files`: count of file records
    - `num_bytes`: sum of (non-null) `size_in_bytes` for counted file records
- `biosample`
    - `num_biosamples`: count of biosample records
- `subject`
    - `num_subjects`: count of subject records


```
file_stats = StatsQuery(helper).entity('file')
biosample_stats = StatsQuery(helper).entity('biosample')
subject_stats = StatsQuery(helper).entity('subject')
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
added, respective dimension fields are added to the query result to
help interpret the grouped statistics:

- `anatomy`: C2M2 `anatomy` term referenced by `biosample`
    - `anatomy`.`id` returned as `anatomy_id`
    - `anatomy`.`name` returned as `anatomy_name`
- `assay_type`: C2M2 `assay_type` term referenced by `biosample`
    - `assay_type`.`id` returned as `assay_type_id`
    - `assay_type`.`name` returned as `assay_typ_name`
- `data_type`: C2M2 `data_type` term referenced by `file`
    - `data_type`.`id` returned as `data_type_id`
    - `data_type`.`name` returned as `data_type_name`
- `species`: C2M2 `ncbi_taxonomy` term with `clade` restricted to `"species"` and linked to `subject` by the `subject_role_taxonomy` association table
    - `ncbi_taxonomy`.`id` returned as `species_id`
    - `ncbi_taxonomy`.`name` returned as `species_name`
- `project_root`: a subset of the DCC submitted `project` table records as transitively attributed by the base entity of the `StatsQuery` and the `project_in_project` relationship
    - `project`.`id_namespace` returned as `project_id_namespace`
    - `project`.`id` returned as `project_id`
    - `project`.`name` returned as `project_name`
    - `project`.`RID` returned as `project_RID`, an internal, surrogate key for projects

The `anatomy`, `assay_type`, and `data_type` dimensions are simply
those terms as referenced by biosample or file entities as listed
above. The `species` dimension is analogous, but it is a filtered
subject of terms from the broader `ncbi_taxonomy` table and the terms
are related to subject entities by the slightly more complex
`subject_role_taxonomy` relationship rather than being simple foreign
key references in C2M2.

The `project_root` dimension is the subset of project entities which
are top-level projects, i.e. they are not listed as being a _member_
of any other project in the C2M2 `project_in_project` association
table. This set of top-level projects should correspond to DCCs aka
data stewards who submitted C2M2 content to the CFDE catalog.

Unlike the other dimensions, the `project_root` dimension has variable
connectivity to the C2M2 model. Because every base entity type has its
own project attribution, this dimension groups by the root project
attributed by the selected base entity in the `StatsQuery`, i.e. the
project specified as the origin for the file, biosample, or subject
records being counted.

Also, projects have a compound key (`id_namespace`, `id`). To simplify
some system integration tasks, we expose an additional surrogate key
(`RID`) which can sometimes be useful to refer to a specific project
in the catalog. These surrogate keys are less deterministic, and so
may vary from catalog instance to catalog instance, even if the same
project (`id_namespace`, `id`) was submitted to each one.

### Implicit query joining

When processing a `StatsQuery`, we lazily expand a core (backbone)
query that joins across the minimal set of required source tables for
the desired information.  The full backbone, including association
tables, looks like this:

- `file`
- `file_describes_biosample`
- `biosample`
- `biosample_from_subject`
- `subject`

Of course, the base entity named in the `StatsQuery.entity(` _entity_
`)` method call is a mandatory part of the query, as statistics are
computed over its constituent rows.

When grouping dimensions are requested, they implicitly demand that we
join additional tables to determine the relationship between the
counted base entity and the requested dimension. At minimum, a
dimension brings in a vocabulary term table, connected to one of the
core base entity tables via a direct foreign key reference. Sometimes,
additional association tables are joined for more complex
relationships in C2M2:

- `anatomy`: `anatomy` joins to `biosample`
- `assay_type`: `assay_type` joins to `biosample`
- `data_type`: `data_type` joins to `file`
- `species`: `ncbi_taxonomy` joins to `subject_role_taxonomy` which joins to `subject`
- `project_root`: `project` joins to `project_root` which joins to `project_in_project_transitive` which joins to base entity

### Simple Examples

These examples all involve only a base entity (for counting) and some
metadata about that entity (including the attributed project which
produced the entity).

Global file statistics: ```
StatsQuery(helper).entity("file").fetch()
```

Per-DCC file statistics: ```
StatsQuery(helper).entity("file").dimension("project_root").fetch()
```

Per-DCC file statistics further divided by file data type: ```
StatsQuery(helper).entity("file").dimension("project_root").dimension("data_type").fetch()
```

Global biosample counts: ```
StatsQuery(helper).entity("biosample").fetch()
```

Per-DCC biosample counts: ```
StatsQuery(helper).entity("biosample").dimension("project_root").fetch()
```

Per-DCC biosample counts further divided by biosample assay type: ```
StatsQuery(helper).entity("biosample").dimension("project_root").dimension("assay_type").fetch()
```

Per-DCC biosample counts further divided by biosample anatomy and assay type: ```
StatsQuery(helper).entity("biosample").dimension("project_root").dimension("anatomy").dimension("assay_type").fetch()
```

### Complex Examples

These examples look similar to the preceding simple examples, but
actually involve much more complex queries because the mixed
dimensions involve different C2M2 entity types and therefore
instantiate more of the core backbone joined query understood by this
module.

Per-DCC file statistics further divided by biosample anatomy and subject species: ```
StatsQuery(helper).entity("file").dimension("project_root").dimension("anatomy").dimension("species")
```

