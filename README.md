# cfde-deriva
Collaboration point for miscellaneous CFDE-deriva scripts, adapting CFDE concepts to a DERIVA-based hosting platform.

## Python package

This repository includes the sources for the `cfde_deriva` Python
package, which is an SDK to assist with various CFDE tasks. It
has several useful modules:

- [`cfde_deriva.tableschema`](cfde_deriva/tableschema.py) Translation utilities to convert the table-schema JSON document into deriva catalog models, including custom configuration for CFDE presentation goals.
- [`cfde_deriva.datapackage`](cfde_deriva/datapackage.py) Client-side library to prepare a CFDE C2M2 catalog and load it with content from a C2M2 datapackage, wrapped by modules described next.
- [`cfde_deriva.registry`](cfde_deriva/registry.py) Client-side library to manipulate a CFDE submission registry.
- [`cfde_deriva.submission`](cfde_deriva/submission.py) Middleware library for submission pipeline, wrapping preceding libraries with ingest processing logic.
- [`cfde_deriva.dashboard_queries`](cfde_deriva/dashboard_queries.py) Client-side query utilities to extract summary information from a CFDE C2M2 catalog. See additional [documentation for the dashboard-query APIs](README-dashboard-query.md).
- `cfde_deriva.configs` CFDE-maintained configuration data used with the preceding code.
- [`cfde_deriva.release`](cfde_deriva/release.py) Client-side test stub for preparing a release catalog by hand.

These client-side APIs provide CFDE-specific abstractions over
the stock deriva-py client SDK which provides generic access to any
deriva platform instance, regardless of deployed model.

## Configuration data

The preceding Python package includes built-in package data for use
with the code. These are further organized by purpose and included as
standalone data files of various types.

### Portal configuration

The `cfde_deriva.configs.portal` sub-package includes configuration
data related to building a CFDE portal UX over the content of one or
more C2M2 datapackages.

The main datapackage schema product
[`c2m2-level1-portal-model.json`](cfde_deriva/configs/portal/c2m2-level1-portal-model.json)
is a copy of the C2M2 Level 1 model definitions augmented with various
embedded display hints to customize the CFDE portal UI built with
Chaise. This uses the FrictionlessIO Table Schema format with
extensions to embed additional deriva-related concepts.

Various `.tsv` files document controlled vocabulary terms which we
load as tabular data to support the portal UI. These are represented
as enumerations in the C2M2 Level 1 model definition used by a DCC,
but turned back into fields governed by foreign key constraints in the
C2M2 catalogs which support the portal UI.

Various `.sql` files document derived table content we compute from
DCC datapackage content. These derived tables are used by the CFDE
portal UI implementation to assist with presentation.

### Registry configuration

The `cfde_deriva.configs.registry` sub-package includes configuration
data related to building a CFDE submission registry, which tracks C2M2
datapackages submitted by participating DCCs.

