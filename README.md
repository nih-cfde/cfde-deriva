# cfde-deriva
Collaboration point for miscellaneous CFDE-deriva scripts


## Model support

The `table-schema/` folder contains various model-related artifacts
used to build CFDE catalogs.

The main datapackage schema product
[`c2m2-level1-portal-model.json`](table-schema/c2m2-level1-portal-model.json)
is a copy of the C2M2 Level 1 model definitions augmented with various
embedded display hints to customize the CFDE portal UI built with
Chaise. This uses the FrictionlessIO Table Schema format with
extensions to embed additional deriva-related concepts.

Various `.sql` files document derived table content we compute from
DCC datapackage content. These derived tables are used by the CFDE
portal UI implementation to assist with presentation.

## Python module

This repository includes the sources for the `cfde_deriva` Python
package, which is an SDK to assist with various CFDE tasks. It
has several useful modules:

- [`cfde_deriva.tableschema`](cfde_deriva/tableschema.py) Translation utilities to convert the table-schema JSON document into deriva catalog schema definitions.
- [`cfde_deriva.datapackage`](cfde_deriva/datapackage.py) Client-side data utilities to prepare a CFDE deriva catalog and load it with content from a C2M2 datapackage (e.g. a bdbag containing the table-schema JSON file and associated TSV files with actual metadata content).
- [`cfde_deriva.dashboard_queries`](cfde_deriva/dashboard_queries.py) Client-side query utilities to extract summary information from a CFDE deriva catalog. See additional [documentation for the dashboard-query APIs](README-dashboard-query.md).

These client-side APIs provide CFDE C2M2-specific abstractions over
the stock deriva-py client SDK which provides generic access to any
deriva platform instance, regardless of deployed model.

