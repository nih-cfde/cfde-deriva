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

### Submission configuration

The `cfde_deriva.configs.submission` sub-package includes
configuration data related to ingestion of a C2M2 datapackage from
BDBag archive format into a SQLite database.

The main datapackage schema product
[`c2m2-datapackage.json`](cfde_deriva/configs/submision/c2m2-datapackage.json)
is a copy of the C2M2 model definitions, with minor modifications.

Several `.tsv` files document controlled vocabulary terms which we
load as tabular data to support the portal UI. These are represented
as enumerations in the C2M2 model definition used by a submitter, but
turned back into fields governed by foreign key constraints.

### Portal-Prep configuration

The `cfde_deriva.configs.portal_prep` sub-package includes
configuration data related to SQL-based transformations to renormalize
the C2M2 content into a different form more appropriate for use in
the portal.

The main datapackage schema product
[`cfde-portal-prep.json`](cfde_deriva/configs/portal_prep/cfde-portal-prep.json)
is derived from the C2M2 model but introduces new supporting tables and
renormalizes other columns. Most significantly:

1. a numeric `nid` surrogate key is introduced to tables
2. a `core_fact` table represents equivalence classes of C2M2 entities with common metadata properties
3. all foreign keys are replaced by numeric surrogate key references
4. several transitive-closure tables are computed over existing C2M2 association tables
5. curated vocabulary and slim-maps from the CFDE Ontology WG are loaded from TSV

Numerous `.sql` files encode transformations to either compute rows of a derived
table or compute additional columns on a table.

### Portal configuration

The `cfde_deriva.configs.portal` sub-package includes configuration data related
to the DERIVA-based web portal serving C2M2 inventory information.

The main datapackage schema product
[`cfde-portal.json`](cfde_deriva/configs/portal/cfde-portal.json) is
derived from the Portal-Prep configuration, effectively describing a
subset of the transformation results that will be loaded into the
portal's backing database. This configuration also includes numerous
DERIVA UI presentation hints to customize how the CFDE portal formats
data on screen and provides search and navigation functions.

### Registry configuration

The `cfde_deriva.configs.registry` sub-package includes configuration
data related to building a CFDE submission registry, which tracks C2M2
datapackages submitted by participating DCCs.

## Command-line tools for Development and Operations

### Submission processing

Normally, the `cfde-submit` CLI tool is used by submitters to
introduce new C2M2 datapackages into the submission system. This
invokes a workflow system which eventually executes the
`cfde_deriva.submission.Submission.ingest` method to apply the various
steps of ingest, provisioning, and configuration implied by the above
configuration artifacts.

For CFDE developers and operations staff, a low-level CLI wrapper
provides access to directly execute this code from an appropriate
development or data preparation workstation, while bypassing the
normal hosted workflow system. In order to succeed, the developer or
admin user must provide credentials which include sufficient
write-access privileges in the CFDE-CC system under test.

The `cfde_deriva.submission` CLI is primarily a test stub and
takes a few settings via environment variables:

- `DERIVA_SERVERNAME=app.example.org` (defaults to the dev sandbox API endpoint)
- `CFDE_SKIP_FRICTIONLESS=true` will bypass frictionless package validation (default `false`)
- `CFDE_SKIP_SQL_CHECKS=true` will bypass additional relational data checks (default `{}`)
    - general syntax is a JSON object `{ "check_name": true, ...}` to bypass specific checks
    - default `{}` does not bypass any checks
    - `true` is a equivalent to an object mapping all check names to `true`
- `SQLITE_TMPDIR=/path/to/dir` will override the default SQLite temp file storage location

#### Submission emulation

This test-stub exercises cfde-deriva in a partial system test
scenario, without relying on the hosted workflow system.

`python -m 'cfde_deriva.submission' submit <dcc_id> <archive_url>`

Command-line arguments:

- dcc\_id is a `CFDE`.`dcc`.`id` key from the registry
- archive\_url is a BDBag location accessible to the running tool

NOTE: This emulated submission _does not_ write to the datapackage
archive system. The input BDBag must be placed in an accessible
archive location manually by the developer, prior to using this test
interface.

A full system test requires having the new cfde-deriva package
deployed to the hosted workflow system (action provider) and using
the `cfde-submit` CLI to start a normal ingest flow.

#### Submission rebuild

When a submission already exists in the registry, e.g. in a failed
state, the developer can restart the idempotent ingest process
to attempt to diagnose a software or hosting issue and/or to test
new features of the ingest process.

`python -m 'cfde_deriva.submission' rebuild <submission_id>`

Command-line arguments:

- submission_id is a `CFDE`.`datapackage`.`id` key from the registry

The developer may wish to intervene with manual changes to the
registry and/or ERMrest catalog state of the hosting environemt
to clear the way for certain submission build test scenarios.

#### Submission reconfiguration

When a submission has already been ingested into a data review
catalog, a cheaper process can be applied to test revisions to
the `cfde_deriva.configs.portal` UI presentation hints.

`python -m 'cfde_deriva.submission' reconfigure <submission_id>`

Command-line arguments:

- `submission_id is a `CFDE`.`datapackage`.`id` key from the registry

This process will only amend annotations and/or access control
policies in the already-built ERMrest catalog in order to influence
how content is presented through the web UI.

A batch form can reconfigure all existing review catalogs when
more test coverage is desired on various datapackage content
scenarions:

`python -m 'cfde_deriva.submission' reconfigure-all

It is the operator's responsibility to determine whether this
reconfiguration is appropriate, i.e. that existing review catalog
content is schema-compatible with the configurations being
applied. Older review catalogs may need to be reconfigured by an
older, compatible version of cfde-deriva or conversely purged and
rebuilt with the `submission rebuild` sub-command to upgrade to
the internal portal schema used by the newer software.

#### Submission rebuild all in release

A set of constituent submissions can be rebuilt in bulk by a
helper routine in the release submodule.

`python -m cfde_deriva.release rebuild-submissions <release_id>`

command-line arguments:

- release_id is a `CFDE`.`release`.`id` key from the registry

#### Submission purge

An administrator can purge ERMrest catalog storage resources
associated with a submission, reducing a submission record to basic
historical information.  (Such a submission can be reconstructed later
using the `submission rebuild ...` command above.)

`python -m cfde_deriva.submission purge <submission_id>`

Command-line arguments:

- `submission_id is a `CFDE`.`datapackage`.`id` key from the registry

This purge method can be applied in bulk to many submissions with
two special commands.

`python -m cfde_deriva.submission purge-auto`

`python -m cfde_deriva.submission purge-ALL`

The `purge-auto` variant uses heuristics to identify older submissions
which are likely irrelevant and purges them. The `purge-ALL` variant
purges every submission, useful in certain administrative scenarios.

### Release preparation

When a set of submissions have been ingested, reviewed, and marked
"approved" by the appropriate CF Program staff, a new release can be
prepared using the `cfde_deriva.release` CLI wrapper.

This process is relatively expensive as it, in effect, performs ingest
over again for each constituent datapackage. However, instead of
producing a number of per-submission review catalogs, it produces a
merged release catalog, combining all the constituent submission
information.

#### Defining or maintaining the CFDE next-release draft

The release tools facilitate the preparation of a special draft
release which is intended to track the likely consituent submissions
for the "next CFDE release". When invoked without other arguments, the
release draft sub-command idempotently creates or updates this record
to reflect the latest _approved_ submissions from each DCC:

`python -m cfde_deriva.release draft`

A built-in description value (currently `future release candidates`)
is applied to mark the new draft release, and/or used to locate the
existing record. Each time the command is invoked, the existing
record will be updated to reflect the latest approved submissions.

Environment variables (some shared with the submission module):

- `DERIVA_SERVERNAME=hostname` (defaults to the dev sandbox API endpoint)
- `DRAFT_NEED_DCC=true` whether DCC approval is required for constituent datapackages (default true)
- `DRAFT_NEED_CFDE=false` whether CFDE-CC approval is required for consituent datapackages (default false)

(A draft release is one in the `cfde_registry_rel_status:planning`
state. The implicit next-release draft is both in this state and
bearing the special built-in descrption value.  In the event that an
admin user manually creates multiple draft releases with the same
description field value, this tool will locate and maintain the
_earliest_ draft, sorting by record creation time in the release
registry.)

#### Defining ad-hoc draft releases

The release tools are oriented around release definitions which are
created in the registry _before_ any release data is prepared. Aside
from the CFDE next-release draft, the tool can also create other drafts
with different description values:

`python -m 'cfde_deriva.release' draft new [<description>]`

With this invocation, a new `CFDE`.`release` record is created in the
registry, automatically populated by the most recently submitted and
approved release for each CF Program.  When a release has already been
drafted as such, but you wish to update it to reflect recent
submission or approval activities, the previously generated release_id
is supplied in place of the `new` keyword:

`python -m 'cfde_deriva.release' draft <release_id> [<description>]`

command-line arguments:

- release_id is a `CFDE`.`release`.`id` key from the registry
- description is a short, human-readable string which will be visible in the release registry

A release definition can also be created or revised manually via
the registry's web UI. This CLI wrapper is a convenience layer
to automatically update the stored definitions.

See previous sub-section for environment variables affecting the
`release draft` sub-command.

#### Release build

A release defined in the registry in a draft/pending state can be
built into a catalog using:

`python -m 'cfde_deriva.release' build <release_id>`

command-line arguments:

- release_id is a `CFDE`.`release`.`id` key from the registry

The successful build will result in a new catalog existing on the
server, without affecting the data currently visible as the de facto
public release.  Each built catalog exists at its own distinct catalog
id in the service.

Normally, a build should be from a clean slate, implicitly
provisioning the catalog, ingesting all constitent datapackages, and
loading the merged database content into the new catalog.  However, on
failure the build command can be restarted to attempt idempotent
rebuild. Progress markers persistend periodically during the build
process are used to efficiently skip ahead to the next task, while
underlying idempotent operations are used to allow some work after
the last progress marker to be repeated.

Environment variables:

- `DERIVA_SERVERNAME=hostname` (defaults to the dev sandbox API endpoint)
- `SQLITE_TMPDIR=/path/to/dir` will override the default SQLite temp file storage location
- `REPROVISION_MODEL=false` whether to attempt to apply model changes to a build that is in progress

Normally, restart should use the exact same cfde-deriva codebase and
so all model provisioning occurs once and only once.  However, for
development and test purposes, incremental reprovisioning can be
enabled to attempt to more rapidly test some small changes, such as
the addition of another computed table or column.  However, it is the
developer's responsibility to decide when this short-cut is valid,
and to start over with a clean build when circumstances require it.

#### Release reconfiguration

A release that is already built can be updated with display hints or
other non-data configuration changes, without rebuilding the tabular
data content.

`python -m 'cfde_deriva.release' reconfigure <release_id>`

command-line arguments:

- release_id is a `CFDE`.`release`.`id` key from the registry

#### Release publishing

After a release has been built and inspected by QA users, a quick task
can change which catalog is addressed by the CFDE standard release
catalog ID of `1`.  This uses a DERIVA feature known as a _catalog alias_
wherein the special catalog id `1` is an alias bound to a specific
release catalog, and the alias can be moved to target newer releases
over time.

`python -m 'cfde_deriva.release' publish <release_id>`

command-line arguments:

- release_id is a `CFDE`.`release`.`id` key from the registry

After moving the _catalog alias_ `1` to point to the specified
release's catalog, this tool also attempts to update release status
metadata in the registry to document the change:

1. Change the new release to "public-release" state
2. Replace the built-in next-release description with a release date description
3. Change the previous release to "obsoleted" state

If used in unexpected ways, the tool may move the alias but skip
metadata updates, requiring the CFDE admin users to manually correct
release status via the web UI.

#### Release prune favorites

The user-specific favorite vocabulary terms can be pruned to remove
orphaned terms by using a given release as the reference source for
known terms.

`python -m 'cfde_deriva.release' prune-favorites <release_id>`

- release_id is a `CFDE`.`release`.`id` key from the registry

#### Release refresh resource markdown

The vocabulary term resource markdown content in a release can be
refreshed to match the latest in the registry.

`python -m cfde_deriva.release refresh-resources <release_id>`

Command-line arguments:

- release_id is a `CFDE`.`release.`id` key from the registry

The release must already be in a content-ready or released
state to allow refresh.

#### Release purge

An administrator can purge ERMrest catalog storage resources
associated with a release, reducing a release record to basic
historical information.  (Such a release can be reconstructed later
using the `release build ...` command above.)

`python -m cfde_deriva.release purge <release_id>`

Command-line arguments:

- `release_id is a `CFDE`.`release`.`id` key from the registry

This purge method can be applied in bulk to many releases with
two special commands.

`python -m cfde_deriva.release purge-auto`

`python -m cfde_deriva.release purge-ALL`

The `purge-auto` variant uses heuristics to identify older releases
which are likely irrelevant and purges them. The `purge-ALL` variant
purges every release, useful in certain administrative scenarios.

### Registry maintenance

The registry is a long-lived component of the CFDE portal
infrastructure. However, similar tools are available to perform
maintenance tasks including the provisioning of a brand-new system.

Command-line arguments shared by nearly all registry sub-commands:

- `catalog_id is an ERMrest catalog ID string (default `registry`)

If provided, an alternate catalog ID allows for testing with a
parallel instance of the registry, independent of the normal CFDE
registry maintained with the default ID `registry`.

#### Provision registry

A new registry can be created from scratch as a new ERMrest catalog
which will be initialized with the registry model and initial,
default content.

`python -m cfde_deriva.registry provision [<catalog_id>]`

It is an error to provision a registry with a catalog ID that
already exists in the ERMrest service host.

#### Reprovision registry

An existing registry can be upgraded, i.e. schema-migrated.

`python -m cfde_deriva.registry reprovision [<catalog_id>]`

unlike the previous command, this requires the registry catalog
to already exist, so it performs incremental upgrades.

#### Reconfigure registry

An existing registry can also be given configuration changes, without
model or data updates.

`python -m cfde_deriva.registry reconfigure [<catalog_id>]`

#### Delete registry

An existing registry can be deleted, e.g. to remove a parallel test
registry.

`python -m cfde_deriva.registry delete [<catalog_id>]`

WARNING: care must be taken as this command, if misused, can also
remove other ERMrest catalogs whether or not they were actually
produced by the registry module.

#### Fixup FQDN

If a CFDE service host is moved from one fully-qualified domain name
to another, e.g. due to changes in a load balancer config or DNS
records in the hosting environment, the registry UX will be
internally-inconsistent due to URLs stored in the registry records.  A
fixup command rewrites these URLs to match the current service host
FQDN.

`python -m cfde_deriva.registry fixup-fqdn [<catalog_id>]`

#### Upload resource markdown

Curated, markdown-formatted resource information can be uploaded to
augment vocabulary terms known by the registry.

`python -m cfde_deriva.registry upload-resources vocabname.json...`

Optional environment variables:

- `SKIP_NONMATCHING_RESOURCES=true`: tolerate unkown term IDs

One or more JSON files are supplied and must be named by a vocabulary
table name known by the registry plus the suffix `.json`. For example:

- `anatomy.json`
- `assay_type.json`
- `ethnicity.json`

This list is not exhaustive, so see other C2M2 documentation for
more vocabulary names.

The structure of each input JSON file is an array of simple
records:

```
[
  {"id": "CURI", "resource_markdown": "Resource description using **markdown**."},
  ...
]
```

Each record identifies one existing term by its CURI `id` and supplies
the extra `resource_markdown` payload. By default, any term `id`
present in the JSON file and not in the registry will cause an error,
rejecting the whole file. With `SKIP_NONMATCHING_RESOURCES=true`,
these non-matching resources will be ignored to upload the other
matching terms in the same file.

Markdown often contains newlines and these will be escaped in the JSON
payload as `\n`, a backslash character followed by the character `n`.

As soon as uploads are completed, the results can be reviewed by
browsing the C2M2 vocabulary tables in the submission system itself.
