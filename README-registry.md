
# CFDE Registry

This document summarizes the design and purpose of the CFDE data submission
and user-preferences registry.

## Overview

The CFDE registry is a bookkeeping system for:

1. The C2M2 datapackage submission pipeline.
2. Metadata related to DCC federation.
3. User-managed preferences/profile data.

The registry data model is illustrated in the [registry model
diagram](diagrams/cfde-registry-model.png).

### DCC Federation State Tables

These registry tables are populated by CFDE-CC staff activity:

- `dcc`: each row represents an onboarded DCC
- `group`: each row represents a known group in the authentication system
- `dcc_group_role`: each row binds a known group to a permission class for a DCC
- `id_namespace`: each row represents a CFDE federated identifier namespace

Other aspects of DCC federation are under continued development.

### Ontological Tables

These registry tables are populated by a mixture of CFDE-CC staff and
submission system activity, representing known C2M2 vocabulary concepts:

- `anatomy`
- `assay_type`
- `data_type`
- `file_format`
- `ncbi_taxonomy`
- `subject_granularity`
- `subject_role`

Generally, these share structure with C2M2 vocabulary tables present
in submissions, review catalogs, and release catalogs. However, we may
introduce additional columns relevant to registry functionality.

It is expected that CFDE-CC and/or DCC staff may pre-populate these
tables with terms intended for common use.  Additionally, the tables
may indicate automatically-detected terms found in C2M2 submissions.

The usage of these tables is under continued development.

### Submission System State Tables

These registry tables are populated based on DCC submission system
activity:

- `datapackage`: each row represents one C2M2 submission
- `datapackage_table`: each row represents one TSV file of one submission

These tables are populated by release-planning and preparation:

- `release`: each row represents a CFDE inventory release in some stage of planning
- `dcc_release_datapackage`: each row binds a constituent submission to a release

Other aspects of submission-tracking under continued development.

### User Profile State Tables

These registry tables are populated by user activity and represent
their saved preferences or other state values relevant to
personalization of CFDE service features:

- `user_profile`: each row represents scalar settings for one user
- `saved_query`: each row represents one saved query for a user
- `favorite_anatomy`: each row represents one favorited vocabulary term for a user
- `favorite_assay_type`: each row represents one favorited vocabulary term for a user
- `favorite_data_type`: each row represents one favorited vocabulary term for a user
- `favorite_file_format`: each row represents one favorited vocabulary term for a user
- `favorite_ncbi_taxonomy`: each row represents one favorited vocabulary term for a user

The `user_profile` table is keyed by authenticated user ID which is
also a foreign key to the built-in `ERMrest_Client` table. Each user
can have at most one profile record associated with their
identity. The latter table is automatically populated by the DERIVA
system, while the former will be populated by an explicit user action
requesting that a profile be created.  The profile record will store
any scalar settings for the user, as a single column for each named
setting.

The `saved_query` table is keyed by ??? and the authenticated user
ID. Each user can have zero or more saved query records associated
with their identity. Each record will store necessary information to
reconstitute a query in Chaise, to name/describe the query in a query
listing UI, and other system metadata TBD.

Generally, the various "favorite" tables form binary associations to
link a subset of vocabulary concepts from a given vocabulary table to
a given user's profile.


### Client Roles

The registry supports several classes of client/user. Generally, one
group corresponds directly to a role, and other groups for
higher-privilege roles also enjoy the same privileges:

1. Submission ingest pipeline automation (CFDE-hosted machine identity)
    - CFDE Submission Pipeline
2. DCC staff who review content of datapackages (read-only)
    - CFDE _DCC_ Reviewer
    - also CFDE _DCC_ Approver
    - also CFDE _DCC_ Submitter
    - also CFDE _DCC_ Admin (not in current practice?)
3. DCC staff who submit datapackages
    - CFDE _DCC_ Submitter
    - also CFDE _DCC_ Admin (not in current practice?)
4. DCC staff who approve datapackages for release
    - CFDE _DCC_ Approver
    - also CFDE _DCC_ Admin (not in current practice?)
5. DCC staff who administer their DCC's submission content
    - CFDE _DCC_ Admin (not in current practice?)
6. CFDE-CC staff who review submitted datapackages (read-only)
    - CFDE Portal Reviewer
    - also CFDE Portal Curator
    - also CFDE Portal Admin
    - also CFDE Infrastructure Operations
7. CFDE-CC staff who review and approve datapackages for release (CFDE-CC decision data-entry)
    - CFDE Portal Curator
    - also CFDE Portal Admin
    - also CFDE Infrastructure Operations
8. CFDE-CC staff who administer the pipeline, onboard DCCs, can also submit:
    - CFDE Portal Admin
    - also CFDE Infrastructure Operations
9. CFDE-CC staff with highest permissions on infrastructure
    - CFDE Infrastructure Operations
10. General users who have personal preferences/profile data
    - a member of a general CFDE Portal group?
    - during dev cycle: CFDE Portal Curator, CFDE Portal Reviewer, CFDE Portal Writer, CFDE Portal Reader
11. The owner of a particular profile
    - where the profile `id` or related content `user_id` matches the authenticated client

Other roles TBD.

## Registry Access Policy

The fine-grained policy for the submission system is implemented in
a number of policy elements:

- For simplicity, the bulk of the registry's CFDE schema is made
  visible to the public, not requiring detailed reconfiguration. This
  includes the general informational/vocabulary tables of the
  registry. Only portal administrators can write to these tables.
  
- The core `datapackage` and `datapackage_table` tables are configured
  with more-specific policies which override the schema-wide defaults
  to make these tables more restrictive for read access and to allow
  the automated submission system to perform certain updates.

- The special built-in ERMrest client table is useful for converting
  low-level authentication IDs into human-readable display
  values. However, we conservatively restrict access to certain
  columns which may be considered more sensitive.


This table summarizes these in more detail:

| resource | rights | roles | conditions | impl. | notes |
|----------|--------|------|------------|--------|-------|
| registry catalog | enumerate | public | N/A | catalog ACL | tables detected (chaise avoids table-not-found) |
| registry `CFDE`.* | select | public | N/A | schema ACL | basic vocabs/config can be public? |
| registry `CFDE`.* | insert, update, delete | CFDE admin | N/A | schema ACL | admin can modify all vocabs/config |
| registry `CFDE`. _vocab_ | select | public | N/A | table ACL | everyone can view vocabulary term sets |
| registry `CFDE`. _vocab_ | insert | CFDE admin/curator/pipeline | N/A | table ACL | staff can curate vocabulary terms, pipeline can add newly encountered terms |
| registry `CFDE`. _vocab_ | update, delete | CFDE admin/curator | N/A | table ACL | staff can curate vocabulary terms |
| registry `CFDE`.`datapackage` | select | CFDE admin/curator/pipeline/reviewer | N/A | table ACL | CFDE-CC roles can read all submission records |
| registry `CFDE`.`datapackage` | insert | CFDE pipeline | N/A | table ACL | CFDE-CC pipeline can record new submissions |
| registry `CFDE`.`datapackage` | update | CFDE admin/curator/pipeline | N/A | table ACL | some CFDE-CC roles can edit all submission records |
| registry `CFDE`.`datapackage` | delete | none | N/A | table ACL | no CFDE-CC role can delete submissions (except ops/infrastructure) |
| registry `CFDE`.`datapackage` | select | DCC group | client belongs to any group role for `submitting_dcc` | table ACL-binding | DCC members can see DCC's submissions |
| registry `CFDE`.`datapackage` | update | DCC decider, admin | client belongs to decider group role for `submitting_dcc` | table ACL-binding | DCC admin or decider can edit DCC's submissions |
| registry `CFDE`.`datapackage`.`id` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage`.`submitting_dcc` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage`.`submitting_user` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage`.`submission_time` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage`.`datapackage_url` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage`.`status` | update | CFDE-CC admin/pipeline | N/A | column ACL | Can be edited by CFDE-CC admin or pipeline |
| registry `CFDE`.`datapackage`.`decision_time` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or pipeline |
| registry `CFDE`.`datapackage`.`review_ermrest_url` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or pipeline |
| registry `CFDE`.`datapackage`.`review_browse_url` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or pipeline |
| registry `CFDE`.`datapackage`.`review_summary_url` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or pipeline |
| registry `CFDE`.`datapackage`.`diagnostics` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or pipeline |
| registry `CFDE`.`datapackage`.`dcc_approval_status` | update | CFDE-CC admin | N/A | column ACL | Can be edited by CFDE-CC admin |
| registry `CFDE`.`datapackage`.`cfde_approval_status` | update | CFDE-CC admin/curator | N/A | column ACL | Can be edited by CFDE-CC admin or curator |
| registry `CFDE`.`datapackage`.`description` | update | DCC admin/decider | client belongs to decider or admin group role with `submitting_dcc` | inherited table ACL-binding | DCC admin or decider can edit DCC's submission description |
| registry `CFDE`.`datapackage`.`dcc_approval_status` | update | DCC admin/decider | client belongs to decider or admin group role with `submitting_dcc` | inerited table ACL-binding | DCC admin or decider can edit DCC's submission approval |
| registry `CFDE`.`datapackage`.* | update | DCC admin/decider | client belongs to decider or admin group role with `submitting_dcc` | masked table ACL-binding | DCC-derived rights are suppressed for all other columns not mentioned previously |
| registry `CFDE`.`datapackage_table` | select | CFDE admin/curator/pipeline/reviewer | N/A | table ACL | CFDE-CC roles can read all submission records |
| registry `CFDE`.`datapackage_table` | insert, update | CFDE admin/pipeline | N/A | table ACL | CFDE-CC admin or pipeline can record or edit submissions |
| registry `CFDE`.`datapackage_table` | delete | CFDE admin | N/A | table ACL | CFDE-CC admin can delete submissions |
| registry `CFDE`.`datapackage_table`.`datapackage` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage_table`.`position` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage_table`.`table_name` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage_table`.`status` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or pipeline |
| registry `CFDE`.`datapackage_table`.`num_rows` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or pipeline |
| registry `CFDE`.`datapackage_table`.`diagnostics` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or pipeline |
| registry `CFDE`.`datapackage_table` | select | DCC group | client belongs to any group role with `submitting_dcc` | table ACL-binding | DCC members can see DCC's submissions |
| registry `CFDE`.`datapackage_` _vocab_ | select | CFDE admin/curator/pipeline/reviewer | N/A | table ACL | CFDE-CC roles can read all submission records |
| registry `CFDE`.`datapackage_` _vocab_ | insert | CFDE admin/pipeline | N/A | table ACL | CFDE-CC admin or pipeline can record new submissions |
| registry `CFDE`.`datapackage_` _vocab_ | update, delete | CFDE admin | N/A | table ACL | CFDE-CC admin can modify all submission records |
| registry `CFDE`.`user_profile` | insert | CFDE community | N/A | table ACL | Community members can create a profile |
| registry `CFDE`.`user_profile` | select, update, delete | CFDE admin | N/A | table ACL | CFDE-CC admin can read all profiles |
| registry `CFDE`.`user_profile` | update, delete | CFDE admin | user `id` matches client | table ACL binding | user can edit their own profile |
| registry `CFDE`.`user_profile`.`id` | update | none | N/A | fkey ACL | user id is immutable |
| registry `CFDE`.`user_profile`.`id` fkey | insert | CFDE admin | N/A | fkey ACL | only CFDE-CC admin can set other profile users |
| registry `CFDE`.`user_profile`.`id` fkey | insert | user-self | N/A | fkey ACL binding | user can only set profile user `id` to self |
| registry `CFDE`.`saved_query` | insert | CFDE community | N/A | table ACL | Community members can create |
| registry `CFDE`.`saved_query` | select, update, delete | CFDE admin | N/A | table ACL | CFDE-CC admin can read and modify all|
| registry `CFDE`.`saved_query` | select, update, delete | CFDE admin | user `user_id` matches client | table ACL binding | user can view and edit their own |
| registry `CFDE`.`saved_query`.`user_id` | update | none | N/A | user_id is immutable |
| registry `CFDE`.`saved_query`.`table_name` | update | none | N/A | column ACL | table name is immutable |
| registry `CFDE`.`saved_query`.`facets` | update | none | N/A | column ACL | facets blob is immutable |
| registry `CFDE`.`saved_query`.`user_id` fkey | insert | profile owner | user_id matches client | user can set own user ID in profile related records |
| registry `CFDE`.`favorite_*` | insert | CFDE community | N/A | table ACL | Community members can create |
| registry `CFDE`.`favorite_*` | select, update, delete | CFDE admin | N/A | table ACL | CFDE-CC admin can read and modify all|
| registry `CFDE`.`favorite_*` | select, update, delete | CFDE admin | user `user_id` matches client | table ACL binding | user can view and edit their own |
| registry `CFDE`.`favorite_*`.`user_id` | update | none | N/A | user_id is immutable |
| registry `CFDE`.`favorite_*`.`user_id` fkey | insert | profile owner | user_id matches client | user can set own user ID in profile related records |
| registry `public`.`ERMrest_Client` | select | users | client matches record ID | table ACL-binding | User can see their own full ERMrest_Client record |
| registry `public`.`ERMrest_Client` | insert | CFDE-CC admin + pipeline | N/A | table ACL | Submission can discover new submitting users before they visit registry themselves |
| registry `public`.`ERMrest_Client`.`Email` | select | CFDE admin/curator | N/A | column ACL | Not everyone needs to know a submitting user's email |
| registry `public`.`ERMrest_Client`.`Client_Object` | enumerate | CFDE-CC pipeline | N/A | column ACL | Column detectable for pipeline deriva-py operations |
| registry `public`.`ERMrest_Client`.`Client_Object` | select | none | N/A | column ACL | No part of the registry or submission needs this at present |
| review catalog | enumerate | public | N/A | catalog ACL | tables detected (chaise avoids table-not-found) |
| review catalog | owner | CFDE-CC ops/pipeline | N/A | catalog ACL | Pipeline needs to own since it creates catalog, ops should own everything |
| review `CFDE`.* | select | CFDE-CC admin/curator + DCC admin/reviewer/decider/submitter | N/A | schema ACL | CFDE-CC roles and DCC groups with role for submitting DCC can view content |
| review `public`.* | select | none | N/A | schema ACL | non-CFDE tables hidden from users |
| release catalog | enumerate | public | N/A | catalog ACL | tables detected (chaise avoids table-not-found) |
| release catalog | owner | CFDE-CC ops/pipeline | N/A | catalog ACL | Pipeline needs to own since it creates catalog, ops should own everything |
| release `CFDE`.* | select | public | N/A | schema ACL | CFDE releases are visible to public |
| release `public`.* | select | none | N/A | schema ACL | non-CFDE tables hidden from users |

The table-level ACLs override the schema-wide defaults and make
certain tables more restrictive.  Likewise, the column-level ACLs
override the table-wide or schema-wide defaults and make specific
columns more restrictive.

In DERIVA, the schema, table, and column ACLs are considered
_data-independent_ policies. They grant a given access right for all
rows of a table.

The table and column ACL bindings are considered _data-dependent_ and
they grant a given access for rows of a table only when certain
conditions are met by the content of that row.
   
The table-level ACL-bindings are inherited by all columns of the table
and so pass-through the data-dependent access privilege. Columns which
should not get these rights are masked off with overriding statements
to disable these passthrough privileges.

Notes:

1. The schema-level `CFDE`.* ACLs provide a _default_ policy for all
   tables in the schema which lack their own custom ACLs. These ACLs
   capture a baseline policy for the registry, which is to be
   read-only accessible to everyone and writable by the CFDE-CC
   admins. The majority of the registry tables are vocabulary and
   configuration which should not be edited by untrusted parties, but
   which is safe to show in a read-only fashion.

2. The built-in ERMrest clients table is useful for showing system
   provenance for which authenticated client changed what row. It is
   also used to represent the "submitting user" concept of the
   registry. Showing this table to all users ensures that reasonable
   display names can be presented for this information.

   - However, the email address column might be considered sensitive
     and will be restricted to CFDE-CC admin and curator roles who
     might have a need to contact a responsible party.

   - The `Client_Object` may contain additional account information
     and does not need to be seen by any of the normal user
     roles. Only the infrastructure operator will have access to this
     column.

3. The core authorization decision for whether a user can make a
   submission is enforced by the submission ingest pipeline logic. The
   server-side automation runs trusted code to do this, and hence
   there is no specific ERMrest policy to reflect per-DCC submission
   policies. The pipeline identity is the one recording the
   submission in the registry.

4. The review catalog ACLs and `CFDE` schema ACLs are configured for
   each submission, based on the submission's `submitting_dcc` and the
   groups known by the registry (with appropriate DCC group roles) _at
   the time of submission ingest_. This is primarily to adjust read
   privileges on review content.  The infrastructure ownership aspects
   do not vary per DCC.
