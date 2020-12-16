
# CFDE Registry

This document summarizes the design and purpose of the CFDE submission
registry.

## Overview

The submission registry is a bookkeeping system for the C2M2
datapackage submission pipeline.  As a records system, it tracks
individual submissions and their processing and review status. It also
stores ancilliary metadata which supports the configuration and
operation of the pipeline.

### Registry Model

The registry model is illustrated in the [registry model
diagram](diagrams/cfde-registry-model.png) and has these core tables
which are populated based on DCC submission activity:

- `datapackage`: each row represents one C2M2 submission
- `datapackage_table`: each row represents one TSV file of one submission

These submission records are augmented by supporting information
which is maintained by adminstrators:

- `dcc`: each row represents one onboarded DCC
- `group`: each row represents one Globus Group known by the
  submission system
- `dcc_group_role`: each row associates a group w/ a pipeline role to
  designate how a set of users relate to a given DCC's usage of the
  submission system

Other aspects of the model are under continued development, but
they do not affect the initial MVP milestone.

### Client Roles

It is intended to support several classes of client/user:

1. Submission ingest pipeline automation
2. DCC staff who submit datapackages
3. DCC staff who review content of datapackages
4. DCC staff who approve datapackages for release
5. DCC staff who administer their DCC's submission content
6. CFDE-CC staff who review submitted datapackages
7. CFDE-CC staff who approve datapackages for release
8. CFDE-CC staff who administer the pipeline
9. Other roles TBD


## Registry Access Policy

The fine-grained policy for the submission system is implemented in
a number of policy elements:

- For simplicity, the bulk of the registry's CFDE schema is made
  visible to the public, not requiring detailed reconfiguration. This
  includes the general informational/vocabulary tables of the
  registry.
  
- The core `datapackage` and `datapackage_table` tables are configured
  with more-specific policies which override the schema-wide defaults
  to make these tables more restrictive.

- The special built-in ERMrest client table is useful for converting
  low-level authentication IDs into human-readable display
  values. However, we conservatively restrict access to certain
  columns which may be considered more sensitive.


This table summarizes these in more detail:

| resource | rights | roles | conditions | impl. | notes |
|----------|--------|------|------------|--------|-------|
| registry `CFDE`.* | select | public | N/A | schema ACL | basic vocabs/config can be public? |
| registry `CFDE`.* | insert | CFDE admin | N/A | schema ACL | admin can insert all vocabs/config |
| registry `CFDE`.* | update | CFDE admin | N/A | schema ACL | admin can update all vocabs/config |
| registry `CFDE`.* | delete | CFDE admin | | N/A | schema ACL | admin can delete all vocabs/config |
| registry `CFDE`.`datapackage` | select | CFDE admin/curator/automation/reviewer | N/A | table ACL | CFDE-CC roles can read all submission records |
| registry `CFDE`.`datapackage` | insert | CFDE admin/automation | N/A | table ACL | CFDE-CC admin or automation can record new submissions |
| registry `CFDE`.`datapackage` | update | CFDE admin/curator/automation | N/A | table ACL | some CFDE-CC roles can edit all submission records |
| registry `CFDE`.`datapackage` | delete | CFDE admin | N/A | table ACL | CFDE-CC admin can delete submissions |
| registry `CFDE`.`datapackage` | select | DCC group | client belongs to any group role with `submitting_dcc` | table ACL-binding | DCC members can see DCC's submissions |
| registry `CFDE`.`datapackage` | update | DCC decider | client belongs to decider group role with `submitting_dcc` | table ACL-binding | DCC decider can edit DCC's submissions |
| registry `CFDE`.`datapackage` | update/delete | DCC admin | client belongs to admin group role with `submitting_dcc` | table ACL-binding | DCC admin can edit/delete DCC's submissions |
| registry `CFDE`.`datapackage`.`id` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage`.`submitting_dcc` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage`.`submitting_user` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage`.`submission_time` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage`.`datapackage_url` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage`.`status` | update | CFDE-CC admin/automation | N/A | column ACL | Can be edited by CFDE-CC admin or automation |
| registry `CFDE`.`datapackage`.`decision_time` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or automation |
| registry `CFDE`.`datapackage`.`review_ermrest_url` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or automation |
| registry `CFDE`.`datapackage`.`review_browse_url` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or automation |
| registry `CFDE`.`datapackage`.`review_summary_url` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or automation |
| registry `CFDE`.`datapackage`.`diagnostics` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or automation |
| registry `CFDE`.`datapackage`.`dcc_approval_status` | update | CFDE-CC admin | N/A | column ACL | Can be edited by CFDE-CC admin |
| registry `CFDE`.`datapackage`.`cfde_approval_status` | update | CFDE-CC admin/curator | N/A | column ACL | Can be edited by CFDE-CC admin or curator |
| registry `CFDE`.`datapackage`.`description` | update | DCC admin/decider | client belongs to decider or admin group role with `submitting_dcc` | inherited table ACL-binding | DCC admin or decider can edit DCC's submission description |
| registry `CFDE`.`datapackage`.`description` | update | DCC admin/decider | client belongs to decider or admin group role with `submitting_dcc` | inerited table ACL-binding | DCC admin or decider can edit DCC's submission approval |
| registry `CFDE`.`datapackage`.* | update | DCC admin/decider | client belongs to decider or admin group role with `submitting_dcc` | masked table ACL-binding | DCC-derived rights are suppressed for all other columns not mentioned previously |
| registry `CFDE`.`datapackage_table` | select | CFDE admin/curator/automation/reviewer | N/A | table ACL | CFDE-CC roles can read all submission records |
| registry `CFDE`.`datapackage_table` | insert | CFDE admin/automation | N/A | table ACL | CFDE-CC admin or automation can record new submissions |
| registry `CFDE`.`datapackage_table` | update | CFDE admin/automation | N/A | table ACL | CFDE-CC admin or automation can edit all submission records |
| registry `CFDE`.`datapackage_table` | delete | CFDE admin | N/A | table ACL | CFDE-CC admin can delete submissions |
| registry `CFDE`.`datapackage_table` | delete | DCC admin | client belongs to admin group role with `submitting_dcc` | table ACL-binding | DCC admin can delete DCC's submissions |
| registry `CFDE`.`datapackage_table`.`datapackage` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage_table`.`position` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage_table`.`table_name` | update | none | N/A | column ACL | Set once during row insertion, then immutable |
| registry `CFDE`.`datapackage_table`.`status` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or automation |
| registry `CFDE`.`datapackage_table`.`num_rows` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or automation |
| registry `CFDE`.`datapackage_table`.`diagnostics` | update | none | N/A | column ACL | Can be edited by CFDE-CC admin or automation |
| registry `CFDE`.`datapackage_table` | select | DCC group | client belongs to any group role with `submitting_dcc` | table ACL-binding | DCC members can see DCC's submissions |
| registry `public`.`ERMrest_Client` | select | public | N/A | table ACL | Anyone who can view a submission should also see the display name of the submitting user |
| registry `public`.`ERMrest_Client` | insert | CFDE-CC admin/automation | N/A | table ACL | Submission can discover new submitting users before they visit registry themselves |
| registry `public`.`ERMrest_Client`.`Email` | select | CFDE admin/curator | N/A | column ACL | Not everyone needs to know a submitting user's email |
| registry `public`.`ERMrest_Client`.`Client_Object` | select | none | N/A | column ACL | No part of the registry or submission needs this at present |

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
   policies. The automation identity is the one recording the
   submission in the registry.

