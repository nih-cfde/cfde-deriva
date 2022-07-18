
import os
import sys
import datetime
import json
import logging
import urllib3

from deriva.core import DerivaServer, ErmrestCatalog, get_credential, DEFAULT_SESSION_CONFIG, init_logging, urlquote
from deriva.core.ermrest_model import nochange
from deriva.core.datapath import ArrayD
from deriva.core.utils.core_utils import AttrDict

from . import exception
from .tableschema import RegistryConfigurator, authn_id, terms
from .datapackage import CfdeDataPackage, registry_schema_json, tag

logger = logging.getLogger(__name__)

ermrest_creators_acl = [
    authn_id.cfde_infrastructure_ops,
    authn_id.cfde_portal_admin,
    authn_id.cfde_submission_pipeline,
]

class WebauthnAttribute (object):
    """Represents authenticated attribute (i.e. group membership) of a user.
    
    """
    def __init__(self, attr_id, display_name):
        """Construct attribute summary from available webauthn context information.

        :param attr_id: The webauthn attribute URI
        :param display_name: The display name of this attribute
        """
        if not attr_id.startswith('https://'):
            raise ValueError('"%s" is not a webauthn attribute URI' % attr_id)
        self.webauthn_id = attr_id
        self.display_name = display_name

    @classmethod
    def from_globus(cls, globus_group_id, display_name):
        """Sugar to construct via raw globus group info.

        :param globus_group_id: The bare globus group UUID as a string
        :param display_name: The display name of the group
        """
        attr_id = 'https://auth.globus.org/%s' % (globus_group_id,)
        return cls(attr_id, display_name)
        
    @classmethod
    def check(cls, attr):
        if not isinstance(attr, cls):
            raise TypeError('expected instance of %s, not %s' % (cls, type(attr)))
        return attr

class WebauthnUser (object):
    """Represents authenticated context of a user."""
    def __init__(self, client_id, display_name, full_name, email, attributes):
        """Construct user summary from available webauthn context information.

        :param client_id: The webauthn client URI
        :param display_name: The display name for this client
        :param full_name: The full name of this client
        :param email: The email address of this client
        :param attributes: A list of verified WebauthnAttribute instances for this client
        """
        if not client_id.startswith('https://'):
            raise ValueError('"%s" is not a webauthn client URI' % attr_id)
        self.webauthn_id = client_id
        self.display_name = display_name
        self.full_name = full_name
        self.email = email
        self.attributes_dict = {
            attr.webauthn_id: attr
            for attr in [
                    WebauthnAttribute.check(attr)
                    for attr in attributes
            ]
        }
        if self.webauthn_id not in self.attributes_dict:
            # idempotently add client ID to attributes
            self.attributes_dict[self.webauthn_id] = self

    @classmethod
    def from_globus(cls, globus_user_id, display_name, full_name, email, attributes):
        """Sugar to construct via raw globus authentication info.

        :param globus_user_id: The bare globus user UUID as a string
        :param display_name: The display name of the user
        :param full_name: The full name of the user
        :param email: The email adress of the user
        :Param attributes: The list of webauthn attributes of the user

        The attributes can be individually constructed from globus
        group info using the WebauthnAttribute.from_globus(...) constructor.
        """
        client_id = 'https://auth.globus.org/%s' % (globus_user_id,)
        return cls(client_id, display_name, full_name, email, attributes)

    @classmethod
    def check(cls, user):
        if not isinstance(user, cls):
            raise TypeError('expected instance of %s, not %s' % (cls, type(user)))

    def acl_authz_test(self, acl, error='Requested access to resource is forbidden.'):
        """Enforce ACL intersection with user context.

        :param acl: A webauthn ACL, a list of webauthn URIs or the wildcard '*'

        Raises Forbidden if this user context does not intersect the ACL.
        """
        acl = set(acl)
        context = set(self.attributes_dict.keys())
        context.add('*')
        if context.intersection(acl):
            return True
        raise exception.Forbidden(error)

class Registry (object):
    """CFDE Registry binding.

    """
    def __init__(self, scheme='https', servername='app.nih-cfde.org', catalog='registry', credentials=None, session_config=None):
        """Bind to specified registry.

        Note: this binding operates as an authenticated client
        identity and may expose different capabilities depending on
        the client's role within the organization.
        """
        if credentials is None:
            credentials = get_credential(servername)
        if not session_config:
            session_config = DEFAULT_SESSION_CONFIG.copy()
        session_config["allow_retry_on_all_methods"] = True
        self._catalog = ErmrestCatalog(scheme, servername, catalog, credentials, session_config=session_config)
        self._builder = self._catalog.getPathBuilder()

    def validate_dcc_id(self, dcc_id, submitting_user):
        """Validate that user has submitter role with this DCC according to registry.

        :param dcc_id: The dcc.id key of the DCC in the registry.
        :param submitting_user: The WebauthnUser representation of the authenticated submission user.

        Raises UnknownDccId for invalid DCC identifiers.
        Raises Forbidden if submitting_user is not a submitter for the named DCC.
        """
        rows = self.get_dcc(dcc_id)
        if len(rows) < 1:
            raise exception.UnknownDccId(dcc_id)
        self.enforce_dcc_submission(dcc_id, submitting_user)

    def _get_entity(self, table_name, id=None, sortby=None):
        """Get one or all entity records from a registry table.

        :param table_name: The registry table to access.
        :param id: A key to retrieve one row (default None retrieves all)
        """
        path = self._builder.CFDE.tables[table_name].path
        if id is not None:
            path = path.filter(path.table_instances[table_name].column_definitions['id'] == id)
        results = path.entities()
        if sortby is not None:
            if isinstance(sortby, str):
                sortby=[sortby]
            results = results.sort(*[ path.table_instances[table_name].column_definitions[cname] for cname in sortby ])
        return list( results.fetch() )

    def get_user(self, client_id):
        """Get WebauthnUser instance representing existing registry user with client_id.

        :param client_id: The public.ERMrest_Client.ID or CFDE.datapackage.submitting_user key value
        """
        path = self._builder.public.ERMrest_Client.path
        path = path.filter(path.ERMrest_Client.ID == client_id)
        rows = list(path.entities().fetch())
        if rows:
            row = rows[0]
            return WebauthnUser(
                row['ID'],
                row['Display_Name'],
                row.get('Full_Name'),
                row.get('Email'),
                []
            )
        else:
            raise ValueError("Registry user with ID=%r not found." % (client_id,))

    def list_datapackages(self, sortby=None):
        """Get a list of all datapackage submissions in the registry

        """
        return self._get_entity('datapackage', sortby=None)

    def list_releases(self, sortby=None):
        """Get a list of all release definitions in the registry

        """
        return self._get_entity('release', sortby=sortby)

    def get_latest_approved_datapackages(self, need_dcc_appr=True, need_cfde_appr=True):
        """Get a map of latest datapackages approved for release for each DCC id."""
        path = self._builder.CFDE.tables['datapackage'].path
        status = path.datapackage.status
        path = path.filter( (status == terms.cfde_registry_dp_status.content_ready) | (status == terms.cfde_registry_dp_status.release_pending) )
        if need_dcc_appr:
            path = path.filter(path.datapackage.dcc_approval_status == terms.cfde_registry_decision.approved)
        if need_cfde_appr:
            path = path.filter(path.datapackage.cfde_approval_status == terms.cfde_registry_decision.approved)
        res = {}
        for row in path.entities().sort(path.datapackage.submitting_dcc,  path.datapackage.submission_time.desc):
            if row['submitting_dcc'] not in res:
                res[row['submitting_dcc']] = row
        return res

    def get_datapackage(self, id):
        """Get datapackage by submission id or raise exception.
        
        :param id: The datapackage.id key for the submission in the registry

        Raises DatapackageUnknown if record is not found.
        """
        rows = self._get_entity('datapackage', id)
        if len(rows) < 1:
            raise exception.DatapackageUnknown('Datapackage "%s" not found in registry.' % (id,))
        return rows[0]

    def get_datapackage_table(self, datapackage, position):
        """Get datapackage by submission id or raise exception.

        :param datapackage: The datapackage.id key for the submission in the registry
        :param position: The 0-based index of the table in the datapackage's list of resources

        Raises IndexError if record is not found.
        """
        path = self._builder.CFDE.datapackage_table.path
        path = path.filter(path.datapackage_table.datapackage == datapackage)
        path = path.filter(path.datapackage_table.position == position)
        rows = list( path.entities().fetch() )
        if len(rows) < 1:
            raise IndexError('Datapackage table ("%s", %d) not found in registry.' % (datapackage, position))
        return rows[0]

    def register_release(self, id, dcc_datapackages, description=None):
        """Idempotently register new release in registry, returning (release row, dcc_datapackages).

        :param id: The release.id for the new record
        :param dcc_datapackages: A dict mapping {dcc_id: datapackage, ...} for constituents
        :param description: A human-readable description of this release

        The constituents are a set of datapackage records (dicts) as
        returned by the get_datapackage() method. The dcc_id key MUST
        match the submitting_dcc of the record.

        For repeat calls on existing releases, the definition will be
        updated if the release is still in the planning state, but a
        StateError will be raised if it is no longer in planning state.

        """
        for dcc_id, dp in dcc_datapackages.items():
            if dcc_id != dp['submitting_dcc']:
                raise ValueError('Mismatch in dcc_datapackages DCC IDs %s != %s' % (dcc_id, dp['submitting_dcc']))

        try:
            rel, old_dcc_dps = self.get_release(id)
        except exception.ReleaseUnknown:
            # create new release record
            newrow = {
                'id': id,
                'status': terms.cfde_registry_rel_status.planning,
                'description': None if description is nochange else description,
            }
            defaults = [
                cname
                for cname in self._builder.CFDE.release.column_definitions.keys()
                if cname not in newrow
            ]
            logger.info('Registering new release %s' % (id,))
            self._catalog.post(
                '/entity/CFDE:release?defaults=%s' % (','.join(defaults),),
                json=[newrow]
            )
            rel, old_dcc_dps = self.get_release(id)

        if rel['status'] != terms.cfde_registry_rel_status.planning:
            raise exception.StateError('Idempotent registration disallowed on existing release %(id)s with status=%(status)s' % rel)

        # prepare for idempotent updates
        old_dp_ids = { dp['id'] for dp in old_dcc_dps.values() }
        dp_ids = { dp['id'] for dp in dcc_datapackages.values() }
        datapackages = {
            dp['id']: dp
            for dp in dcc_datapackages.values()
        }

        # idempotently revise description
        if rel['description'] != description:
            logger.info('Updating release %s description: %s' % (id, description,))
            self.update_release(id, description=description)

        # find currently registered constituents
        path = self._builder.CFDE.dcc_release_datapackage.path
        path = path.filter(path.dcc_release_datapackage.release == id)
        old_dp_ids = { row['datapackage'] for row in path.entities().fetch() }

        # remove stale consituents
        for dp_id in old_dp_ids.difference(dp_ids):
            logger.info('Removing constituent datapackage %s from release %s' % (dp_id, id))
            self._catalog.delete(
                '/entity/CFDE:dcc_release_datapackage/release=%s&datapackage=%s' % (urlquote(id), urlquote(dp_id),)
            )

        # add new consituents
        new_dp_ids = dp_ids.difference(old_dp_ids)
        if new_dp_ids:
            logger.info('Adding constituent datapackages %s to release %s' % (new_dp_ids, id))
            self._catalog.post(
                '/entity/CFDE:dcc_release_datapackage',
                json=[
                    {
                        'dcc': datapackages[dp_id]['submitting_dcc'],
                        'release': id,
                        'datapackage': dp_id,
                    }
                    for dp_id in new_dp_ids
                ]
            )

        # return registry content
        return self.get_release(id)

    def get_release(self, id):
        """Get release by submission id or raise exception, returning (release_row, dcc_datapackages).
        
        :param id: The release.id key for the release definition in the registry

        Raises ReleaseUnknown if record is not found.
        """
        rows = self._get_entity('release', id)
        if len(rows) < 1:
            raise exception.ReleaseUnknown('Release "%s" not found in registry.' % (id,))
        rel = rows[0]
        path = self._builder.CFDE.dcc_release_datapackage.path
        path = path.filter(path.dcc_release_datapackage.release == id)
        path = path.link(self._builder.CFDE.datapackage)
        return rel, {
            row['submitting_dcc']: row
            for row in path.entities().fetch()
        }

    def register_datapackage(self, id, dcc_id, submitting_user, archive_url):
        """Idempotently register new submission in registry.

        :param id: The datapackage.id for the new record
        :param dcc_id: The datapackage.submitting_dcc for the new record
        :param submitting_user: The datapackage.submitting_user for the new record
        :param archive_url: The datapackage.datapackage_url for the new record

        May raise non-CfdeError exceptions on operational errors.
        """
        try:
            return self.get_datapackage(id)
        except exception.DatapackageUnknown:
            pass

        # poke the submitting user into the registry's user-tracking table in case they don't exist
        # this acts as controlled domain table for submitting_user fkeys
        self._catalog.post('/entity/public:ERMrest_Client?onconflict=skip', json=[{
            'ID': submitting_user.webauthn_id,
            'Display_Name': submitting_user.display_name,
            'Full_Name': submitting_user.full_name,
            'Email': submitting_user.email,
            'Client_Object': {
                'id': submitting_user.webauthn_id,
                'display_name': submitting_user.display_name,
            }
        }])

        newrow = {
            "id": id,
            "submitting_dcc": dcc_id,
            "submitting_user": submitting_user.webauthn_id,
            "datapackage_url": archive_url,
            # we need to supply these unless catalog starts giving default values for us
            "submission_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "status": terms.cfde_registry_dp_status.submitted,
        }
        defaults = [
            cname
            for cname in self._builder.CFDE.datapackage.column_definitions.keys()
            if cname not in newrow
        ]
        self._catalog.post(
            '/entity/CFDE:datapackage?defaults=%s' % (','.join(defaults),),
            json=[newrow]
        )
        # kind of redundant, but make sure we round-trip this w/ server-applied defaults?
        return self.get_datapackage(id)

    def register_datapackage_table(self, datapackage, position, table_name):
        """Idempotently register new datapackage table in registry.

        :param datapackage: The datapackage.id for the containing datapackage
        :param position: The integer position of this table in the datapackage's list of resources
        :param table_name: The "name" field of the tabular resource

        """
        newrow = {
            'datapackage': datapackage,
            'position': position,
            'table_name': table_name,
            'status': terms.cfde_registry_dpt_status.enumerated,
            'num_rows': None,
            'diagnostics': None,
        }

        rows = self._catalog.post(
            '/entity/CFDE:datapackage_table?onconflict=skip',
            json=[newrow]
        ).json()

        if len(rows) == 0:
            # row exits
            self.update_datapackage_table(datapackage, position, status=terms.cfde_registry_dpt_status.enumerated)

    def update_release(self, id, status=nochange, description=nochange, cfde_approval_status=nochange, release_time=nochange, ermrest_url=nochange, browse_url=nochange, summary_url=nochange, diagnostics=nochange):
        """Idempotently update release metadata in registry.

        :param id: The release.id of the existing record to update
        :param status: The new release.status value (default nochange)
        :param description: The new release.description value (default nochange)
        :param cfde_approval_status: The new release.cfde_approval_status value (default nochange)
        :param release_time: The new release.release_time value (default nochange)
        :param ermrest_url: The new release.review_ermrest_url value (default nochange)
        :param browse_url: The new release.review_browse_url value (default nochange)
        :param summary_url: The new release.review_summary_url value (default nochange)
        :param diagnostics: The new release.diagnostics value (default nochange)

        The special `nochange` singleton value used as default for
        optional arguments represents the desire to keep whatever
        current value exists for that field in the registry.

        May raise non-CfdeError exceptions on operational errors.
        """
        if not isinstance(id, str):
            raise TypeError('expected id of type str, not %s' % (type(id),))
        existing, existing_dcc_dps = self.get_release(id)
        changes = {
            k: v
            for k, v in {
                    'status': status,
                    'description': description,
                    'cfde_approval_status': cfde_approval_status,
                    'release_time': release_time,
                    'ermrest_url': ermrest_url,
                    'browse_url': browse_url,
                    'summary_url': summary_url,
                    'diagnostics': diagnostics,
            }.items()
            if v is not nochange and v != existing[k]
        }
        if not changes:
            return
        changes['id'] = id
        self._catalog.put(
            '/attributegroup/CFDE:release/id;%s' % (','.join([ c for c in changes.keys() if c != 'id']),),
            json=[changes]
        )

    def update_datapackage(self, id, status=nochange, diagnostics=nochange, review_ermrest_url=nochange, review_browse_url=nochange, review_summary_url=nochange):
        """Idempotently update datapackage metadata in registry.

        :param id: The datapackage.id of the existing record to update
        :param status: The new datapackage.status value (default nochange)
        :param diagnostics: The new datapackage.diagnostics value (default nochange)
        :param review_ermrest_url: The new datapackage.review_ermrest_url value (default nochange)
        :param review_browse_url: The new datapackage.review_browse_url value (default nochange)
        :param review_summary_url: The new datapackage.review_summary_url value (default nochange)

        The special `nochange` singleton value used as default for
        optional arguments represents the desire to keep whatever
        current value exists for that field in the registry.

        May raise non-CfdeError exceptions on operational errors.
        """
        if not isinstance(id, str):
            raise TypeError('expected id of type str, not %s' % (type(id),))
        existing = self.get_datapackage(id)
        changes = {
            k: v
            for k, v in {
                    'status': status,
                    'diagnostics': diagnostics,
                    'review_ermrest_url': review_ermrest_url,
                    'review_browse_url': review_browse_url,
                    'review_summary_url': review_summary_url,
            }.items()
            if v is not nochange and v != existing[k]
        }
        if not changes:
            return
        changes['id'] = id
        self._catalog.put(
            '/attributegroup/CFDE:datapackage/id;%s' % (','.join([ c for c in changes.keys() if c != 'id']),),
            json=[changes]
        )

    def update_datapackage_table(self, datapackage, position, status=nochange, num_rows=nochange, diagnostics=nochange):
        """Idempotently update datapackage_table metadata in registry.

        :param datapackage: The datapackage_table.datapackage key value
        :param position: The datapackage_table.position key value
        :param status: The new datapackage_table.status value (default nochange)
        :param num_rows: The new datapackage_table.num_rows value (default nochange)
        :Param diagnostics: The new datapackage_table.diagnostics value (default nochange)

        """
        if not isinstance(datapackage, str):
            raise TypeError('expected datapackage of type str, not %s' % (type(datapackage),))
        if not isinstance(position, int):
            raise TypeError('expected id of type int, not %s' % (type(position),))
        existing = self.get_datapackage_table(datapackage, position)
        changes = {
            k: v
            for k, v in {
                    'status': status,
                    'num_rows': num_rows,
                    'diagnostics': diagnostics,
            }.items()
            if v is not nochange and v != existing[k]
        }
        if not changes:
            return
        changes.update({
            'datapackage': datapackage,
            'position': position,
        })
        self._catalog.put(
            '/attributegroup/CFDE:datapackage_table/datapackage,position;%s' % (
                ','.join([ c for c in changes.keys() if c not in {'datapackage', 'position'} ]),
            ),
            json=[changes]
        )

    def get_dcc(self, dcc_id=None):
        """Get one or all DCC records from the registry.

        :param dcc_id: Optional dcc.id key string to limit results to single DCC (default None)

        Returns a list of dict-like records representing rows of the
        registry dcc table, optionally restricted to a specific dcc.id
        key.
        """
        return self._get_entity('dcc', dcc_id)

    def get_group(self, group_id=None):
        """Get one or all group records from the registry.

        :param group_id: Optional group.id key string to limit results to single group (default None)

        Returns a list of dict-like records representing rows of the
        registry group table, optionally restricted to a specific group.id
        key.
        """
        return self._get_entity('group', group_id)

    def get_group_role(self, role_id=None):
        """Get one or all group-role records from the registry.

        :param role_id: Optional group_role.id key string to limit results to single role (default None)

        Returns a list of dict-like records representing rows of the
        registry group_role table, optionally restricted to a specific
        group_role.id key.
        """
        return self._get_entity('group_role', role_id)

    def get_groups_by_dcc_role(self, role_id=None, dcc_id=None):
        """Get groups by DCC x role for one or all roles and DCCs.

        :param role_id: Optional role.id key string to limit results to a single group role (default None)
        :param dcc_id: Optional dcc.id key string to limit results to a single DCC (default None)

        Returns a list of dict-like records associating a DCC id, a
        role ID, and a list of group IDs suitable as an ACL for that
        particular dcc-role combination.
        """
        # find range of possible values
        dccs = {
            row['id']: row
            for row in self.get_dcc(dcc_id)
        }
        roles = {
            row['id']: row
            for row in self.get_group_role(role_id)
        }

        # find mapped groups (an inner join)
        path = self._builder.CFDE.dcc_group_role.path.link(self._builder.CFDE.group)
        if role_id is not None:
            path = path.filter(path.dcc_group_role.role == role_id)
        if dcc_id is not None:
            path = path.filter(path.dcc_group_role.dcc == dcc_id)
        dcc_roles = {
            (row['dcc'], row['role']): row
            for row in path.groupby(path.dcc_group_role.dcc, path.dcc_group_role.role) \
            .attributes(ArrayD(path.group).alias("groups")) \
            .fetch()
        }

        # as a convenience for simple consumers, emulate a full outer
        # join pattern to return empty lists for missing combinations
        return [
            (
                dcc_roles[(dcc_id, role_id)] \
                if (dcc_id, role_id) in dcc_roles \
                else {"dcc": dcc_id, "role": role_id, "groups": []}
            )
            for dcc_id in dccs
            for role_id in roles
        ]

    def get_dcc_acl(self, dcc_id, role_id):
        """Get groups for one DCC X group_role as a webauthn-style ACL.

        :param dcc_id: A dcc.id key known by the registry.
        :param role_id: A group_role.id key known by the registry.

        Returns a list of webauthn ID strings as an access control
        list suitable for intersection tests with
        WebauthnUser.acl_authz_test().
        """
        acl = set()
        roles_sufficient = {
            terms.cfde_registry_grp_role.submitter: {
                terms.cfde_registry_grp_role.submitter,
                terms.cfde_registry_grp_role.admin,
            },
            terms.cfde_registry_grp_role.reviewer: {
                terms.cfde_registry_grp_role.submitter,
                terms.cfde_registry_grp_role.reviewer,
                terms.cfde_registry_grp_role.review_decider,
                terms.cfde_registry_grp_role.admin,
            },
            terms.cfde_registry_grp_role.review_decider: {
                terms.cfde_registry_grp_role.review_decider,
                terms.cfde_registry_grp_role.admin,
            },
            terms.cfde_registry_grp_role.admin: {
                terms.cfde_registry_grp_role.admin,
            }
        }
        for sub_role_id in roles_sufficient.get(role_id, {role_id,}):
            for row in self.get_groups_by_dcc_role(sub_role_id, dcc_id):
                acl.update({ grp['webauthn_id'] for grp in row['groups'] })
        return list(sorted(acl))

    def enforce_dcc_submission(self, dcc_id, submitting_user):
        """Verify that submitting_user is authorized to submit datapackages for dcc_id.

        :param dcc_id: The dcc.id key of the DCC in the registry
        :param submitting_user: The WebauthnUser representation of the user context.

        Raises Forbidden if user does not have submitter role for DCC.
        """
        submitting_user.acl_authz_test(
            self.get_dcc_acl(dcc_id, terms.cfde_registry_grp_role.submitter),
            'Submission to DCC %s is forbidden' % (dcc_id,)
        )

    def custom_migration(self):
        """Perform custom schema/content migration tasks"""
        # idempotently remove approved-hold vocab term
        approved_hold = 'cfde_registry_decision:approved-hold'
        res = self._catalog.get('/entity/CFDE:approval_status/id=%s' % urlquote(approved_hold)).json()
        if res:
            # rewrite any references to approved-hold to approved so we can drop the term
            remap = [{"old": approved_hold, "new": terms.cfde_registry_decision.approved}]
            self._catalog.put(
                '/attributegroup/CFDE:datapackage/old:=dcc_approval_status;new:=dcc_approval_status',
                json=remap,
            ).json()
            self._catalog.put(
                '/attributegroup/CFDE:datapackage/old:=cfde_approval_status;new:=cfde_approval_status',
                json=remap,
            ).json()
            self._catalog.put(
                '/attributegroup/CFDE:release/old:=cfde_approval_status;new:=cfde_approval_status',
                json=remap,
            ).json()
            self._catalog.delete(
                '/entity/CFDE:approval_status/id=%s' % urlquote(approved_hold)
            )

    def fixup_url_hostnames(self, fqdn):
        """Rewrite URLs in registry if we've moved the FQDN of the deployment"""
        rewrites = []
        for row in self.list_datapackages():
            changed = False
            for cname in {'review_ermrest_url', 'review_browse_url', 'review_summary_url'}:
                url = row[cname]
                if url is None:
                    continue
                u = urllib3.util.parse_url(url)
                if u.host != fqdn or u.scheme != 'https':
                    row[cname] = u._replace(scheme='https', host=fqdn).url
                    logger.info('Updating %s: %s -> %s' % (cname, u.url, row[cname]))
                    changed = True
            if changed:
                rewrites.append(row)
        if rewrites:
            self._catalog.put(
                '/attributegroup/CFDE:datapackage/id;review_ermrest_url,review_browse_url,review_summary_url',
                json=rewrites
            ).json()

        rewrites = []
        for row in self.list_releases():
            changed = False
            for cname in {'ermrest_url', 'browse_url', 'summary_url'}:
                url = row[cname]
                if url is None:
                    continue
                u = urllib3.util.parse_url(url)
                if u.host != fqdn or u.scheme != 'https':
                    row[cname] = u._replace(scheme='https', host=fqdn).url
                    logger.info('Updating %s: %s -> %s' % (cname, u.url, row[cname]))
                    changed = True
            if changed:
                rewrites.append(row)
        if rewrites:
            self._catalog.put(
                '/attributegroup/CFDE:release/id;ermrest_url,browse_url,summary_url',
                json=rewrites
            ).json()

    @classmethod
    def dump_onboarding(self, registry_datapackage):
        """Dump onboarding info about DCCs in registry"""
        resources = [
            resource
            for resource in registry_datapackage.package_def['resources']
            if resource['name'] in {'dcc', 'group', 'dcc_group_role'}
        ]
        registry_datapackage.dump_data_files(resources=resources)

    def upload_resource_records(self, reg_model, vocab_tname, records=[]):
        """Upload resource record dicts to vocab table.

        :param reg_model: The deriva-py ermrest Model of this registry
        :param vocab_tname: The name of a C2M2 vocab table known by the registry
        :param records: A list of dict-like resource records

        Each record supports two fields:
        - id: required and CURI must be found in registry's CV table already
        - resource_markdown: markdown-formatted resource info string
        """
        batch_size = 500
        cfde_schema = reg_model.schemas['CFDE']
        try:
            vocab_table = cfde_schema.tables[vocab_tname]
        except KeyError as e:
            raise ValueError('Unsupported resource vocabulary table name %r' % (vocab_tname,))

        def existing_batches():
            after = ''
            while True:
                rows = self._catalog.get('/attribute/CFDE:%s/id,resource_markdown@sort(id)%s?limit=%d' % (
                    urlquote(vocab_tname),
                    after,
                    batch_size
                )).json()
                if rows:
                    after = '@after(%s)' % urlquote(rows[-1]['id'])
                    yield rows
                else:
                    return

        logger.info('Getting existing resource info for %r...' % (vocab_tname,))
        existing = {}
        for batch in existing_batches():
            existing.update({ row['id']: row for row in batch })

        def is_distinct(v1, v2):
            if v1 is None:
                return not (v2 is None)
            elif v2 is None:
                return True
            else:
                return v1 != v2

        def needs_update(id=None, resource_markdown=None):
            if id not in existing:
                raise ValueError('Cannot set resource info for unknown %r term %r' % (vocab_tname, id))
            return is_distinct(existing[id]['resource_markdown'], resource_markdown)

        need_update = [ record for record in records if needs_update(**record) ]
        logger.info('Found %d input records with new and different resource information' % (len(need_update),))

        if not need_update:
            logger.info('Skipping %r with no resource information needing update' % (vocab_tname,))
        while need_update:
            batch = need_update[0:batch_size]
            self._catalog.put(
                '/attributegroup/CFDE:%s/id;resource_markdown' % (urlquote(vocab_tname),),
                json=batch,
            ).json() # discard response data
            logger.info('Updated %d resources for %r' % (len(batch), vocab_tname,))
            need_update = need_update[batch_size:]
        logger.info('Updated resource information for %r' % (vocab_tname,))

    def upload_resource_files(self, filepaths):
        """Take input filepaths and upload as resource info for vocab terms.

        :param filepaths: List of one or more filepaths we can open.

        Each file path must have a basename matching a CV table name, after
        stripping format-specific suffix. Supported format(s):

        - `.json`: File is a JSON array of record objects

        Input is decoded to find a set of records to be passed to
        self.upload_resource_records.

        """
        reg_model = self._catalog.getCatalogModel()
        for fpath in filepaths:
            bname = os.path.basename(fpath)
            suffix = bname.split('.')[-1]
            vocab_tname = '.'.join(bname.split('.')[0:-1])
            with open(fpath, 'rb') as f:
                if suffix == 'json':
                    records = json.load(f)
                else:
                    raise ValueError('Unsupported resource filepath suffix %r' % (suffix,))
                self.upload_resource_records(reg_model, vocab_tname, records)

def main(subcommand, *extra_args):
    """Perform registry maintenance.

    :param subcommand: A named sub-command of this utility.
    :param catalog_id: The existing catalog ID, if any.

    Subcommands:

    - 'provision' [ catalog_id ]
       - Build a new registry catalog and report its ID
    - 'reprovision' [ catalog_id ]
       - Adjust existing registry model
    - 'reconfigure' [ catalog_id ]
       - Re-configure existing registry
    - 'delete' catalog_id
       - Delete an existing registry, i.e. a parallel test DB
    - 'creators-acl' [ catalog_id ]
       - Print ermrest creators ACL
    - 'dump-onboarding' [ catalog_id ]
       - Write out *.tsv files for onboarding info in registry
    - 'fixup-fqdn' [ catalog_id ]
       - Update URLs to match FQDN service host
    - 'upload-resources' vocabname.json...

    Set environment variables:
    - DERIVA_SERVERNAME to choose service host.

    """
    init_logging(logging.INFO)

    servername = os.getenv('DERIVA_SERVERNAME', 'app-dev.nih-cfde.org')
    credentials = get_credential(servername)
    session_config = DEFAULT_SESSION_CONFIG.copy()
    session_config["allow_retry_on_all_methods"] = True
    server = DerivaServer('https', servername, credentials, session_config=session_config)

    catalog_id = 'registry'
    if subcommand == 'delete' and not extra_args:
        raise ValueError('delete command requires explicit catalog_id argument')

    if subcommand in { 'provision', 'reconfigure', 'delete', 'reprovision', 'dump-onboarding', 'fixup-fqdn' }:
        if extra_args:
            catalog_id = extra_args[0]

    if catalog_id == '':
        catalog_id = None

    if subcommand == 'creators-acl':
        print(json.dumps(ermrest_creators_acl, indent=2))
        return 0
    if subcommand == 'provision':
        catalog_id = server.post(
            '/ermrest/catalog',
            json={
                "id": catalog_id,
                "owner":  [
                    authn_id.cfde_portal_admin,
                    authn_id.cfde_infrastructure_ops,
                ],
            }
        ).json()["id"]
        print('Created new catalog %r' % (catalog_id,))

    if subcommand in { 'provision', 'reconfigure', 'delete', 'reprovision', 'dump-onboarding', 'fixup-fqdn', 'upload-resources' }:
        catalog = server.connect_ermrest(catalog_id)
        print('Connected to catalog %r' % catalog.catalog_id)
    else:
        raise ValueError('unknown subcommand %r' % subcommand)

    dp = CfdeDataPackage(registry_schema_json, RegistryConfigurator(catalog))
    registry = Registry('https', servername, catalog.catalog_id, credentials)
    dp.set_catalog(catalog, registry)

    if subcommand == 'provision':
        dp.provision()
        dp.load_data_files()
        # reconnect registry after provisioning
        registry = Registry('https', servername, catalog.catalog_id, credentials)
        dp.set_catalog(catalog, registry)
        dp.apply_custom_config()
    elif subcommand == 'reprovision':
        dp.provision(alter=True)
        dp.set_catalog(catalog, registry) # to force model reload
        dp.load_data_files(onconflict='update')
        registry.custom_migration()
        dp.apply_custom_config()
    elif subcommand == 'reconfigure':
        dp.apply_custom_config()
    elif subcommand == 'delete':
        catalog.delete_ermrest_catalog(really=True)
        print('Deleted existing catalog %s' % catalog.catalog_id)
    elif subcommand == 'dump-onboarding':
        registry.dump_onboarding(dp)
    elif subcommand == 'fixup-fqdn':
        registry.fixup_url_hostnames(servername)
    elif subcommand == 'upload-resources':
        registry.upload_resource_files(extra_args)
    return 0

if __name__ == '__main__':
    exit(main(*sys.argv[1:]))
