
import sys
import datetime
import json
import logging

from deriva.core import DerivaServer, ErmrestCatalog, get_credential, DEFAULT_SESSION_CONFIG, init_logging, urlquote
from deriva.core.ermrest_model import nochange
from deriva.core.datapath import ArrayD
from deriva.core.utils.core_utils import AttrDict

from . import exception
from .tableschema import RegistryConfigurator, authn_id, terms
from .datapackage import CfdeDataPackage, registry_schema_json

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

    def _get_entity(self, table_name, id=None):
        """Get one or all entity records from a registry table.

        :param table_name: The registry table to access.
        :param id: A key to retrieve one row (default None retrieves all)
        """
        path = self._builder.CFDE.tables[table_name].path
        if id is not None:
            path = path.filter(path.table_instances[table_name].column_definitions['id'] == id)
        return list( path.entities().fetch() )

    def list_datapackages(self):
        """Get a list of all datapackage submissions in the registry

        """
        return self._get_entity('datapackage')

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
            "submission_time": datetime.datetime.utcnow().isoformat(),
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
        for row in self.get_groups_by_dcc_role(role_id, dcc_id):
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

    @classmethod
    def dump_onboarding(self, registry_datapackage):
        """Dump onboarding info about DCCs in registry"""
        resources = [
            resource
            for resource in registry_datapackage.package_def['resources']
            if resource['name'] in {'dcc', 'group', 'dcc_group_role'}
        ]
        registry_datapackage.dump_data_files(resources=resources)

def main(servername, subcommand, catalog_id=None):
    """Perform registry maintenance.

    :param servername: The DERIVA server where the registry should reside.
    :param subcommand: A named sub-command of this utility.
    :param catalog_id: The existing catalog ID, if any.

    Subcommands:

    - 'provision': Build a new registry catalog and report its ID (ignores catalog_id)
    - 'reprovision': Adjust existing registry model (required catalog_id)
    - 'reconfigure': Re-configure existing registry (requires catalog_id)
    - 'delete': Delete an existing (test) registry
    - 'creators-acl': Print ermrest creators ACL
    - 'dump-onboarding': Write out *.tsv files for onboarding info in registry

    """
    init_logging(logging.INFO)

    credentials = get_credential(servername)
    session_config = DEFAULT_SESSION_CONFIG.copy()
    session_config["allow_retry_on_all_methods"] = True
    server = DerivaServer('https', servername, credentials, session_config=session_config)

    if subcommand == 'creators-acl':
        print(json.dumps(ermrest_creators_acl, indent=2))
        return 0
    if subcommand == 'provision':
        catalog = server.create_ermrest_catalog()
        print('Created new catalog %s' % catalog.catalog_id)
    elif subcommand in { 'reconfigure', 'delete', 'reprovision', 'dump-onboarding' }:
        if catalog_id is None:
            raise TypeError('missing 1 required positional argument: catalog_id')
        catalog = server.connect_ermrest(catalog_id)
        print('Connected to existing catalog %s' % catalog.catalog_id)
    else:
        raise ValueError('unknown subcommand %s' % subcommand)

    dp = CfdeDataPackage(registry_schema_json, RegistryConfigurator(catalog))
    registry = Registry('https', servername, catalog.catalog_id, credentials)
    dp.set_catalog(catalog, registry)

    if subcommand == 'provision':
        # HACK: need to pre-populate ERMrest client w/ identities used in test data for submitting_user
        catalog.post(
            '/entity/public:ERMrest_Client?onconflict=skip',
            json=[
                {
                    'ID': 'https://auth.globus.org/ad02dee8-d274-11e5-b4a0-8752ee3cf7eb',
                    'Display_Name': 'karlcz@globusid.org',
                    'Full_Name': 'Karl Czajkowski',
                    'Email': 'karlcz@isi.edu',
                    'Client_Object': {},
                }
            ]
        ).raise_for_status()
        dp.provision()
        dp.load_data_files()
        # reconnect registry after provisioning
        registry = Registry('https', servername, catalog.catalog_id, credentials)
        dp.set_catalog(catalog, registry)
        dp.apply_custom_config()
    elif subcommand == 'reprovision':
        dp.provision(alter=True)
        dp.load_data_files(onconflict='update')
        dp.apply_custom_config()
    elif subcommand == 'reconfigure':
        dp.apply_custom_config()
    elif subcommand == 'delete':
        catalog.delete_ermrest_catalog(really=True)
        print('Deleted existing catalog %s' % catalog.catalog_id)
    elif subcommand == 'dump-onboarding':
        registry.dump_onboarding(dp)

    return 0

if __name__ == '__main__':
    exit(main(*sys.argv[1:]))
