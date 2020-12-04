
from deriva.core import ErmrestCatalog, get_credential
from deriva.core.ermrest_model import nochange
from deriva.core.datapath import ArrayD
from deriva.core.utils.core_utils import AttrDict

from . import exception

def _attrdict_from_strings(*strings):
    new = AttrDict()
    for prefix, term in [ s.replace('-', '_').split(':') for s in strings ]:
        if prefix not in new:
            new[prefix] = AttrDict()
        if term not in new[prefix]:
            new[prefix][term] = '%s:%s' % (prefix, term)
    return new

# structured access to controlled terms we will use in this code...
terms = _attrdict_from_strings(
    'cfde_registry_grp_role:admin',
    'cfde_registry_grp_role:submitter',
    'cfde_registry_grp_role:review-decider',
    'cfde_registry_grp_role:reviewer',
    'cfde_registry_dp_status:submitted',
    'cfde_registry_dp_status:ops-error',
    'cfde_registry_dp_status:bag-valid',
    'cfde_registry_dp_status:bag-error',
    'cfde_registry_dp_status:check-valid',
    'cfde_registry_dp_status:check-error',
    'cfde_registry_dp_status:content-ready',
    'cfde_registry_dp_status:content-error',
    'cfde_registry_dp_status:rejected',
    'cfde_registry_dp_status:release-pending',
    'cfde_registry_dp_status:obsoleted',
)

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
    def __init__(self, scheme='https', servername='app.nih-cfde.org', catalog='registry', credentials=None):
        """Bind to specified registry.

        Note: this binding operates as an authenticated client
        identity and may expose different capabilities depending on
        the client's role within the organization.
        """
        if credentials is None:
            credentials = get_credential(servername)
        self._catalog = ErmrestCatalog(scheme, servername, catalog)
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
        path = self._builder.CFDE.tables[table_name]
        if id is not None:
            path = path.filter(path.table_instances[table_name].id == id)
        return list( path.entities().fetch() )

    def get_datapackage(self, id):
        """Get datapackage by submission id or raise exception.
        
        :param id: The datapackage.id key for the submission in the registry

        Raises DatapackageUnknown if record is not found.
        """
        rows = self._get_entity('datapackage', id)
        if len(rows) < 1:
            raise exception.DatapackageUnknown('Datapackage "%s" not found in registry.' % (id,))
        return rows[0]

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

    def update_datapackage(self, id, status=nochange, diagnostics=nochange, review_ermrest_url=nochange, review_browse_url=nochange, review_symmary_url=nochange):
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
            }.items()
            if v is not nochange and v != existing[k]
        }
        if not changes:
            return
        changes['id'] = id
        self._catalog.put(
            '/attributegroup/CFDE:datapackage/id;%s' % (','.join(changes.keys()),),
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
        path = self._builder.CFDE.dcc_group_role.link(self._builder.CFDE.group)
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
                if (_id, role_id) in dcc_roles \
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
        return [
            grp['webauthn_id']
            for grp in self.get_groups_by_dcc_role(role_id, dcc_id)[0]['groups']
        ]

    def enforce_submission(self, dcc_id, submitting_user):
        """Verify that submitting_user is authorized to submit datapackages for dcc_id.

        :param dcc_id: The dcc.id key of the DCC in the registry
        :param submitting_user: The WebauthnUser representation of the user context.

        Raises Forbidden if user does not have submitter role for DCC.
        """
        submitting_user.acl_authz_test(
            self.get_dcc_acl(dcc_id, terms.cfde_registry_grp_role.submitter),
            'Submission to DCC %s is forbidden' % (dcc_id,)
        )
