
import os
import sys
import uuid
import logging
import json
import traceback
import requests
import datetime
import dateutil.parser

from deriva.core import DerivaServer, get_credential, init_logging, urlquote, urlunquote

from . import exception
from .cfde_login import get_archive_headers_map
from .tableschema import ReleaseConfigurator, authn_id
from .datapackage import CfdeDataPackage, submission_schema_json, portal_prep_schema_json, portal_schema_json, make_session_config
from .registry import Registry, nochange, terms
from .submission import Submission

logger = logging.getLogger(__name__)

class Release (object):
    """Processing support for CFDE C2M2 catalog releases

    This class collects some utility functions, but instances of the
    class represent a stateful processing lifecycle which is coupled
    with updates to the CFDE registry, causing side-effects in the
    registry as other processing methods are invoked.

    """

    # Allow monkey-patching or other caller-driven reconfig in future?
    content_path_root = '/var/tmp/cfde_deriva_submissions'
    next_rel_descr = 'future release candidates'

    @classmethod
    def by_id(cls, server, registry, release_id, archive_headers_map=None):
        """Construct an instance bound to an existing entry in the registry"""
        rel_row, dcc_datapackages = registry.get_release(release_id)
        release = cls(server, registry, rel_row['id'], dcc_datapackages=dcc_datapackages, archive_headers_map=archive_headers_map)
        return release

    def __init__(self, server, registry, id, dcc_datapackages=None, archive_headers_map=None):
        """Represent a stateful processing flow for a C2M2 release.

        :param server: A DerivaServer binding object where release catalogs are created.
        :param registry: A Registry binding object.
        :param id: The unique identifier for the release, i.e. UUID.
        :param dcc_datapackages: A map {dcc_id: dp_id, ...} or None (default) to get automatic draft config.
        :param archive_headers_map: A pass-through for Submission.__init__(...)

        The new instance is a binding for a release which may or may
        not yet exist in the registry. The constructor WILL NOT cause
        state changes to the registry.

        The archive_headers_map is a dict hierarchy to pass when
        constructing instances of the sibling Submission class.

        """
        self.server = server
        self.registry = registry
        self.release_id = id
        self.archive_headers_map = archive_headers_map

        if dcc_datapackages:
            self.dcc_datapackages = {}
            for dcc_id, dp in dcc_datapackages.items():
                if isinstance(dp, str):
                    dp = registry.get_datapackage(dp_id)
                elif isinstance(dp, dict):
                    pass
                else:
                    raise TypeError('Expected datapackage id (str) or registry row (dict) not %s' % (type(dp),))
                if dcc_id != dp['submitting_dcc']:
                    raise ValueError('Datapackage %s submitting DCC %s not valid for dcc %s' % (dp['id'], dp['submitting_dcc'], dcc_id))
                self.dcc_datapackages[dcc_id] = dp
        else:
            self.dcc_datapackages = registry.get_latest_approved_datapackages()

        # check filesystem config early to abort ASAP on errors
        # TBD: check permissions for safe service config?
        os.makedirs(os.path.dirname(self.ingest_sqlite_filename), exist_ok=True)
        os.makedirs(os.path.dirname(self.portal_prep_sqlite_filename), exist_ok=True)

    @classmethod
    def purge_multiple(cls, server, registry, purge_mode='auto'):
        """Purge multiple release catalogs, updating records appropriately

        :param purge_mode: Target selection mode string (default 'auto')

        Supported purge_mode values:
        - 'auto': heuristically select likely dead-end releases
        - 'ALL': purge ALL catalogs, regardless of release status

        """
        if purge_mode not in {'auto', 'ALL'}:
            raise ValueError('Invalid purge_mode %r' % (purge_mode,))

        in_past = True
        for rel_row in registry.list_releases(sortby="RCT"):
            purge_this = False

            if rel_row["status"] == terms.cfde_registry_rel_status.public_release:
                logger.debug("Found public-release %(id)s, dividing release history into past/present/future" % rel_row)
                in_past = False

            if purge_mode == 'ALL':
                logger.info("Purging release %(id)s in purge ALL mode" % rel_row)
                purge_this = True
            elif rel_row["status"] in {
                    terms.cfde_registry_rel_status.ops_error,
                    terms.cfde_registry_rel_status.obsoleted,
                    terms.cfde_registry_rel_status.rejected,
                    terms.cfde_registry_rel_status.content_error,
            }:
                logger.info("Purging release %(id)s with unconditional purge status %(status)s" % rel_row)
                purge_this = True
            elif in_past and rel_row["status"] in {
                    terms.cfde_registry_rel_status.planning,
                    terms.cfde_registry_rel_status.pending,
                    terms.cfde_registry_rel_status.content_ready,
            }:
                logger.info("Purging past release %(id)s with conditional purge status %(status)s" % rel_row)
                purge_this = True
            else:
                logger.info("Skipping release %(id)s with unmatched status %(status)s for purge." % rel_row)

            if purge_this:
                release = cls.by_id(server, registry, rel_row['id'])
                release.purge()

    @classmethod
    def configure_release_catalog(cls, registry, catalog, id, provision=False):
        """Configure release catalog

        Configure (or reconfigure) a release catalog.

        :param registry: The Registry instance for the submission system
        :param catalog: The ErmrestCatalog for the existing review catalog
        :param id: The submission id of the submission providing the review content
        :param provision: Perform model provisioning if True (default False, only reconfigure policies/presentation)

        """
        canon_dp = CfdeDataPackage(portal_schema_json, ReleaseConfigurator(catalog, registry))
        canon_dp.set_catalog(catalog, registry)
        if provision:
            canon_dp.provision() # get the model deployed
        # TBD: annotate with submission ID for easier ops/inventory purposes?
        canon_dp.apply_custom_config() # get the chaise hints deloyed
        return catalog

    def provision(self):
        rel, dcc_datapackages = self.registry.get_release(self.release_id)
        if rel['ermrest_url'] is not None:
            return rel

        try:
            dp = CfdeDataPackage(portal_schema_json, ReleaseConfigurator())
            catalog = self.server.create_ermrest_catalog()
            logger.info('New catalog has catalog_id=%s for release=%s' % (catalog.catalog_id, self.release_id))
            dp.set_catalog(catalog, self.registry)
            dp.provision()
            dp.apply_custom_config()
            logger.debug('New, empty catalog provisioned')
            self.registry.update_release(
                self.release_id,
                ermrest_url=catalog.get_server_uri(),
            )
            logger.debug('New catalog registered for release')
            return self.registry.get_release(self.release_id)[0]
        except Exception as e:
            et, ev, tb = sys.exc_info()
            logger.debug(''.join(traceback.format_exception(et, ev, tb)))
            logger.error(
                'Failed with exception %s in provisioning for release %s' % (e, self.release_id)
            )
            self.registry.update_release(
                self.release_id,
                status=terms.cfde_registry_rel_status.ops_error,
                diagnostics=str(e),
            )
            raise

    @property
    def rel_row(self):
        rel_row, dcc_datapackages = self.registry.get_release(self.release_id)
        return rel_row

    @property
    def catalog_id(self):
        rel_row = self.rel_row
        if rel_row['ermrest_url'] is None:
            raise exception.StateError('Catalog %(id)s ermrest_url=%(ermrest_url)r does not have a catalog_id' % rel_row)
        return urlunquote(rel_row['ermrest_url'].split('/')[-1])

    def purge(self):
        """Purge release catalog state from service, updating release record appropriately"""
        rel_row = self.rel_row
        status = rel_row["status"]

        try:
            catalog_id = self.catalog_id

            new_state = {
                terms.cfde_registry_rel_status.planning: terms.cfde_registry_rel_status.rejected,
                terms.cfde_registry_rel_status.pending: terms.cfde_registry_rel_status.rejected,
                terms.cfde_registry_rel_status.content_ready: terms.cfde_registry_rel_status.rejected,
                terms.cfde_registry_rel_status.content_error: terms.cfde_registry_rel_status.content_error,
                terms.cfde_registry_rel_status.rejected: terms.cfde_registry_rel_status.rejected,
                terms.cfde_registry_rel_status.public_release: terms.cfde_registry_rel_status.obsoleted,
                terms.cfde_registry_rel_status.obsoleted: terms.cfde_registry_rel_status.obsoleted,
                terms.cfde_registry_rel_status.ops_error: terms.cfde_registry_rel_status.ops_error,
            }.get(status, terms.cfde_registry_rel_status.ops_error)

            if status != new_state:
                logger.debug('Changing release %r status %s -> %s' % (self.release_id, status, new_state))
                self.registry.update_release(self.release_id, status=new_state)

            logger.debug('Deleting ermrest catalog %r' % (catalog_id,))
            self.server.delete('/ermrest/catalog/%s' % urlquote(catalog_id))
            self.registry.update_release(self.release_id, ermrest_url=None, browse_url=None, summary_url=None)
            logger.info('Purged catalog %r for release %r.' % (catalog_id, self.release_id))
            return True
        except exception.StateError:
            logger.info('Release %r already purged.' % self.release_id)
            return False

    def publish(self, publish_id='1'):
        """Adjust ermrest catalog alias to publish this release"""
        rel_row = self.rel_row

        if rel_row.get('status') not in {
                terms.cfde_registry_rel_status.content_ready,
                terms.cfde_registry_rel_status.obsoleted, # allow rollback to prior release
                terms.cfde_registry_rel_status.public_release, # eventual consistency/idempotence
        } or rel_row['ermrest_url'] is None:
            raise exception.StateError('Release %(id)s cannot be published with status=%(status)r ermrest_url=%(ermrest_url)r' % rel_row)

        res = self.server.get('/ermrest/')
        if not res.json().get('features', {}).get('catalog_alias', False):
            raise exception.StateError('The server does not support catalog aliasing features!')

        prev_release = None
        try:
            # find previous release currently bound to publish_id
            res = self.server.get('/ermrest/alias/%s' % urlquote(publish_id))
            aliasdoc = res.json()
            if aliasdoc.get('alias_target') is not None:
                # HACK: search release by pattern-match on ermrest_url
                pattern = '/%s$' % urlquote(aliasdoc['alias_target'])
                rows = self.registry._catalog.get('/entity/CFDE:release/ermrest_url::regexp::%s' % (urlquote(pattern),)).json()
                if len(rows) > 1:
                    raise NotImplementedError('Found more than one release matching ermrest_url ::regexp:: %r:\n%r' % (pattern, rows))
                logger.info(rows)
                if rows:
                    prev_release = Release.by_id(self.server, self.registry, rows[0]['id'])
                    if prev_release.release_id == self.release_id:
                        prev_release = None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                # maybe we are creating a new publish_id?
                pass
            else:
                raise

        res = self.server.put(
            '/ermrest/alias/%s' % urlquote(publish_id),
            json={
                "id": publish_id,
                "owner": [
                    authn_id.cfde_portal_admin,
                    authn_id.cfde_infrastructure_ops,
                ],
                "alias_target": self.catalog_id
            }
        )
        res.raise_for_status()
        aliasdoc = res.json()
        self.registry.update_release(
            self.release_id,
            status=terms.cfde_registry_rel_status.public_release,
            cfde_approval_status=terms.cfde_registry_decision.approved,
            release_time=datetime.datetime.utcnow().isoformat() if rel_row['release_time'] is None else nochange,
            description=('%s public release' % datetime.date.today().isoformat()) if rel_row['description'] == self.next_rel_descr else nochange,
        )

        if prev_release is not None:
            self.registry.update_release(prev_release.release_id, status=terms.cfde_registry_rel_status.obsoleted)

        return aliasdoc

    def build(self):
        """Idempotently run release-build processing lifecycle.

        """
        # general sequence (with many idempotent steps)
        rel, dcc_datapackages = self.registry.get_release(self.release_id)

        failed = True
        diagnostics = 'An unknown operational error has occurred.'
        failed_exc = None
        browse_url = nochange
        summary_url = nochange
        # streamline handling of error reporting...
        # next_error_state anticipates how to categorize exceptions
        # based on what we are trying to do during sequence
        next_error_state = terms.cfde_registry_rel_status.ops_error

        try:
            os.makedirs(os.path.dirname(self.restart_marker_filename), exist_ok=True)
            with open(self.restart_marker_filename, 'r') as f:
                progress = json.load(f)
            logger.info("Loaded restart marker file %s" % self.restart_marker_filename)
        except:
            progress = dict()

        try:
            # shortcut if already in terminal state
            if rel['status'] in {
                    terms.cfde_registry_rel_status.content_ready,
                    terms.cfde_registry_rel_status.content_error,
                    terms.cfde_registry_rel_status.rejected,
                    terms.cfde_registry_rel_status.public_release,
                    terms.cfde_registry_rel_status.obsoleted,
            }:
                logger.info('Skipping ingest for release %s with existing terminal status %s.' % (
                    self.release_id,
                    rel['status'],
                ))
                failed = False
                return rel

            logger.info('Provisioning catalog...')
            rel = self.provision()
            catalog = self.server.connect_ermrest(
                Submission.extract_catalog_id(self.server, rel['ermrest_url'])
            )

            # this includes portal schema and built-in vocabs
            logger.info('Provisioning sqlite...')
            Submission.provision_sqlite(submission_schema_json, self.ingest_sqlite_filename)
            Submission.provision_sqlite(portal_prep_schema_json, self.portal_prep_sqlite_filename)

            logger.info('Loading %s release constituents...' % len(self.dcc_datapackages))
            for dprow in self.dcc_datapackages.values():
                submission = Submission(
                    self.server,
                    self.registry,
                    dprow['id'],
                    dprow['submitting_dcc'],
                    dprow['datapackage_url'],
                    None,
                    self.archive_headers_map,
                    skip_dcc_check=True,
                )
                submission.retrieve_datapackage(
                    submission.archive_url,
                    submission.download_filename,
                    submission.archive_headers_map
                )
                submission.unpack_datapackage(
                    submission.download_filename,
                    submission.content_path
                )
                # truncate validation, since we did that during submission
                # re-check for storage corruption?
                submission.bdbag_validate(submission.content_path)
                # re-check if portal model has changed since submission was checked?
                submission.datapackage_model_check(submission.content_path)

                submission.load_sqlite(
                    submission.content_path,
                    self.ingest_sqlite_filename,
                    progress=progress.setdefault('sqlite_load', {}).setdefault(dprow['id'], {}),
                )
                # this works because we incrementally translate each DCC's primary_dcc_contact rows
                submission.transitional_etl_dcc_table(submission.content_path, self.ingest_sqlite_filename, submission.submitting_dcc_id)
                self.dump_progress(progress)

            # do this once w/ all content now loaded in sqlite
            logger.info('Preparing derived data...')
            Submission.prepare_sqlite_derived_data(
                self.portal_prep_sqlite_filename,
                progress=progress.setdefault('etl', {}),
                attach={'submission': self.ingest_sqlite_filename},
            )
            logger.info('Uploading all release content...')
            self.dump_progress(progress)
            Submission.upload_sqlite_content(catalog, self.portal_prep_sqlite_filename, progress=progress.setdefault('upload', {}))
            logger.info('All release content successfully uploaded to %(ermrest_url)s' % rel)
            browse_url = '/chaise/recordset/#%s/CFDE:file' % catalog.catalog_id
            summary_url = '/pdashboard.html?catalogId=%s' % catalog.catalog_id
            self.dump_progress(progress)

            failed = False

            return rel
        except Exception as e:
            failed, failed_exc = True, e
            raise
        finally:
            self.dump_progress(progress)
            if failed:
                status, diagnostics = next_error_state, diagnostics
                if failed_exc is not None:
                    et, ev, tb = sys.exc_info()
                    for line in traceback.format_exception(et, ev, tb):
                        logger.debug(line)
                else:
                    diagnostics = 'Processing interrupted?'
                logger.error(
                    'Failed with exception %s in release build sequence with next_error_state=%s for release %s' \
                    % (failed_exc, next_error_state, self.release_id)
                )
            else:
                status, diagnostics = terms.cfde_registry_rel_status.content_ready, None
                logger.debug(
                    'Finished release build sequence for release %s' % (self.release_id,)
                )
            logger.debug(
                'Updating release %s status=%s diagnostics=%s browse_url=%s summary_url=%s...' % (
                    self.release_id,
                    status,
                    '(nochange)' if diagnostics is nochange else diagnostics,
                    '(nochange)' if browse_url is nochange else browse_url,
                    '(nochange)' if summary_url is nochange else summary_url,
                )
            )
            self.registry.update_release(
                self.release_id,
                status=status,
                diagnostics=diagnostics,
                browse_url=browse_url,
                summary_url=summary_url,
            )
            logger.debug('Release %s status successfully updated.' % (self.release_id,))

    def dump_progress(self, progress):
        with open(self.restart_marker_filename, 'w') as f:
            json.dump(progress, f, indent=2)
        logger.info("Dumped restart marker file %s" % self.restart_marker_filename)

    @property
    def ingest_sqlite_filename(self):
        """Return ingest_sqlite_filename scratch C2M2 DB target name for given release id.

        We use a deterministic mapping of release id to
        ingest_sqlite_filename so that we can do reentrant processing.

        """
        # TBD: check or remap id character range?
        # naive mapping should be OK for UUIDs...
        return '%s/databases/%s_submission.sqlite3' % (self.content_path_root, self.release_id)

    @property
    def portal_prep_sqlite_filename(self):
        """Return portal_prep_sqlite_filename scratch C2M2 DB target name for given release id.

        We use a deterministic mapping of release id to
        portal_prep_sqlite_filename so that we can do reentrant processing.

        """
        # TBD: check or remap id character range?
        # naive mapping should be OK for UUIDs...
        return '%s/databases/%s_portal_prep.sqlite3' % (self.content_path_root, self.release_id)

    @property
    def restart_marker_filename(self):
        """Return restart_marker JSON file name for given release id.
        """
        return '%s/progress/%s.json' % (self.content_path_root, self.release_id)


def main(subcommand, *args):
    """Ugly test-harness for data release.

    Usage: python3 -m cfde_deriva.release <sub-command> ...

    Sub-commands:
    - 'draft-preview'
       - List current latest content w/o modifying release definitions
    - 'draft'
       - Register or update a canonical next-release draft
       - description will be filled automatically as special marker
    - 'draft' 'new' [ description ]
       - Register a new draft release (not the canonical next-release)
    - 'draft' release_id [ description ] ]
       - Update an existing draft release known by release_id (UUID)
    - 'provision' release_id
       - Create new, empty release catalog
    - 'build' release_id
       - Build content based on release definition
    - 'reconfigure' release_id
       - Revise policy/presentation config on existing catalog
    - 'publish' release_id
       - Adjust the /ermrest/catalog/1 alias to point to the identified release's catalog
    - 'purge' release_id
       - Purge release's backing ermrest catalog storage and update release record status
    - 'purge-auto'
       - Purge redundant releases by timeline-aware heuristics
    - 'purge-ALL'
       - Purge ALL releases including current public one
       - This is a disruptive administrative tool to be used with offline sytems!

    This client uses default DERIVA credentials for server.

    Set environment variables:

    - DERIVA_SERVERNAME to choose service host.
    - DRAFT_NEED_CFDE to 'false' to relax approval checks for release drafts
    - DRAFT_NEED_DCC to 'false' to relax approval checks for release drafts

    """
    init_logging(logging.INFO)

    servername = os.getenv('DERIVA_SERVERNAME', 'app-dev.nih-cfde.org')
    credential = get_credential(servername)
    session_config = make_session_config()
    registry = Registry('https', servername, credentials=credential, session_config=session_config)
    server = DerivaServer('https', servername, credential, session_config=session_config)

    need_dcc_appr = os.getenv('DRAFT_NEED_DCC', 'true').lower() in {'t', 'y', 'true', 'yes'}
    need_cfde_appr = os.getenv('DRAFT_NEED_CFDE', 'false').lower() in {'t', 'y', 'true', 'yes'}

    archive_headers_map = get_archive_headers_map(servername)

    if subcommand == 'draft':
        if len(args) == 0:
            # next-release mode
            # HACK: find/label next-release by specific description field
            res = registry._catalog.get('/entity/CFDE:release/status=%s/description=%s@sort(RCT,RID)' % (
                urlquote(terms.cfde_registry_rel_status.planning),
                urlquote(Release.next_rel_descr),
            )).json()
            if len(res) > 0:
                rel_id = res[0]['id']
            else:
                rel_id = str(uuid.uuid4())
            description = Release.next_rel_descr
        else:
            if args[0] == 'new':
                rel_id = str(uuid.uuid4())
            else:
                rel_id = args[0]
            if len(args) > 1:
                description = args[1]
            else:
                description = nochange
        dcc_datapackages = registry.get_latest_approved_datapackages(need_dcc_appr, need_cfde_appr)
        release, dcc_datapackages = registry.register_release(rel_id, dcc_datapackages, description)
        # also update submission states
        datapackages = { dp_row['id'] for dp_row in dcc_datapackages.values() }
        for dp_row in registry.list_datapackages(sortby='submission_time'):
            next_state = dp_row['status']
            if dp_row['id'] in datapackages:
                if dp_row['status'] != terms.cfde_registry_dp_status.release_pending:
                    next_state = terms.cfde_registry_dp_status.release_pending
            elif dp_row['status'] in {
                    terms.cfde_registry_dp_status.release_pending,
                    terms.cfde_registry_dp_status.content_ready,
            }:
                next_state = terms.cfde_registry_dp_status.content_ready
                dcc_latest = dcc_datapackages.get(dp_row['submitting_dcc'])
                if dcc_latest is not None:
                    this_date = dateutil.parser.parse(dp_row['submission_time'])
                    latest_date = dateutil.parser.parse(dcc_latest['submission_time'])
                    if this_date < latest_date:
                        next_state = terms.cfde_registry_dp_status.obsoleted

            if next_state != dp_row['status']:
                logger.info('Updating datapackage %r status %s -> %s' % (dp_row['id'], dp_row['status'], next_state))
                registry.update_datapackage(dp_row['id'], status=next_state)

        print(json.dumps(list(dcc_datapackages.values()), indent=4))
        print(json.dumps(release, indent=4))
    elif subcommand == 'draft-preview':
        res = registry.get_latest_approved_datapackages(need_dcc_appr, need_cfde_appr)
        print('Found %d elements for draft release' % len(res))
        print(json.dumps(list(res.values()), indent=4))
    elif subcommand in  {'provision', 'build', 'reconfigure', 'publish', 'purge'}:
        if len(args) < 1:
            raise TypeError('%r requires one positional argument: release_id' % (subcommand,))

        rel_id = args[0]
        release = Release.by_id(server, registry, rel_id, archive_headers_map=archive_headers_map)

        if subcommand == 'provision':
            rel_row = release.provision()
            print('Release %(id)s has catalog %(ermrest_url)r' % rel_row)
        elif subcommand == 'build':
            rel_row = release.build()
            print('Release %(id)s has been built in %(ermrest_url)s' % rel_row)
        elif subcommand == 'reconfigure':
            catalog_id = release.catalog_id
            catalog = server.connect_ermrest(catalog_id)
            reprovision = os.getenv('REPROVISION_MODEL', 'false').lower() in {'t', 'y', 'true', 'yes'}
            Release.configure_release_catalog(registry, catalog, catalog_id, provision=reprovision)
        elif subcommand == 'publish':
            aliasdoc = release.publish()
            print("Publishing alias %(id)r now bound to target %(alias_target)r" % aliasdoc)
        elif subcommand == 'purge':
            release.purge()
        else:
            assert(False)
    elif subcommand in {'purge-ALL', 'purge-auto'}:
        Release.purge_multiple(server, registry, purge_mode={'purge-ALL': 'ALL', 'purge-auto': 'auto'}[subcommand])
    else:
        raise ValueError('unknown sub-command "%s"' % subcommand)

if __name__ == '__main__':
    exit(main(*sys.argv[1:]))

