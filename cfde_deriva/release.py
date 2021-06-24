
import os
import sys
import uuid
import logging
import json
import traceback

from deriva.core import DerivaServer, get_credential, DEFAULT_SESSION_CONFIG, init_logging

from . import exception
from .cfde_login import get_archive_headers_map
from .tableschema import ReleaseConfigurator
from .datapackage import CfdeDataPackage, portal_schema_json
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
        os.makedirs(os.path.dirname(self.sqlite_filename), exist_ok=True)

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
            self.registry.update_release(self.release_id, ermrest_url=catalog.get_server_uri())
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

    def build(self):
        """Idempotently run release-build processing lifecycle.

        """
        # general sequence (with many idempotent steps)
        rel, dcc_datapackages = self.registry.get_release(self.release_id)

        failed = True
        diagnostics = 'An unknown operational error has occurred.'
        failed_exc = None
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
            Submission.provision_sqlite(None, self.sqlite_filename)

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
                    self.sqlite_filename,
                    progress=progress.setdefault('sqlite_load', {}).setdefault(dprow['id'], {}),
                )
                self.dump_progress(progress)

            # do this once w/ all content now loaded in sqlite
            logger.info('Preparing derived data...')
            Submission.prepare_sqlite_derived_data(self.sqlite_filename, progress=progress.setdefault('etl', {}))
            logger.info('Uploading all release content...')
            self.dump_progress(progress)
            Submission.upload_sqlite_content(catalog, self.sqlite_filename, progress=progress.setdefault('upload', {}))
            logger.info('All release content successfully uploaded to %(ermrest_url)s' % rel)
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
                'Updating release %s status=%s diagnostics=%s...' % (
                    self.release_id,
                    status,
                    '(nochange)' if diagnostics is nochange else diagnostics
                )
            )
            self.registry.update_release(self.release_id, status=status, diagnostics=diagnostics)
            logger.debug('Release %s status successfully updated.' % (self.release_id,))

    def dump_progress(self, progress):
        with open(self.restart_marker_filename, 'w') as f:
            json.dump(progress, f, indent=2)
        logger.info("Dumped restart marker file %s" % self.restart_marker_filename)

    @property
    def sqlite_filename(self):
        """Return sqlite_filename scratch C2M2 DB target name for given release id.

        We use a deterministic mapping of release id to
        sqlite_filename so that we can do reentrant processing.

        """
        # TBD: check or remap id character range?
        # naive mapping should be OK for UUIDs...
        return '%s/databases/%s.sqlite3' % (self.content_path_root, self.release_id)

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
    - 'draft' [ release_id [ description ] ]
       - Register a new or revised draft release
    - 'provision' release_id
       - Create new, empty release catalog
    - 'build' release_id
       - Build content based on release definition
    - 'reconfigure' catalog_id
       - Revise policy/presentation config on existing catalog

    This client uses default DERIVA credentials for server.

    Set environment variables:

    - DERIVA_SERVERNAME to choose service host.
    - DRAFT_NEED_CFDE to 'false' to relax approval checks for release drafts
    - DRAFT_NEED_DCC to 'false' to relax approval checks for release drafts

    """
    init_logging(logging.INFO)

    servername = os.getenv('DERIVA_SERVERNAME', 'app-dev.nih-cfde.org')
    credential = get_credential(servername)
    session_config = DEFAULT_SESSION_CONFIG.copy()
    session_config["allow_retry_on_all_methods"] = True
    registry = Registry('https', servername, credentials=credential, session_config=session_config)
    server = DerivaServer('https', servername, credential, session_config=session_config)

    need_dcc_appr = os.getenv('DRAFT_NEED_DCC', 'true').lower() in {'t', 'y', 'true', 'yes'}
    need_cfde_appr = os.getenv('DRAFT_NEED_CFDE', 'true').lower() in {'t', 'y', 'true', 'yes'}

    archive_headers_map = get_archive_headers_map(servername)

    if subcommand == 'draft':
        if len(args) > 0:
            rel_id = args[0]
        else:
            rel_id = str(uuid.uuid4())
        if len(args) > 1:
            description = args[1]
        else:
            description = nochange
        dcc_datapackages = registry.get_latest_approved_datapackages(need_dcc_appr, need_cfde_appr)
        release, dcc_datapackages = registry.register_release(rel_id, dcc_datapackages, description)
        print(json.dumps(list(dcc_datapackages.values()), indent=4))
        print(json.dumps(release, indent=4))
    elif subcommand == 'draft-preview':
        res = registry.get_latest_approved_datapackages(need_dcc_appr, need_cfde_appr)
        print('Found %d elements for draft release' % len(res))
        print(json.dumps(list(res.values()), indent=4))
    elif subcommand == 'provision':
        if len(args) < 1:
            raise TypeError('"provision" requires one positional argument: release_id')
        rel_id = args[0]
        release = Release(server, registry, rel_id)
        rel = release.provision()
        print('Release %(id)s has catalog %(ermrest_url)s' % rel)
    elif subcommand == 'build':
        if len(args) < 1:
            raise TypeError('"build" requires one positional argument: release_id')

        rel_id = args[0]
        rel_row, dcc_datapackages = registry.get_release(rel_id)
        release = Release(server, registry, rel_id, dcc_datapackages=dcc_datapackages, archive_headers_map=archive_headers_map)
        rel = release.build()
        print('Release %(id)s has been built in %(ermrest_url)s' % rel)
    elif subcommand == 'reconfigure':
        if len(args) == 1:
            catalog_id = args[0]
            catalog = server.connect_ermrest(catalog_id)
        else:
            raise TypeError('"reconfigure" requires exactly one positional argument: catalog_id')

        Release.configure_release_catalog(registry, catalog, catalog_id)
    else:
        raise ValueError('unknown sub-command "%s"' % subcommand)

if __name__ == '__main__':
    exit(main(*sys.argv[1:]))

