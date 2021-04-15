
import os
import os.path
import sys
import traceback
import re
import shutil
import zipfile
import tarfile
import logging
import json
import csv
import uuid
import pkgutil
import tempfile
import sqlite3
import requests
from glob import glob
from bdbag import bdbag_api
from bdbag.bdbagit import BagError, BagValidationError
import frictionless
from deriva.core import DerivaServer, get_credential, DEFAULT_SESSION_CONFIG, init_logging, urlquote

from . import exception, tableschema
from .registry import Registry, WebauthnUser, WebauthnAttribute, nochange, terms
from .datapackage import CfdeDataPackage, portal_schema_json, sql_literal
from .cfde_login import get_archive_headers_map

logger = logging.getLogger(__name__)

class Submission (object):
    """Processing support for C2M2 datapackage submissions.

    This class collects some utility functions, but instances of the
    class represent a stateful processing lifecycle which is coupled
    with updates to the CFDE submission registry, causing side-effects
    in the registry as other processing methods are invoked.

    Typical call-sequence from automated ingest pipeline:

      from cfde_deriva.submission import Submission, Registry, \
        ErmrestUser, ErmrestAttributes

      registry = Registry('https', 'server.nih-cfde.org')

      # gather request context
      dcc_id = ...
      user = WebauthnUser.from_globus(
        globus_user_uuid,
        display_name,
        full_name,
        email,
        [
           WebauthnAttribute.from_globus(
             group_uuid,
             display_name
           )
           for group_uuid, display_name in ...
        ]
      )

      # early pre-flight check of submission context
      # (raises exceptions for early abort cases)
      registry.validate_dcc_id(dcc_id, user)

      # other ingest system prep, like archiving submitted data
      archive_url = ...

      submission = Submission(registry, id, dcc_id, archive_url, submitting_user)
      # register and perform ingest processing of archived data
      # should register (if pre-flight worked above)
      # raises exceptions for aysnc failure (should update registry before that)
      submission.ingest()

    """

    # Allow monkey-patching or other caller-driven reconfig in future?
    content_path_root = '/var/tmp/cfde_deriva_submissions'
    
    def __init__(self, server, registry, id, dcc_id, archive_url, submitting_user, archive_headers_map=None, skip_dcc_check=False):
        """Represent a stateful processing flow for a C2M2 submission.

        :param server: A DerivaServer binding object where review catalogs are created.
        :param registry: A Registry binding object.
        :param id: The unique identifier for the submission, i.e. UUID.
        :param dcc_id: The submitting DCC, using a registry dcc.id key value.
        :param archive_url: The stable URL where the submission BDBag can be found.
        :param submitting_user: A WebauthnUser instance representing submitting user.
        :param archive_headers_map: A map of URL patterns to additional request headers.
        :param skip_dcc_check: True overrides normal safety check during constructor (default False).

        The new instance is a binding for a submission which may or
        may not yet exist in the registry. The constructor WILL NOT
        cause state changes to the registry.

        The archive_headers_map is a dict hierarchy with structure like:

          {
             "url_regexp": { "header-name": "header-content", ... }
          }

        For example:

          {
             "https://[^/]*[.]data[.]globus[.]org/.*": { "Authorization": "Bearer globus-token-here" }
          }

        The purpose is to allow the caller to specify trust policies
        for which URLs ought to be requested with potentially
        sensitive request headers.  For all patterns matched by
        re.fullmatch(pattern, url), the corresponding header
        dictionaries will be merged via dict.update() while iterating
        over all matched rules. Supply an ordered dict if you care
        about the order of this merge.

        Raises UnknownDccId if dcc_id is not known by registry.
        Raises Forbidden if submitting_user is not known as a submitter for DCC by registry.
        Raises non-CfdeError exceptions for operational errors.

        """
        if not skip_dcc_check:
            registry.validate_dcc_id(dcc_id, submitting_user)
        self.server = server
        self.registry = registry
        self.datapackage_id = id
        self.submitting_dcc_id = dcc_id
        self.archive_url = archive_url
        self.review_catalog = None
        self.submitting_user = submitting_user
        self.archive_headers_map = archive_headers_map

        # check filesystem config early to abort ASAP on errors
        # TBD: check permissions for safe service config?
        os.makedirs(os.path.dirname(self.download_filename), exist_ok=True)
        os.makedirs(os.path.dirname(self.sqlite_filename), exist_ok=True)
        os.makedirs(os.path.dirname(self.content_path), exist_ok=True)

    def ingest(self):
        """Idempotently run submission-ingest processing lifecycle.

        Performs this reentrant/idempotent sequence:
        1. Register this datapackage submission
        2. Retrieve and unpack datapackage into working dir
        3. Perform client-side datapackage pre-validation steps
        4. Prepare a temporary sqlite DB to validate content and compute derived portal tables
        5. Create and register (empty) C2M2 review catalog
        6. Load datapackage content into review catalog
        7. Update registry with success/failure status

        Raises RegistrationError and aborts if step (1) failed.
        ...
        Raises non-CfdeError exceptions for operational errors.

        In error cases other than RegistrationError, the registry will
        be updated with error status prior to the exception being
        raised, unless an operational error prevents that action as
        well.  Errors derived from CfdeError will be reflected in the
        datapackage.diagnostics field of the registry, while other
        operational errors will only be reflected in the logger
        output.

        """
        # idempotently get into registry
        try:
            dp = self.registry.register_datapackage(
                self.datapackage_id,
                self.submitting_dcc_id,
                self.submitting_user,
                self.archive_url,
            )
        except Exception as e:
            logger.error('Got exception %s when registering datapackage %s, aborting!' % (e, self.datapackage_id,))
            raise exception.RegistrationError(e)

        # general sequence (with many idempotent steps)
        failed = True
        diagnostics = 'An unknown operational error has occurred.'
        failed_exc = None
        # streamline handling of error reporting...
        # next_error_state anticipates how to categorize exceptions
        # based on what we are trying to do during sequence
        next_error_state = terms.cfde_registry_dp_status.ops_error
        try:

            # shortcut if already in terminal state
            if dp['status'] in {
                    terms.cfde_registry_dp_status.content_ready,
                    terms.cfde_registry_dp_status.rejected,
                    terms.cfde_registry_dp_status.release_pending,
                    terms.cfde_registry_dp_status.obsoleted,
            }:
                logger.info('Skipping ingest for datapackage %s with existing terminal status %s.' % (
                    self.datapackage_id,
                    dp['status'],
                ))
                return

            self.retrieve_datapackage(self.archive_url, self.download_filename, self.archive_headers_map)
            self.unpack_datapackage(self.download_filename, self.content_path)

            next_error_state = terms.cfde_registry_dp_status.bag_error
            if dp['status'] not in {
                    terms.cfde_registry_dp_status.bag_valid,
                    terms.cfde_registry_dp_status.check_valid,
            }:
                self.bdbag_validate(self.content_path)
                self.registry.update_datapackage(self.datapackage_id, status=terms.cfde_registry_dp_status.bag_valid)

            def dpt_prepare(packagefile):
                """Prepare lookup tools for packagefile"""
                with open(packagefile, 'r') as f:
                    packagedoc = json.load(f)
                resources = packagedoc.get('resources', [])
                rname_to_pos = dict()
                rpath_to_pos = dict()
                for pos in range(len(resources)):
                    rname_to_pos[resources[pos].get("name")] = pos
                    rpath_to_pos[resources[pos].get("path")] = pos
                return (
                    { k: v for k, v in rname_to_pos.items() if k is not None },
                    { k: v for k, v in rpath_to_pos.items() if k is not None },
                    resources,
                )

            def dpt_register(content_path, packagefile):
                """Register resources in frictionless schema"""
                self.rname_to_pos, self.rpath_to_pos, self.resources = dpt_prepare(packagefile)
                for pos in range(len(self.resources)):
                    # this could be a null or repeated name, which we'll reject later...
                    self.registry.register_datapackage_table(self.datapackage_id, pos, self.resources[pos].get("name"))

            def dpt_update1(content_path, packagefile, report):
                """Update status of resources from frictionless report"""
                for task in report.tasks:
                    if not hasattr(task, 'resource'):
                        continue
                    if not hasattr(task.resource, 'path'):
                        continue
                    if not hasattr(task.resource, 'name'):
                        logger.debug('Could not understand report resource lacking "name": %s' % task.resource)
                        continue

                    if task.errors:
                            status = terms.cfde_registry_dpt_status.check_error
                            diagnostics = 'Tabular resource found %d errors. First error: %s' % (
                                len(task.errors),
                                task.errors[0].message
                            )
                    else:
                        status = nochange
                        diagnostics = nochange
                    num_rows = task.resource.stats.get('rows', nochange)
                    self.registry.update_datapackage_table(
                        self.datapackage_id,
                        self.rname_to_pos[task.resource.name],
                        status= status,
                        num_rows= num_rows,
                        diagnostics= diagnostics
                    )

            def dpt_update2(name, path):
                """Update status of resource following content upload"""
                try:
                    pos = self.rname_to_pos[name],
                    self.registry.update_datapackage_table(
                        self.datapackage_id,
                        self.rname_to_pos[name],
                        status= terms.cfde_registry_dpt_status.content_ready
                    )
                except KeyError as e:
                    logger.debug("Swallowing dpt_update2 callback for table %s lacking position in datapackage" % (name,))

            def dpt_error2(name, path, diagnostics):
                try:
                    pos = self.rname_to_pos[name],
                    self.registry.update_datapackage_table(
                        self.datapackage_id,
                        pos,
                        status= terms.cfde_registry_dpt_status.content_error,
                        diagnostics= diagnostics,
                    )
                except KeyError as e:
                    logger.debug("Swallowing dpt_error2 callback for table %s lacking position in datapackage" % (name,))

            if dp['status'] not in {
                    terms.cfde_registry_dp_status.check_valid,
            }:
                next_error_state = terms.cfde_registry_dp_status.check_error
                self.datapackage_model_check(self.content_path, pre_process=dpt_register)
                self.datapackage_validate(self.content_path, post_process=dpt_update1, check_fkeys=False, check_keys=False)

            next_error_state = terms.cfde_registry_dp_status.ops_error
            self.provision_sqlite(self.content_path, self.sqlite_filename)
            if self.review_catalog is None:
                self.review_catalog = self.create_review_catalog(self.server, self.registry, self.datapackage_id)

            next_error_state = terms.cfde_registry_dp_status.content_error
            self.load_sqlite(self.content_path, self.sqlite_filename, table_error_callback=dpt_error2)
            self.registry.update_datapackage(self.datapackage_id, status=terms.cfde_registry_dp_status.check_valid)
            self.record_vocab_usage(self.registry, self.sqlite_filename, self.datapackage_id)
            self.transitional_etl_dcc_table(self.content_path, self.sqlite_filename, self.submitting_dcc_id)

            next_error_state = terms.cfde_registry_dp_status.ops_error
            self.prepare_sqlite_derived_data(self.sqlite_filename)
            self.validate_submission_dcc_table(self.sqlite_filename, self.submitting_dcc_id)
            self.upload_sqlite_content(self.review_catalog, self.sqlite_filename, table_done_callback=dpt_update2, table_error_callback=dpt_error2)

            review_browse_url = '%s/chaise/recordset/#%s/CFDE:file' % (
                self.review_catalog._base_server_uri,
                self.review_catalog.catalog_id,
            )
            review_summary_url = '%s/dcc_review.html?catalogId=%s' % (
                self.review_catalog._base_server_uri,
                self.review_catalog.catalog_id,
            )
            self.registry.update_datapackage(
                self.datapackage_id,
                review_browse_url=review_browse_url,
                review_summary_url=review_summary_url,
            )
            # guard against unexpected abends not caught explicitly...
            failed = False
        except exception.CfdeError as e:
            # assume we can expose CfdeError text content
            failed, failed_exc, diagnostics = True, e, str(e)
            raise
        except Exception as e:
            # don't assume we can expose unexpected error content
            failed, failed_exc = True, e
            raise
        finally:
            # record whatever we've discovered above
            if failed:
                status, diagnostics = next_error_state, diagnostics
                if failed_exc is not None:
                    et, ev, tb = sys.exc_info()
                    logger.debug(traceback.format_exception(et, ev, tb))
                else:
                    diagnostics = 'Processing interrupted?'
                logger.error(
                    'Failed with exception %s in ingest sequence with next_error_state=%s for datapackage %s' \
                    % (failed_exc, next_error_state, self.datapackage_id,)
                )
            else:
                status, diagnostics = terms.cfde_registry_dp_status.content_ready, None
                logger.debug(
                    'Finished ingest processing for datapackage %s' % (self.datapackage_id,)
                )
            logger.debug(
                'Updating datapackage %s status=%s diagnostics=%s...' % (
                    self.datapackage_id,
                    status,
                    '(nochange)' if diagnostics is nochange else diagnostics
                )
            )
            self.registry.update_datapackage(self.datapackage_id, status=status, diagnostics=diagnostics)
            logger.debug('Datapackage %s status successfully updated.' % (self.datapackage_id,))

    ## mapping of submission ID to local processing resource names
    
    @property
    def download_filename(self):
        """Return download_filename target name for given submission id.

        We use a deterministic mapping of submission id to so that we
        can do reentrant processing.

        """
        # TBD: check or remap id character range?
        # naive mapping should be OK for UUIDs...
        return '%s/downloads/%s' % (self.content_path_root, self.datapackage_id)

    @property
    def content_path(self):
        """Return content_path working state path for a given submssion id.

        We use a deterministic mapping of submission id to
        content_path so that we can do reentrant processing.

        """
        # TBD: check or remap id character range?
        # naive mapping should be OK for UUIDs...
        return '%s/unpacked/%s' % (self.content_path_root, self.datapackage_id)

    @property
    def sqlite_filename(self):
        """Return sqlite_filename scratch C2M2 DB target name for given submssion id.

        We use a deterministic mapping of submission id to
        sqlite_filename so that we can do reentrant processing.

        """
        # TBD: check or remap id character range?
        # naive mapping should be OK for UUIDs...
        return '%s/databases/%s.sqlite3' % (self.content_path_root, self.datapackage_id)

    ## utility functions to help with various processing and validation tasks

    @classmethod
    def report_external_ops_error(cls, registry, id, diagnostics=None, status=terms.cfde_registry_dp_status.ops_error):
        """Idempotently update datapackage entry in registry with ops error and diagnostics

        :param registry: An instance of Registry authorized to make updates to submissions.
        :param id: The datapackage.id key for the failed submission
        :param diagnostics: Optional text describing the error to a user.
        :param status: The error status state to report.
        
        This method is to report errors detected outside the normal
        ingest() routine, e.g. in a parent detecting a child process
        failure.  It will set the submission status and diagnostics
        information.

        The diagostics should be a sanitized message appropriate for
        sharing with system users, while the caller should log any
        additional information necessary for devops staff.

        The status should normally be left at its default to report an
        ops-error.

        """
        try:
            dp = registry.get_datapackage(id)
        except exception.DatapackageUnknown as e:
            logger.debug('report_external_ops_error: Cannot adjust status of unknown submission id=%s' % (id,))
            logger.debug('report_external_ops_error: Discarding status="%s" diagnostics="%s"' % (status, diagnostics))
            return

        # strip newlines for presumed expansion inside a markdown table in UI
        def get_stripped_default(orig, default):
            if not orig:
                orig = default
            return orig.replace('\n', ' ').strip(' .')

        # idempotently append to diagnostics string
        # in case prior diagnostics are also useful to retain for user
        new_diagnostics = get_stripped_default(diagnostics, "Unknown ingest() process failure")
        diagnostics = get_stripped_default(dp["diagnostics"], new_diagnostics)
        if diagnostics.endswith(new_diagnostics):
            diagnostics += '.'
        else:
            diagnostics = '%s. %s' % (diagnostics, new_diagnostics)

        # figure out changes for nicer, idempotent behavior and logging
        update = {
            k: v
            for k, v in { "status": status, "diagnostics": diagnostics }.items()
            if dp[k] != v
        }

        if update:
            logger.debug("report_external_ops_error: Updating submission id=%(id)s status=%(status)s diagnostics=%(diagnostics)s" % dp)
            logger.debug("report_external_ops_error: id=%s changes: %s" % (
                id,
                ", ".join([ '%s="%s"' % (k, v) for k, v in update.items() ]),
            ))
            registry.update_datapackage(id, **update)
        else:
            logger.debug('report_external_ops_error: No change needed on submission id=%(id)s status="%(status)s" diagnostics="%(diagnostics)s"' % dp)

    @classmethod
    def retrieve_datapackage(cls, archive_url, download_filename, archive_headers_map):
        """Idempotently stage datapackage content from archive_url into download_filename.

        Uses a temporary download name and renames after successful
        download, so we can assume a file present at this name is
        already downloaded.
        """
        if os.path.isfile(download_filename):
            return

        headers = dict()
        if archive_headers_map is not None:
            for pat, hdrs in archive_headers_map.items():
                if re.fullmatch(pat, archive_url):
                    headers.update(hdrs)

        fd, tmp_name  = None, None
        try:
            # use a temporary download file in same dir
            fd, tmp_name = tempfile.mkstemp(
                suffix='.downloading',
                prefix=os.path.basename(download_filename) + '_',
                dir=os.path.dirname(download_filename),
                text=False,
            )
            logger.debug('Downloading %s to temporary download file "%s"' % (
                archive_url,
                tmp_name,
            ))

            r = requests.get(archive_url, headers=headers, stream=True)
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=128*1024):
                os.write(fd, chunk)
            os.close(fd)
            logger.debug('Finished downloading to "%s"' % (tmp_name,))
            fd = None

            # rename completed download to canonical name
            os.rename(tmp_name, download_filename)
            logger.info('Renamed download "%s" to final "%s"' % (tmp_name, download_filename))
            tmp_name = None
        finally:
            if fd is not None:
                os.close(fd)
            if tmp_name is not None:
                os.remove(tmp_name)

    @classmethod
    def unpack_datapackage(cls, download_filename, content_path):
        """Idempotently unpack download_filename (a BDBag) into content_path (bag contents dir).

        Uses a temporary unpack name and renames after successful
        unpack, so we can assume a contents dir already present at
        this name is already completely unpacked.
        """
        if os.path.isdir(content_path):
            return

        tmp_name = None
        try:
            # use temporary unpack dir in same parent dir
            tmp_name = tempfile.mkdtemp(
                suffix='.unpacking',
                prefix=os.path.basename(content_path) + '_',
                dir=os.path.dirname(content_path),
            )
            logger.debug('Extracting "%s" in temporary unpack dir "%s"' % (
                download_filename,
                tmp_name,
            ))

            # unpack ourselves so we can control output names vs. extract_bag()
            if zipfile.is_zipfile(download_filename):
                with open(download_filename, 'rb') as bag_file:
                    with zipfile.ZipFile(bag_file) as decoder:
                        decoder.extractall(tmp_name)
            elif tarfile.is_tarfile(download_filename):
                with tarfile.open(download_filename) as decoder:
                    decoder.extractall(tmp_name)
            else:
                raise exception.InvalidDatapackage('Unknown or unsupported bag archive format')

            logger.debug('Finished extracting to "%s"' % (tmp_name,))

            children = glob('%s/*' % tmp_name)
            if len(children) < 1:
                raise exception.InvalidDatapackage('Did not find expected top-level folder in bag archive')
            elif len(children) > 1:
                raise exception.InvalidDatapackage('Found too many top-level folders in bag archive')

            os.rename(children[0], content_path)
            logger.info('Renamed output "%s" to final "%s"' % (children[0], content_path))
        finally:
            if tmp_name is not None:
                shutil.rmtree(tmp_name)

    @classmethod
    def bdbag_validate(cls, content_path):
        """Perform BDBag validation of unpacked bag contents."""
        try:
            logger.debug('Validating unpacked bag at "%s"' % (content_path,))
            bdbag_api.validate_bag(content_path)
            logger.info('Bag valid at %s' % content_path)
        except (BagError, BagValidationError) as e:
            logger.error('Validation failed for bag "%s" with error "%s"' % (content_path, e,))
            raise exception.InvalidDatapackage(e)

    @classmethod
    def datapackage_name_from_path(cls, content_path):
        """Find datapackage name by globbing ./data/*.json under content_path."""
        candidates = glob('%s/data/*.json' % content_path)
        if len(candidates) < 1:
            raise exception.FilenameError('Could not locate datapackage *.json file.')
        elif len(candidates) > 1:
            raise exception.FilenameError('Found too many (%d) potential datapackage *.json choices.' % (len(candidates),))
        return candidates[0]

    @classmethod
    def datapackage_model_check(cls, content_path, pre_process=None):
        """Perform datapackage model validation for submission content.

        This validation compares the JSON datapackage specification
        against the expectations of the CFDE project for C2M2
        datapackages, i.e. ensuring that the package does not
        introduce any undesired deviations in the model definition.

        """
        canon_dp = CfdeDataPackage(portal_schema_json)
        packagefile = cls.datapackage_name_from_path(content_path)
        if pre_process:
            pre_process(content_path, packagefile)
        submitted_dp = CfdeDataPackage(packagefile)
        canon_dp.validate_model_subset(submitted_dp)

    @classmethod
    def datapackage_validate(cls, content_path, post_process=None, check_keys=True, check_fkeys=True):
        """Perform datapackage validation.

        :param content_path: The path to the submission
        :param post_process: Optional callback function with signature lambda content_path, packagefilename, report: ... (default None)
        :param check_keys: Whether to check primary key and uniqueness constraints (default True)
        :param check_fkeys: Whether to check foreign key reference constraints (default True)

        This validation considers the TSV content of the datapackage
        to be sure it conforms to its own JSON datapackage
        specification.
        """
        packagefile = cls.datapackage_name_from_path(content_path)
        logger.debug('Validating frictionless datapackage at "%s"' % packagefile)

        package = frictionless.Package(packagefile, trusted=False)
        for resource in package.resources:
            if not check_fkeys:
                resource.schema.pop('foreignKeys', None)
            if not check_keys:
                resource.schema.pop('primaryKey', None)
                for field in resource.schema.fields:
                    field.constraints.pop('unique', None)

        report = frictionless.validate_package(package, trusted=False, original=True, parallel=False)
        if post_process:
            post_process(content_path, packagefile, report)
        if report.stats['errors'] > 0:
            if report.errors:
                message = report.errors[0].message
            else:
                message = report.flatten(['message'])[0][0]
            raise exception.InvalidDatapackage(
                'Found %d errors in datapackage "%s". First error: %s' % (
                    report.stats['errors'],
                    os.path.basename(packagefile),
                    message,
            ))

    @classmethod
    def provision_sqlite(cls, content_path, sqlite_filename):
        """Idempotently prepare sqlite database containing portal model and base vocab."""
        canon_dp = CfdeDataPackage(portal_schema_json)
        # this with block produces a transaction in sqlite3
        with sqlite3.connect(sqlite_filename) as conn:
            logger.debug('Idempotently provisioning schema in %s' % (sqlite_filename,))
            canon_dp.provision_sqlite(conn)
            canon_dp.sqlite_import_data_files(conn, onconflict='skip')

    @classmethod
    def load_sqlite(cls, content_path, sqlite_filename, table_error_callback=None):
        """Idempotently insert submission content."""
        packagefile = cls.datapackage_name_from_path(content_path)
        submitted_dp = CfdeDataPackage(packagefile)
        # this with block produces a transaction in sqlite3
        with sqlite3.connect(sqlite_filename) as conn:
            logger.debug('Idempotently loading data for %s into %s' % (content_path, sqlite_filename))
            submitted_dp.sqlite_import_data_files(conn, onconflict='skip', table_error_callback=table_error_callback)

    @classmethod
    def transitional_etl_dcc_table(cls, content_path, sqlite_filename, submitting_dcc):
        """Apply transitional ETL if needed to prepare dcc table"""
        packagefile = cls.datapackage_name_from_path(content_path)
        submitted_dp = CfdeDataPackage(packagefile)
        # this with block produces a transaction in sqlite3
        with sqlite3.connect(sqlite_filename) as conn:
            cur = conn.cursor()
            if 'primary_dcc_contact' in submitted_dp.doc_cfde_schema.tables:
                if 'dcc' in submitted_dp.doc_cfde_schema.tables:
                    raise exception.InvalidDatapackage('Submission mixes C2M2 dcc and legacy primary_dcc_contact tables')
                logger.info('Translating legacy primary_dcc_contact into dcc table...')
                cur.execute("""
INSERT INTO dcc (id, dcc_name, dcc_abbreviation, dcc_description, contact_email, contact_name, dcc_url, project_id_namespace, project_local_id)
SELECT
  %(dcc_id)s,
  dcc_name, dcc_abbreviation, dcc_description, contact_email, contact_name, dcc_url, project_id_namespace, project_local_id
FROM (
  SELECT
    dcc_name, dcc_abbreviation, dcc_description, contact_email, contact_name, dcc_url, project_id_namespace, project_local_id
  FROM primary_dcc_contact
  EXCEPT
  SELECT
    dcc_name, dcc_abbreviation, dcc_description, contact_email, contact_name, dcc_url, project_id_namespace, project_local_id
   FROM dcc
) s;
""" % {
    'dcc_id': sql_literal(submitting_dcc)
})
                logger.info('Deleting legacy primary_dcc_contact records...')
                cur.execute("""DELETE FROM primary_dcc_contact;""")

    @classmethod
    def validate_submission_dcc_table(cls, sqlite_filename, submitting_dcc):
        """Validate that the dcc table in sqlite has exactly one row matching the submitting_dcc"""
        with sqlite3.connect(sqlite_filename) as conn:
            cur = conn.cursor()
            cur.execute("""SELECT count(*) FROM dcc;""")
            cnt = cur.fetchone()[0]
            if cnt != 1:
                raise exception.InvalidDatapackage('The CFDE submission must have one entry in the dcc table, not %d.' % cnt)
            cur.execute("""SELECT id FROM dcc;""")
            dcc_id = cur.fetchone()[0]
            if dcc_id != submitting_dcc:
                raise exception.InvalidDatapackage('Submission dcc.id = %s does not match submitting DCC %s' % (dcc_id, submitting_dcc,))
            cur.execute("""
SELECT d.project_id_namespace, d.project_local_id, p."RID"
FROM dcc d
LEFT OUTER JOIN project_root p ON (d.project_id_namespace = p.project_id_namespace AND d.project_local_id = p.project_local_id);
""")
            id_namespace, local_id, rid = cur.fetchone()
            if rid is None:
                raise exception.InvalidDatapackage('DCC project identifier (%s, %s) does not designate a root in the project hierarchy' % (
                    id_namespace,
                    local_id
                ))

    @classmethod
    def prepare_sqlite_derived_data(cls, sqlite_filename):
        """Prepare derived content via SQL queries in the C2M2 portal model.

        This method will clear and recompute the derived results
        each time it is invoked.

        """
        canon_dp = CfdeDataPackage(portal_schema_json)
        # this with block produces a transaction in sqlite3
        with sqlite3.connect(sqlite_filename) as conn:
            logger.debug('Building derived data in %s' % (sqlite_filename,))
            canon_dp.sqlite_do_etl(conn)

    @classmethod
    def extract_catalog_id(cls, server, catalog_url):
        m = re.match('%s/ermrest/catalog/(?P<catalog>[^/]+)/?' % server.get_server_uri(), catalog_url)
        if m:
            catalogid = m.groupdict()['catalog']
        else:
            raise ValueError('Unexpected review_ermrest_url %s does not look like a catalog on server %s' % (catalog_url, server.get_server_uri(),))
        return catalogid

    @classmethod
    def create_review_catalog(cls, server, registry, id):
        """Create and an empty review catalog for given submission id, returning deriva.ErmrestCatalog binding object.

        The resulting catalog will be properly provisioned with C2M2
        portal schema and policies, but lacking any data from this
        submission.  Some controlled vocabulary tables will be
        populated, as appropriate for the currently defined C2M2
        portal configuration.

        NOT SUPPORTED: handle model extensions present in the
        datapackage to augment the core C2M2 portal model.

        """
        # handle idempotence...
        metadata = registry.get_datapackage(id)
        catalog_url = metadata["review_ermrest_url"]
        if catalog_url:
            catalog = server.connect_ermrest(cls.extract_catalog_id(server, catalog_url))
        else:
            # register ASAP after creating, to narrow gap for orphaned catalogs...
            catalog = server.create_ermrest_catalog()
            registry.update_datapackage(id, review_ermrest_url=catalog.get_server_uri())
        # perform other catalog setup idempotently
        cls.configure_review_catalog(registry, catalog, id, provision=True)
        return catalog

    @classmethod
    def configure_review_catalog(cls, registry, catalog, id, provision=False):
        """Configure review catalog

        Configure (or reconfigure) a review catalog.

        :param registry: The Registry instance for the submission system
        :param catalog: The ErmrestCatalog for the existing review catalog
        :param id: The submission id of the submission providing the review content
        :param provision: Perform model provisioning if True (default False, only reconfigure policies/presentation)

        """
        canon_dp = CfdeDataPackage(portal_schema_json, tableschema.ReviewConfigurator(catalog=catalog, registry=registry, submission_id=id))
        canon_dp.set_catalog(catalog, registry)
        if provision:
            canon_dp.provision() # get the model deployed
        # TBD: annotate with submission ID for easier ops/inventory purposes?
        canon_dp.apply_custom_config() # get the chaise hints deloyed
        return catalog

    @classmethod
    def upload_sqlite_content(cls, catalog, sqlite_filename, table_done_callback=None, table_error_callback=None):
        """Idempotently upload (augmented) datapackage content in sqlite db into review catalog."""
        with sqlite3.connect(sqlite_filename) as conn:
            logger.debug('Idempotently uploading derived ETL data from %s' % (sqlite_filename,))
            canon_dp = CfdeDataPackage(portal_schema_json)
            canon_dp.set_catalog(catalog)
            canon_dp.load_sqlite_tables(conn, onconflict='skip', table_done_callback=table_done_callback, table_error_callback=table_error_callback)

    @classmethod
    def record_vocab_usage(cls, registry, sqlite_filename, id):
        """Upload vocabulary information to registry.

        :param registry: The Registry instance for the submission system
        :param sqlite_filename: The sqlite3 file for the loaded submission content.
        :param id: The submission id.
        """
        def get_batches(cur):
            batch = cur.fetchmany()
            while batch:
                yield batch
                batch = cur.fetchmany()

        catalog = registry._catalog
        # HACK: use portal schema to load same tables that exist in registry
        canon_dp = CfdeDataPackage(portal_schema_json)
        canon_dp.set_catalog(catalog)

        with sqlite3.connect(sqlite_filename) as conn:
            logger.info('Augmenting registry vocabulary tables...')
            canon_dp.load_sqlite_tables(conn, onconflict='skip', tablenames={'anatomy', 'assay_type', 'data_type', 'file_format', 'ncbi_taxonomy'})
            logger.info('Recording submission vocabulary usage in registry...')
            cur = conn.cursor()
            cur.arraysize = CfdeDataPackage.batch_size
            for src_tname, src_cname, dst_tname, dst_cname in [
                    ('biosample', 'anatomy', 'datapackage_anatomy', 'anatomy'),
                    ('file', 'assay_type', 'datapackage_assay_type', 'assay_type'),
                    ('file', 'data_type', 'datapackage_data_type', 'data_type'),
                    ('file', 'file_format', 'datapackage_file_format', 'file_format'),
                    ('subject_role_taxonomy', 'taxonomy_id', 'datapackage_ncbi_taxonomy', 'ncbi_taxonomy'),
                    ('subject', 'granularity', 'datapackage_subject_granularity', 'subject_granularity'),
                    ('subject_role_taxonomy', 'role_id', 'datapackage_subject_role', 'subject_role'),
            ]:
                try:
                    cur.execute("""
SELECT DISTINCT %(cname)s
FROM %(tname)s
WHERE %(cname)s IS NOT NULL
""" % {
    "tname": src_tname,
    "cname": src_cname,
})
                    for batch in get_batches(cur):
                        batch = [ { "datapackage": id, dst_cname: row[0] } for row in batch ]
                        entity_url = "/entity/CFDE:%s?onconflict=skip" % (urlquote(dst_tname),)
                        r = catalog.post(entity_url, json=batch)
                        logger.info("Batch of terms for %s recorded" % dst_tname)
                        r.json() # consume response

                    logger.info("All terms for table %s recorded" % dst_tname)
                except Exception as e:
                    logger.error("Error while recording terms for table %s: %s" % (dst_tname, e))
                    raise

def main(subcommand, *args):
    """Ugly test-harness for data submission library.

    Usage: python3 -m cfde_deriva.submission <sub-command> ...

    Sub-commands:
    - 'submit' <dcc_id> <archive_url>
       - Test harness for submission pipeline ingest flow
    - 'reconfigure' <submission_id>
       - Revise policy/resentation config on existing review catalog

    This client uses default DERIVA credentials for server both for
    registry operations and as "submitting user" in CFDE parlance.

    Set environment variable DERIVA_SERVERNAME to choose registry host.

    """
    init_logging(logging.INFO)

    servername = os.getenv('DERIVA_SERVERNAME', 'app-dev.nih-cfde.org')

    # find our authenticated user info for this test harness
    # action provider would derive this from Globus?
    credential = get_credential(servername)
    session_config = DEFAULT_SESSION_CONFIG.copy()
    session_config["allow_retry_on_all_methods"] = True
    registry = Registry('https', servername, credentials=credential, session_config=session_config)
    server = DerivaServer('https', servername, credential, session_config=session_config)
    user_session = server.get('/authn/session').json()
    submitting_user = WebauthnUser(
        user_session['client']['id'],
        user_session['client']['display_name'],
        user_session['client'].get('full_name'),
        user_session['client'].get('email'),
        [
            WebauthnAttribute(attr['id'], attr.get('display_name', 'unknown'))
            for attr in user_session['attributes']
        ]
    )

    def reconfigure_submission(row):
        if row["review_ermrest_url"] is None:
            logger.info("Submission %s does not have a catalog to reconfigure." % row["id"])
        else:
            catalog = server.connect_ermrest(Submission.extract_catalog_id(server, row['review_ermrest_url']))
            Submission.configure_review_catalog(registry, catalog, row['id'], provision=False)
            logger.info("Submission %s (%s) reconfigured." % (row["id"], row["review_ermrest_url"]))

    if subcommand == 'submit':
        # arguments dcc_id and archive_url would come from action provider
        # and it would also have a different way to obtain a submission ID
        if len(args) != 2:
            raise TypeError('"submit" requires exactly two positional arguments: dcc_id, archive_url')
        dcc_id, archive_url = args
        submission_id = str(uuid.uuid3(uuid.NAMESPACE_URL, archive_url))
        archive_headers_map = get_archive_headers_map(servername)

        # pre-flight check like action provider might want to do?
        # this is optional, implicitly happening again in Submission(...)
        registry.validate_dcc_id(dcc_id, submitting_user)

        # run the actual submission work if we get this far
        submission = Submission(server, registry, submission_id, dcc_id, archive_url, submitting_user, archive_headers_map=archive_headers_map)
        submission.ingest()
    elif subcommand == 'reconfigure':
        if len(args) == 1:
            submission_id = args[0]
        else:
            raise TypeError('"reconfigure" requires exactly one positional argument: submission_id')

        row = registry.get_datapackage(submission_id)
        reconfigure_submission(row)
    elif subcommand == 'reconfigure-all':
        for row in registry.list_datapackages():
            try:
                reconfigure_submission(row)
            except Exception as e:
                logger.info("Submission %s reconfiguration failed: %s" % (row['id'], e))
    elif subcommand == 'test_external_error':
        if len(args) != 3:
            raise TypeError('"test_external_error" requires three positional arguments: submission_id, diagnostics, status')
        id, diagnostics, status = args
        Submission.report_external_ops_error(registry, id, diagnostics, status)
    else:
        raise ValueError('unknown sub-command "%s"' % subcommand)

if __name__ == '__main__':
    exit(main(*sys.argv[1:]))

