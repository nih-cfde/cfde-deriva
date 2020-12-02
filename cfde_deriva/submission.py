
import os
import sys
import logging
import json
import csv
import pkgutil

from . import exception
from .registry import Registry, WebauthnUser, WebauthnAttributes, nochange

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

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
    
    def __init__(self, registry, id, dcc_id, archive_url, submitting_user):
        """Represent a stateful processing flow for a C2M2 submission.

        :param registry: A Registry binding object.
        :param id: The unique identifier for the submission, i.e. UUID.
        :param dcc_id: The submitting DCC, using a registry dcc.id key value.
        :param archive_url: The stable URL where the submission BDBag can be found.
        :param submitting_user: A WebauthnUser instance representing submitting user.

        The new instance is a binding for a submission which may or
        may not yet exist in the registry. The constructor WILL NOT
        cause state changes to the registry.

        """
        registry.validate_dcc_id(dcc_id, submitting_user)
        self.registry = registry
        self.datapackage_id = id
        self.submitting_dcc_id = dcc_id
        self.archive_url = archive_url
        self.review_catalog = None
        self.submitting_user = submitting_user

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
        
        May raise other exceptions during remaining processing:

          - 

        in these cases, the registry should be updated with error
        status prior to the exception being raised, unless an
        operational error prevents that action as well.

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
            logger.debug('Got exception %s when registering datapackage %s, aborting!' % (e, self.datapackage_id,))
            raise exception.RegistrationError(e)

        # shortcut if already in terminal state
        if dp['status'] in {
                term.cfde_registry_dp_status.content_ready,
                term.cfde_registry_dp_status.rejected,
                term.cfde_registry_dp_status.release_pending,
                term.cfde_registry_dp_status.obsoleted,
        }:
            logger.debug('Skiping ingest for datapackage %s with existing terminal status %s.' % (
                self.datapackage_id,
                dp['status'],
            ))
            return

        # general sequence (with many idempotent steps)
        try:
            # streamline handling of error reporting...
            # next_error_state anticipates how to categorize exceptions
            # based on what we are trying to do during sequence
            failed = False
            diagnostics = 'An unknown operational error has occured.'
            failed_exc = None

            next_error_state = terms.cfde_registry_dp_status.ops_error
            self.retrieve_datapackage(self.archive_url, self.download_filename)
            self.unpack_datapackage(self.download_filename, self.content_path)

            next_error_state = terms.cfde_registry_dp_status.bag_error
            self.bdbag_validate(self.content_path)

            next_error_state = terms.cfde_registry_dp_status.check_error
            self.datapackage_model_check(self.content_path)
            self.datapackage_validate(self.content_path)

            next_error_state = terms.cfde_registry_dp_status.content_error
            self.prepare_sqlite(self.content_path, self.sqlite_filename)

            next_error_state = terms.cfde_registry_dp_status.ops_error
            self.prepare_sqlite_derived_tables(self.sqlite_filename)
            if self.review_catalog is None:
                self.review_catalog = self.create_review_catalog(self.datapackage_id)

            next_error_state = terms.cfde_registry_dp_status.content_error
            self.upload_datapackage_content(self.review_catalog, self.content_path)

            next_error_state = terms.cfde_registry_dp_status.ops_error
            self.upload_derived_content(self.review_catalog, self.sqlite_filename)
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
                logger.debug(
                    'Got exception %s in ingest sequence with next_error_state=%s for datapackage %s' \
                    % (failed_exc, next_error_state, self.datapackage_id,)
                )
            else:
                status, diagnostics = term.cfde_registry_dp_status.content_ready, nochange
                logger.debug(
                    'Finished ingest processing for datapackage %s' % (self.datapackage_id,)
                )
            logger.debug(
                'Updating datapackage %s status=%s diagnostics=%s...' % (
                    status,
                    '(nochange)' if diagnostics is nochange else diagnostics
                )
            )
            self.registry.update_status(self.datapackage_id, status=status, diagnostics=diagnostics)
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
        return '%s/downloads/%s' % (cls.content_path_root, id)

    @property
    def content_path(self):
        """Return content_path working state path for a given submssion id.

        We use a deterministic mapping of submission id to
        content_path so that we can do reentrant processing.

        """
        # TBD: check or remap id character range?
        # naive mapping should be OK for UUIDs...
        return '%s/unpacked/%s' % (cls.content_path_root, id)

    @property
    def sqlite_filename(self):
        """Return sqlite_filename scratch C2M2 DB target name for given submssion id.

        We use a deterministic mapping of submission id to
        sqlite_filename so that we can do reentrant processing.

        """
        # TBD: check or remap id character range?
        # naive mapping should be OK for UUIDs...
        return '%s/databases/%s.sqlite3' % (cls.content_path_root, id)

    ## utility functions to help with various processing and validation tasks

    @classmethod
    def retrieve_datapackage(cls, archive_url, download_filename):
        """Idempotently stage datapackage content from archive_url into download_filename.

        Use a temporary download name and rename after successful
        download, so we can assume a file present at this name is
        already downloaded.

        """
        pass

    @classmethod
    def unpack_datapackage(cls, download_filename, content_path):
        """Idempotently unpack download_filename (a BDBag) into content_path (bag contents dir).

        Use a temporary unpack name and rename after successful
        unpack, so we can assume a contents dir already present at
        this name is already completely unpacked.

        """
        pass

    @classmethod
    def bdbag_validate(cls, content_path):
        """Perform BDBag validation of unpacked bag contents."""
        # or is this implicit part of unpack_datapackage?
        pass

    @classmethod
    def datapackage_validate(cls, content_path):
        """Perform datapackage validation.

        This validation considers the TSV content of the datapackage
        to be sure it conforms to its own JSON datapackage
        specification.

        """
        pass

    @classmethod
    def datapackage_model_check(cls, content_path):
        """Perform datapackage model validation for submission content.

        This validation compares the JSON datapackage specification
        against the expectations of the CFDE project for C2M2
        datapackages, i.e. ensuring that the package does not
        introduce any undesired deviations in the model definition.

        """
        pass

    @classmethod
    def prepare_sqlite(cls, content_path, sqlite_filename):
        """Idempotently prepare sqlite database containing submission content."""
        pass

    @classmethod
    def prepare_sqlite_derived_tables(cls, sqlite_filename):
        """Prepare derived table content via SQL queries in the C2M2 portal model.

        This method will clear and recompute the derived results
        each time it is invoked.

        """
        pass

    @classmethod
    def create_review_catalog(cls, id):
        """Create and an empty review catalog for given submission id, returning deriva.ErmrestCatalog binding object.

        The resulting catalog will be properly provisioned with C2M2
        portal schema and policies, but lacking any data from this
        submission.  Some controlled vocabulary tables will be
        populated, as appropriate for the currently defined C2M2
        portal configuration.

        NOT SUPPORTED: handle model extensions present in the
        datapackage to augment the core C2M2 portal model.

        """
        # TBD: use annotation to embed submission ID into catalog, for error recovery/cleanup tasks?
        return catalog_object

    @classmethod
    def upload_datapackage_content(cls, catalog, content_path):
        """Idempotently upload submission content from datapackage into review catalog."""
        pass

    @classmethod
    def upload_derived_content(cls, catalog, sqlite_filename):
        """Idempotently upload prepared review content in sqlite db into review catalog."""
        pass


def main(dcc_id, archive_url):
    """Ugly test-harness for data submission library.

    Usage: python3 -m cfde_deriva.submission 'dcc_id' 'archive_url'

    Runs submission functions using default DERIVA credentials for
    server both for registry operations and as "submitting user" in
    CFDE parlance.

    Set environment variable DERIVA_SERVERNAME to choose registry host.

    """
    logger.addHandler(logging.StreamHandler(stream=sys.stderr))

    servername = os.getenv('DERIVA_SERVERNAME', 'app-dev.nih-cfde.org')
    registry = Registry('https', servername)

    # find our authenticated user info for this test harness
    # action provider would derive this from Globus?
    credential = get_credential(servername)
    server = DerivaServer('https', servername, credential)
    user_session = server.get('/authn/session').json()
    submitting_user = WebauthnUser(
        user_session['client']['id'],
        user_session['client']['display_name'],
        user_session['client'].get('full_name'),
        user_session['client'].get('email'),
        [
            WebauthnAttribute(attr['id'], attr['display_name'])
            for attr in user_session['attributes']
        ]
    )

    # arguments dcc_id and archive_url would come from action provider
    # and it would also have a different way to obtain a submission ID
    submission_id = uuid.uuid3(uuid.NAMESPACE_URL, archive_url)

    # pre-flight check like action provider might want to do?
    # this is optional, implicitly happening again in Submission(...)
    registry.validate_dcc_id(dcc_id, submitting_user)

    # run the actual submission work if we get this far
    submission = Submission(registry, submission_id, dcc_id, archive_url, submitting_user)
    submission.ingest()

if __name__ == '__main__':
    exit(main(sys.argv[1:]))

