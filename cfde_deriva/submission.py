
class Submission (object):
    """Processing support for C2M2 datapackage submissions.

    This class collects some utility functions, but instances of the
    class represent a stateful processing lifecycle which is coupled
    with updates to the CFDE submission registry, causing side-effects
    in the registry as other processing methods are invoked.

    Typical call-sequence from automated ingest pipeline:
    
      submission = Submission(registry, id, dcc_id, archive_url)
      submission.ingest()

    The ingest() process will perform the entire lifecycle.

    """

    # Allow monkey-patching or other caller-driven reconfig in future?
    content_path_root = '/var/tmp/cfde_deriva_submissions'
    
    def __init__(self, registry, id, dcc_id, archive_url):
        """Represent a stateful processing flow for a C2M2 submission.

        :param registry: A Registry binding object.
        :param id: The unique identifier for the submission, i.e. UUID.
        :param dcc_id: The submitting DCC, using a registry dcc.id key value.
        :param archive_url: The stable URL where the submission BDBag can be found.

        The new instance is a binding for a submission which may or
        may not yet exist in the registry. The constructor WILL NOT
        cause state changes to the registry.

        """
        self.registry = registry
        self.datapackage_id = id
        self.dcc_id = dcc_id
        self.archive_url = archive_url

    @classmethod
    def from_registry(cls, registry, id):
        """Re-instantiate a Submission using metadata from an existing registry entry.

        :param registry: A Registry binding object.
        :param id: The unique identifier for the submission, i.e. UUID.
    
        This alternate construction method supports restart of an
        existing submission already known by the registry.  The
        `dcc_id` and `archive_url` parameters will be recovered from
        the registry metadata.

        """
        entry = registry.get_datapackage(id)
        dcc_id = entry.submitting_dcc
        archive_url = entry.datapackage_url
        return cls(registry, id, dcc_id, archive_url)

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
        
        """
        # TBD: add registry side-effects
        self.retrieve_datapackage(self.archive_url, self.download_filename)
        self.unpack_datapackage(self.download_filename, self.content_path)
        self.bdbag_validate(self.content_path)
        self.datapackage_model_check(self.content_path)
        self.datapackage_validate(self.content_path)
        self.prepare_sqlite(self.content_path, self.sqlite_filename)
        self.prepare_sqlite_derived_tables(self.sqlite_filename)
        catalog = self.create_review_catalog(self.datapackage_id)
        self.upload_datapackage_content(catalog, self.content_path)
        self.upload_derived_content(catalog, self.sqlite_filename)

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
