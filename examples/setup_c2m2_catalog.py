#!/usr/bin/python3

import os
import sys
import subprocess
import json
import csv

from deriva.core import DerivaServer, get_credential, urlquote, AttrDict
from deriva.core.ermrest_config import tag
from deriva.core.ermrest_model import Table, Column, Key, ForeignKey, builtin_types

"""
Basic C2M2 catalog sketch

Demonstrates use of deriva-py APIs:
- server authentication (assumes active deriva-auth agent)
- catalog creation
- model provisioning
- basic configuration of catalog ACLs
- small Chaise presentation tweaks via model annotations
- simple insertion of tabular content

Examples:

   python3 ./examples/setup_c2m2_catalog.py ./table-schema/cfde-core-model.json

   python3 /path/to/GTEx.v7.C2M2_preload.bdbag/data/GTEx_C2M2_instance.json

when the JSON includes "path" attributes for the resources, as in the
second example above, the data files (TSV assumed) are loaded for each
resource after the schema is provisioned.

"""

# this is the deriva server where we will create a catalog
servername = os.getenv('DERIVA_SERVERNAME', 'demo.derivacloud.org')

## bind to server
credentials = get_credential(servername)
server = DerivaServer('https', servername, credentials)


# we'll use this utility function later...
def topo_sorted(depmap):
    """Return list of items topologically sorted.

       depmap: { item: [required_item, ...], ... }

    Raises ValueError if a required_item cannot be satisfied in any order.

    The per-item required_item iterables must allow revisiting on
    multiple iterations.

    """
    ordered = [ item for item, requires in depmap.items() if not requires ]
    depmap = { item: set(requires) for item, requires in depmap.items() if requires }
    satisfied = set(ordered)
    while depmap:
        additions = []
        for item, requires in list(depmap.items()):
            if requires.issubset(satisfied):
                additions.append(item)
                satisfied.add(item)
                del depmap[item]
        if not additions:
            raise ValueError(("unsatisfiable", depmap))
        ordered.extend(additions)
        additions = []
    return ordered

class CfdeDataPackage (object):
    # the translation stores frictionless table resource metadata under this annotation
    resource_tag = 'tag:isrd.isi.edu,2019:table-resource'
    # the translation leaves extranneous table-schema stuff under this annotation
    # (i.e. stuff that perhaps wasn't translated to deriva equivalents)
    schema_tag = 'tag:isrd.isi.edu,2019:table-schema-leftovers'

    # some useful group IDs to use later in ACLs...
    grp = AttrDict({
        # USC/ISI ISRD roles
        "isrd_staff": "https://auth.globus.org/176baec4-ed26-11e5-8e88-22000ab4b42b",
        'isrd_testers':    "https://auth.globus.org/9d596ac6-22b9-11e6-b519-22000aef184d",
        # demo.derivacloud.org roles
        "demo_admin": "https://auth.globus.org/5a773142-e2ed-11e8-a017-0e8017bdda58",
        "demo_creator": "https://auth.globus.org/bc286232-a82c-11e9-8157-0ed6cb1f08e0",
        "demo_writer": "https://auth.globus.org/caa11064-e2ed-11e8-9d6d-0a7c1eab007a",
        "demo_curator": "https://auth.globus.org/a5cfa412-e2ed-11e8-a768-0e368f3075e8",
        "demo_reader": "https://auth.globus.org/b9100ea4-e2ed-11e8-8b39-0e368f3075e8",
    })
    writers = [grp.demo_curator, grp.demo_writer]
    catalog_acls = {
        "owner": [grp.demo_admin, grp.demo_creator],
        "insert": writers,
        "update": writers,
        "delete": writers,
        "select": [grp.demo_reader, grp.isrd_testers, grp.isrd_staff],
        "enumerate": ["*"],
    }

    def __init__(self, filename):
        self.filename = filename
        self.dirname = os.path.dirname(self.filename)
        self.catalog = None
        self.model_root = None
        self.cfde_schema = None

        with open(self.filename, 'r') as f:
            ## ugly: use subprocess to acquire ERMrest model definitions
            self.model_doc = json.loads(
                subprocess.run(
                    ['python3', './examples/tableschema_to_deriva.py',],
                    stdin=f,
                    stdout=subprocess.PIPE
                ).stdout
            )

        if set(self.model_doc['schemas']) != {'CFDE'}:
            raise NotImplementedError('Unexpected schema set in data package: %s' % (self.model_doc['schemas'],))

    def set_catalog(self, catalog):
        self.catalog = catalog
        self.get_model()

    def get_model(self):
        self.model_root = self.catalog.getCatalogModel()
        self.cfde_schema = self.model_root.schemas.get('CFDE')

    def provision_dataset_ancestor_tables(self):
        def tdef(tname):
            return Table.define(
                tname,
                [
                    Column.define("descendant", builtin_types.text, nullok=False, comment="Contained dataset in transitive relationship."),
                    Column.define("ancestor", builtin_types.text, nullok=False, comment="Containing dataset in transitive relationship."),
                ],
                [
                    Key.define(["descendant", "ancestor"], constraint_names=[["CFDE", tname + "_assoc_key"]]),
                ],
                [
                    ForeignKey.define(
                        ["descendant"], "CFDE", "Dataset", ["id"],
                        constraint_names=[["CFDE", tname + "_descendant_fkey"]],
                    ),
                    ForeignKey.define(
                        ["ancestor"], "CFDE", "Dataset", ["id"],
                        constraint_names=[["CFDE", tname + "_ancestor_fkey"]],
                    ),
                ],
                comment="Flattened, transitive closure of nested DatasetsInDatasets relationship.",
            )

        if 'Dataset_Ancestor' not in self.model_root.schemas['CFDE'].tables:
            self.model_root.schemas['CFDE'].create_table(self.catalog, tdef("Dataset_Ancestor"))
            self.model_root.schemas['CFDE'].create_table(self.catalog, tdef("Dataset_Ancestor_Reflexive"))
        self.get_model()

    def provision(self):
        if 'CFDE' not in self.model_root.schemas:
            # blindly load the whole model on an apparently empty catalog
            self.catalog.post('/schema', json=self.model_doc).raise_for_status()
        else:
            # do some naively idempotent model definitions on existing catalog
            # adding missing tables and missing columns
            need_tables = []
            need_columns = []
            hazard_fkeys = {}
            for tname, tdoc in self.model_doc['schemas']['CFDE']['tables'].items():
                if tname in self.cfde_schema.tables:
                    table = self.cfde_schema.tables[tname]
                    for cdoc in tdoc['column_definitions']:
                        if cdoc['name'] in table.column_definitions.elements:
                            column = table.column_definitions.elements[cdoc['name']]
                            # TODO: check existing columns for compatibility?
                        else:
                            cdoc.update({'table_name': tname, 'nullok': True})
                            need_columns.append(cdoc)
                    # TODO: check existing table keys/foreign keys for compatibility?
                else:
                    tdoc['schema_name'] = 'CFDE'
                    need_tables.append(tdoc)

            if need_tables:
                print("Added tables %s" % ([tdoc['table_name'] for tdoc in need_tables]))
                self.catalog.post('/schema', json=need_tables).raise_for_status()

            for cdoc in need_columns:
                self.catalog.post(
                    '/schema/CFDE/table/%s/column' % urlquote(cdoc['table_name']),
                    json=cdoc
                ).raise_for_status()
                print("Added column %s.%s" % (cdoc['table_name'], cdoc['name']))

        self.get_model()
        self.provision_dataset_ancestor_tables()

    def apply_custom_config(self):
        self.get_model()
        self.model_root.acls.update(self.catalog_acls)

        # set custom chaise configuration values for this catalog
        self.model_root.annotations[tag.chaise_config] = {
            # hide system metadata by default in tabular listings, to focus on CFDE-specific content
            "SystemColumnsDisplayCompact": [],
        }

        # have Chaise display underscores in model element names as whitespace
        self.cfde_schema.display.name_style = {"underline_space": True}

        # prettier display of built-in ERMrest_Client table entries
        self.model_root.table('public', 'ERMrest_Client').table_display.row_name = {
            "row_markdown_pattern": "{{{Full_Name}}} ({{{Display_Name}}})"
        }

        def find_fkey(from_table, from_columns):
            from_table = self.model_root.table("CFDE", from_table)
            if isinstance(from_columns, str):
                from_columns = [from_columns]
            for fkey in from_table.foreign_keys:
                if set(from_columns) == set([ c['column_name'] for c in fkey.foreign_key_columns ]):
                    return fkey
            raise KeyError(from_columns)

        def assoc_source(markdown_name, assoc_table, left_columns, right_columns, **kwargs):
            d = {
                "source": [
                    {"inbound": find_fkey(assoc_table, left_columns).names[0]},
                    {"outbound": find_fkey(assoc_table, right_columns).names[0]},
                    "RID"
                ],
                "markdown_name": markdown_name,
            }
            d.update(kwargs)
            return d

        dsa_to_dsd = [
            {"inbound": find_fkey("Dataset_Ancestor", "ancestor").names[0]},
            {"outbound": find_fkey("Dataset_Ancestor", "descendant").names[0]},
        ]

        dsa_to_dsd_r = [
            {"inbound": find_fkey("Dataset_Ancestor_Reflexive", "ancestor").names[0]},
            {"outbound": find_fkey("Dataset_Ancestor_Reflexive", "descendant").names[0]},
        ]

        ds_to_file = dsa_to_dsd_r + [
            {"inbound": find_fkey("FilesInDatasets", "DatasetID").names[0]},
            {"outbound": find_fkey("FilesInDatasets", "FileID").names[0]},
        ]

        ds_to_devent = ds_to_file + [
            {"inbound": find_fkey("ProducedBy", "FileID").names[0]},
            {"outbound": find_fkey("ProducedBy", "DataEventID").names[0]},
        ]

        ds_to_bsamp = ds_to_devent + [
            {"inbound": find_fkey("AssayedBy", "DataEventID").names[0]},
            {"outbound": find_fkey("AssayedBy", "BioSampleID").names[0]},
        ]
        
        # improve Dataset with pseudo columns?
        sponsors = {
            "source": dsa_to_dsd_r + [
                {"inbound": find_fkey("SponsoredBy", ["DatasetID"]).names[0]},
                {"outbound": find_fkey("SponsoredBy", ["OrganizationID"]).names[0]},
                "RID"
            ],
            "markdown_name": "Sponsors",
            "aggregate": "array_d",
            "array_display": "ulist",
        }
        
        self.model_root.table('CFDE', 'Dataset').annotations[tag.visible_columns] = {
            "compact": ["title", sponsors, "description", "url"],
            "detailed": ["id", "title", sponsors, "url", "description"],
            "filter": {"and": [
                "title",
                #sponsors,
                "description",
                "url",
                {
                    "markdown_name": "Data Method",
                    "source": ds_to_devent + [ {"outbound": find_fkey("DataEvent", "method").names[0]}, "RID" ]
                },
                {
                    "markdown_name": "Data Platform",
                    "source": ds_to_devent + [ {"outbound": find_fkey("DataEvent", "platform").names[0]}, "RID" ]
                },
                {
                    "markdown_name": "Data Protocol",
                    "source": ds_to_devent + [ {"outbound": find_fkey("DataEvent", "protocol").names[0]}, "RID" ]
                },
                {
                    "markdown_name": "Biosample Type",
                    "source": ds_to_bsamp + [ {"outbound": find_fkey("BioSample", "sample_type").names[0]}, "RID" ]
                },
                assoc_source("Included By", "Dataset_Ancestor", ["descendant"], ["ancestor"]),
                assoc_source("Included Datasets", "Dataset_Ancestor", ["ancestor"], ["descendant"]),
                assoc_source("Files", "FilesInDatasets", ["DatasetID"], ["FileID"]),
            ]}
        }

        self.model_root.table('CFDE', 'Dataset').annotations[tag.visible_foreign_keys] = {
            "*": [
                {
                    "source": dsa_to_dsd + [ "RID" ],
                    "markdown_name": "Included Datasets",
                },
                {
                    "source": ds_to_file + [ "RID" ],
                    "markdown_name": "Included Files",
                }
            ]
        }

        ## apply the above ACL and annotation changes to server
        self.model_root.apply(self.catalog)
        self.get_model()

    @classmethod
    def make_row2dict(cls, table, header):
        """Pickle a row2dict(row) function for use with a csv reader"""
        numcols = len(header)
        missingValues = set(table.annotations[cls.schema_tag].get("missingValues", []))

        for cname in header:
            if cname not in table.column_definitions.elements:
                raise ValueError("header column %s not found in table %s" % (cname, table.name))

        def row2dict(row):
            """Convert row tuple to dictionary of {col: val} mappings."""
            return dict(zip(
                header,
                [ None if x in missingValues else x for x in row ]
            ))

        return row2dict

    def data_tnames_topo_sorted(self):
        def target_tname(fkey):
            return fkey.referenced_columns[0]["table_name"]
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        return topo_sorted({
            table.name: [
                target_tname(fkey)
                for fkey in table.foreign_keys
                if target_tname(fkey) != table.name and target_tname(fkey) in tables_doc
            ]
            for table in self.cfde_schema.tables.values()
            if table.name in tables_doc
        })

    def load_dataset_ancestor_tables(self):
        assoc_rows = self.catalog.get('/entity/DatasetsInDatasets').json()
        ds_ids = [ row['id'] for row in self.catalog.get('/attributegroup/Dataset/id').json() ]

        contains = {} # ancestor -> {descendant, ...}
        contained = {} # descendant -> {ancestor, ...}

        def add(d, k, v):
            if k not in d:
                d[k] = set([v])
            else:
                d[k].add(v)

        # reflexive links
        for ds in ds_ids:
            add(contains, ds, ds)
            add(contained, ds, ds)

        for row in assoc_rows:
            child = row['ContainedDatasetID']
            parent = row['ContainingDatasetID']
            add(contains, parent, child)
            add(contained, child, parent)
            for descendant in contains.get(child, []):
                add(contains, parent, descendant)
                add(contained, descendant, parent)
            for ancestor in contained.get(parent, []):
                add(contains, ancestor, child)
                add(contained, child, ancestor)

        da_pairs = {
            (descendant, ancestor)
            for descendant, ancestors in contained.items()
            for ancestor in ancestors
        }

        self.catalog.post(
            '/entity/Dataset_Ancestor_Reflexive',
            json=[
                {"descendant": descendant, "ancestor": ancestor}
                for descendant, ancestor in da_pairs
            ],
        )

        self.catalog.post(
            '/entity/Dataset_Ancestor',
            json=[
                {"descendant": descendant, "ancestor": ancestor}
                for descendant, ancestor in da_pairs
                # drop reflexive pairs
                if descendant != ancestor
            ],
        )

    def load_data_files(self):
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        for tname in self.data_tnames_topo_sorted():
            # we are doing a clean load of data in fkey dependency order
            table = self.model_root.table("CFDE", tname)
            resource = tables_doc[tname]["annotations"].get(self.resource_tag, {})
            if "path" in resource:
                fname = "%s/%s" % (self.dirname, resource["path"])
                with open(fname, "r") as f:
                    # translate TSV to python dicts
                    reader = csv.reader(f, delimiter="\t")
                    raw_rows = list(reader)
                    row2dict = self.make_row2dict(table, raw_rows[0])
                    dict_rows = [ row2dict(row) for row in raw_rows[1:] ]
                    self.catalog.post("/entity/CFDE:%s" % urlquote(table.name), json=dict_rows)
                    print("Table %s data loaded from %s." % (table.name, fname))

# ugly quasi CLI...
if len(sys.argv) < 2:
    raise ValueError('At least one data package JSON filename required as argument')

# pre-load all JSON files and convert to models
# in order to abort early on basic usage errors
datapackages = [
    CfdeDataPackage(fname)
    for fname in sys.argv[1:]
]


## create catalog
newcat = server.create_ermrest_catalog()
print('New catalog has catalog_id=%s' % newcat.catalog_id)
print("Don't forget to delete it if you are done with it!")

## deploy model(s)
for dp in datapackages:
    dp.set_catalog(newcat)
    dp.provision()
    print("Model deployed for %s." % (dp.filename,))


## customize catalog policy/presentation (only need to do once)
datapackages[0].apply_custom_config()
print("Policies and presentation configured.")

## load some sample data?
for dp in datapackages:
    dp.load_data_files()

## compute transitive-closure relationships
datapackages[0].load_dataset_ancestor_tables()

print("All data packages loaded.")

print("Try visiting 'https://%s/chaise/recordset/#%s/CFDE:Dataset'" % (
    servername,
    newcat.catalog_id,
))

## to re-bind to the same catalog in the future, extract catalog_id from URL

# server = DerivaServer('https', servername, credentials)
# catalog_id = '1234'
# catalog = server.connect_ermrest(catalog_id)

## after binding to your catalog, you can delete it too
## but we force you to be explicit:

# catalog.delete_ermrest_catalog(really=True)

