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

# this is an existing catalog we just want to re-configure!
catid = os.getenv('DERIVA_CATALOGID')

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
        "owner": [grp.demo_admin],
        "insert": writers,
        "update": writers,
        "delete": writers,
        "select": [grp.demo_reader, grp.isrd_testers, grp.isrd_staff, "*"],
        "enumerate": ["*"],
    }
    ermrestclient_acls = {
        "select": ["*"],
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

    def provision_denorm_tables(self):
        def dataset_property(srctable, srccolumn):
            tname = 'Dataset_denorm_%s' % srccolumn.name
            return (
                tname,
                Table.define(
                    tname,
                    [
                        Column.define("dataset", builtin_types.text, nullok=False),
                        Column.define(srccolumn.name, builtin_types.text, srccolumn.nullok),
                    ],
                    [
                        Key.define(["dataset", srccolumn.name]),
                    ],
                    [
                        ForeignKey.define(
                            ["dataset"], "CFDE", "Dataset", ["id"],
                            constraint_names=[["CFDE", "%s_ds_fkey" % tname]],
                        )
                    ] +  [
                        ForeignKey.define(
                            [srccolumn.name], 'CFDE', fkey.referenced_columns[0]['table_name'], [ c['column_name'] for c in fkey.referenced_columns ],
                            constraint_names=[['CFDE', '%s_prop_fkey' % tname]]
                        )
                        for fkey in srctable.foreign_keys
                        if {srccolumn.name} == set([ c['column_name'] for c in fkey.foreign_key_columns ])
                    ],
                )
            )

        for tname, cname in [
                ('DataEvent', 'protocol'),
                ('DataEvent', 'method'),
                ('DataEvent', 'platform'),
                ('BioSample', 'sample_type'),
        ]:
            tab = self.model_root.table('CFDE', tname)
            col = tab.column_definitions.elements[cname]
            tname, tdef = dataset_property(tab, col)
            if tname not in self.model_root.schemas['CFDE'].tables:
                self.model_root.schemas['CFDE'].create_table(self.catalog, tdef)

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
        self.provision_denorm_tables()

    def apply_custom_config(self):
        self.get_model()
        acls = dict(self.catalog_acls)
        # keep original catalog ownership
        # since ERMrest will prevent a client from discarding ownership rights
        acls['owner'].append(self.model_root.acls['owner'][0])
        self.model_root.acls.update(acls)
        self.model_root.table('public', 'ERMrest_Client').acls.update(self.ermrestclient_acls)
        self.model_root.table('public', 'ERMrest_Group').acls.update(self.ermrestclient_acls)

        # set custom chaise configuration values for this catalog
        self.model_root.annotations[tag.chaise_config] = {
            #"navbarBrandText": "CFDE Data Browser",
            "navbarMenu": {
                "children": [
                    {
                        "name": "Browse",
                        "children": [
                            { "name": "Dataset", "url": "/chaise/recordset/#%s/CFDE:Dataset" % self.catalog._catalog_id },
                            { "name": "Data Event", "url": "/chaise/recordset/#%s/CFDE:DataEvent" % self.catalog._catalog_id },
                            { "name": "File", "url": "/chaise/recordset/#%s/CFDE:File" % self.catalog._catalog_id },
                            { "name": "Biosample", "url": "/chaise/recordset/#%s/CFDE:BioSample" % self.catalog._catalog_id },
                            { "name": "Subject", "url": "/chaise/recordset/#%s/CFDE:Subject" % self.catalog._catalog_id },
                            { "name": "Organization", "url": "/chaise/recordset/#%s/CFDE:Organization" % self.catalog._catalog_id },
                            { "name": "Common Fund Program", "url": "/chaise/recordset/#%s/CFDE:CommonFundProgram" % self.catalog._catalog_id },
                        ]
                    }
                ]
            }
        }

        def _update(parent, key, d):
            if key not in parent:
                parent[key] = dict()
            parent[key].update(d)
        
        # have Chaise display underscores in model element names as whitespace
        _update(
            self.cfde_schema.display,
            "name_style",
            {"underline_space": True, "title_case": True}
        )

        # definitions from table-schema file (may be newer than deployed model's annotations)
        ts_tables = self.model_doc['schemas']['CFDE']['tables']

        def _ts_column(ts_table, column):
            for cdef in ts_table['column_definitions']:
                if cdef['name'] == column.name:
                    return cdef
            raise KeyError(column.name)

        def _ts_fkey(ts_table, fkey):
            def _cmap(fkey):
                def _get(obj, key):
                    if hasattr(obj, key):
                        return getattr(obj, key)
                    else:
                        return obj[key]
                return tuple(sorted([
                    (
                        _get(fkey, "foreign_key_columns")[i]['column_name'],
                        _get(fkey, "referenced_columns")[i]['schema_name'],
                        _get(fkey, "referenced_columns")[i]['table_name'],
                        _get(fkey, "referenced_columns")[i]['column_name'],
                    )
                    for i in range(len(_get(fkey, "foreign_key_columns")))
                ]))
            
            cmap = _cmap(fkey)
            for fkdef in ts_table['foreign_keys']:
                if cmap == _cmap(fkdef):
                    return fkdef
            raise KeyError(cmap)

        def compact_visible_columns(table):
            """Emulate Chaise heuristics while hiding system metadata"""
            # hacks for CFDE:
            # - assume we have an app-level primary key (besides RID)
            # - ignore possibility of compound or overlapping fkeys
            fkeys_by_col = {
                fkey.foreign_key_columns[0]['column_name']: fkey.names[0]
                for fkey in table.foreign_keys
            }
            return [
                fkeys_by_col.get(col.name, col.name)
                for col in table.column_definitions
                if col.name not in {"RID", "RCT", "RMT", "RCB", "RMB"}
            ]

        for table in self.model_root.schemas['CFDE'].tables.values():
            if table.name not in ts_tables:
                # ignore tables not in the input table-schema file
                continue
            ts_table = ts_tables[table.name]
            table.comment = ts_table.get('annotations', {}).get(self.schema_tag, {}).get("description")
            title = ts_table.get('annotations', {}).get(tag.display, {}).get("name")
            if title:
                table.display["name"] = title
            for column in table.column_definitions:
                ts_column = _ts_column(ts_table, column)
                column.comment = ts_column.get('comment')
                if column.name in {'id', 'url', 'md5'}:
                    # set these acronyms to all-caps
                    column.display["name"] = column.name.upper()
            for fkey in table.foreign_keys:
                ts_fkey = _ts_fkey(ts_table, fkey)
                to_name = ts_fkey.get('annotations', {}).get(tag.foreign_key, {}).get("to_name")
                if to_name:
                    fkey.foreign_key["to_name"] = to_name
            table.visible_columns = {'compact': compact_visible_columns(table)}

        # prettier display of built-in ERMrest_Client table entries
        _update(
            self.model_root.table('public', 'ERMrest_Client').table_display,
            'row_name',
            {"row_markdown_pattern": "{{{Full_Name}}} ({{{Display_Name}}})"}
        )

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

        ds_to_file_flat = [
            {"inbound": find_fkey("FilesInDatasets", "DatasetID").names[0]},
            {"outbound": find_fkey("FilesInDatasets", "FileID").names[0]},
        ]

        ds_to_file = dsa_to_dsd_r + ds_to_file_flat

        ds_to_devent = ds_to_file_flat + [
            {"inbound": find_fkey("GeneratedBy", "FileID").names[0]},
            {"outbound": find_fkey("GeneratedBy", "DataEventID").names[0]},
        ]

        ds_to_bsamp = ds_to_devent + [
            {"inbound": find_fkey("AssayedBy", "DataEventID").names[0]},
            {"outbound": find_fkey("AssayedBy", "BioSampleID").names[0]},
        ]
        
        # improve Dataset with pseudo columns?
        orgs = {
            "source": [
                {"inbound": find_fkey("ProducedBy", ["DatasetID"]).names[0]},
                {"outbound": find_fkey("ProducedBy", ["OrganizationID"]).names[0]},
                "RID"
            ],
            "markdown_name": "Organization",
            "aggregate": "array_d",
            "array_display": "ulist",
        }

        program = {
            "source": [
                {"outbound": find_fkey("Dataset", ["data_source"]).names[0]},
                "RID"
            ],
            "markdown_name": "Common Fund Program",
            "open": True,
        }
        self.model_root.table('CFDE', 'Dataset').visible_columns = {
            "compact": ["title", program, orgs, "description", "url"],
            "filter": {"and": [
                program,
                {
                    "markdown_name": "Data Method",
                    "source": [
                        {"inbound": ["CFDE", "Dataset_denorm_method_ds_fkey"]},
                        {"outbound": ["CFDE", "Dataset_denorm_method_prop_fkey"]},
                        "RID",
                    ],
                    "open": True,
                },
                {
                    "markdown_name": "Data Platform",
                    "source": [
                        {"inbound": ["CFDE", "Dataset_denorm_platform_ds_fkey"]},
                        {"outbound": ["CFDE", "Dataset_denorm_platform_prop_fkey"]},
                        "RID",
                    ],
                },
                {
                    "markdown_name": "Data Protocol",
                    "source": [
                        {"inbound": ["CFDE", "Dataset_denorm_protocol_ds_fkey"]},
                        {"outbound": ["CFDE", "Dataset_denorm_protocol_prop_fkey"]},
                        "RID",
                    ],
                },
                {
                    "markdown_name": "Biosample Type",
                    "source": [
                        {"inbound": ["CFDE", "Dataset_denorm_sample_type_ds_fkey"]},
                        {"outbound": ["CFDE", "Dataset_denorm_sample_type_prop_fkey"]},
                        "RID",
                    ],
                    "open": True,
                },
                assoc_source("Containing Dataset", "Dataset_Ancestor", ["descendant"], ["ancestor"]),
                assoc_source("Contained Dataset", "Dataset_Ancestor", ["ancestor"], ["descendant"]),
                assoc_source("Contained File", "FilesInDatasets", ["DatasetID"], ["FileID"]),
                orgs,
            ]}
        }

        orgs_e = dict(orgs)
        del orgs_e['aggregate']
        del orgs_e['array_display']
        self.model_root.table('CFDE', 'Dataset').visible_foreign_keys = {
            "*": [
                orgs_e,
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

        self.model_root.column('CFDE', 'Dataset', 'url').column_display["*"] = {
            "markdown_pattern": "[{{{url}}}]({{{url}}})"
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

    def load_denorm_tables(self):
        query_prefix = '/attributegroup/D:=Dataset/FilesInDatasets/F:=File/GeneratedBy/DE:=DataEvent'
        for tname, cname, query in [
                ('DataEvent', 'platform', '/dataset:=D:id,DE:platform'),
                ('DataEvent', 'protocol', '/dataset:=D:id,DE:protocol'),
                ('DataEvent', 'method', '/dataset:=D:id,DE:method'),
                ('BioSample', 'sample_type', '/AssayedBy/B:=BioSample/dataset:=D:id,B:sample_type'),
        ]:
            rows = self.catalog.get("%s%s" % (query_prefix, query)).json()
            self.catalog.post("/entity/Dataset_denorm_%s" % cname, json=rows).raise_for_status()
            print("Denormalization table for %s.%s loaded." % (tname, cname))

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
                    entity_url = "/entity/CFDE:%s" % urlquote(table.name)
                    try:
                        self.catalog.post(entity_url, json=dict_rows)
                        print("Table %s data loaded from %s." % (table.name, fname))
                    except Exception as e:
                        print("Table %s data load FAILED from %s: %s" % (table.name, fname, e))
                        raise

# ugly quasi CLI...
if len(sys.argv) < 2:
    raise ValueError('At least one data package JSON filename required as argument')

# pre-load all JSON files and convert to models
# in order to abort early on basic usage errors
datapackages = [
    CfdeDataPackage(fname)
    for fname in sys.argv[1:]
]

if catid is None:
    ## create catalog
    newcat = server.create_ermrest_catalog()
    print('New catalog has catalog_id=%s' % newcat.catalog_id)
    print("Don't forget to delete it if you are done with it!")

    try:
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
        datapackages[0].load_denorm_tables()

        print("All data packages loaded.")
    except Exception as e:
        print('Provisioning failed: %s.\nDeleting catalog...' % e)
        newcat.delete_ermrest_catalog(really=True)
        raise

    print("Try visiting 'https://%s/chaise/recordset/#%s/CFDE:Dataset'" % (
        servername,
        newcat.catalog_id,
    ))
else:
    ## reconfigure existing catalog
    oldcat = server.connect_ermrest(catid)
    datapackages[0].set_catalog(oldcat)
    datapackages[0].apply_custom_config()
    print('Policies and presentation configured for %s.' % (oldcat._server_uri,))

## to re-bind to the same catalog in the future, extract catalog_id from URL

# server = DerivaServer('https', servername, credentials)
# catalog_id = '1234'
# catalog = server.connect_ermrest(catalog_id)

## after binding to your catalog, you can delete it too
## but we force you to be explicit:

# catalog.delete_ermrest_catalog(really=True)

