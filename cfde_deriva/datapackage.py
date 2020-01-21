#!/usr/bin/python3

import os
import sys
import json
import csv
import logging

from deriva.core import DerivaServer, get_credential, urlquote, AttrDict, topo_sorted, tag
from deriva.core.ermrest_model import Model, Table, Column, Key, ForeignKey, builtin_types

from . import tableschema

"""
Basic C2M2 catalog sketch

Demonstrates use of deriva-py APIs:
- server authentication (assumes active deriva-auth agent)
- catalog creation
- model provisioning
- basic configuration of catalog ACLs
- small Chaise presentation tweaks via model annotations
- simple insertion of tabular content

"""
logger = logging.getLogger(__name__)

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

    def __init__(self, filename, verbose=True):
        self.filename = filename
        self.dirname = os.path.dirname(self.filename)
        self.catalog = None
        self.cat_model_root = None
        self.cat_cfde_schema = None

        if verbose:
            logger.setLevel(logging.DEBUG)
            logger.addHandler(logging.StreamHandler(stream=sys.stdout))

        with open(self.filename, 'r') as f:
            self.model_doc = tableschema.make_model(json.load(f))
            self.doc_model_root = Model(None, self.model_doc)
            self.doc_cfde_schema = self.doc_model_root.schemas.get('CFDE')

        if set(self.model_doc['schemas']) != {'CFDE'}:
            raise NotImplementedError('Unexpected schema set in data package: %s' % (self.model_doc['schemas'],))

    def set_catalog(self, catalog):
        self.catalog = catalog
        self.get_model()

    def get_model(self):
        self.cat_model_root = self.catalog.getCatalogModel()
        self.cat_cfde_schema = self.cat_model_root.schemas.get('CFDE')

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
                        ["descendant"], "CFDE", "dataset", ["id"],
                        constraint_names=[["CFDE", tname + "_descendant_fkey"]],
                    ),
                    ForeignKey.define(
                        ["ancestor"], "CFDE", "dataset", ["id"],
                        constraint_names=[["CFDE", tname + "_ancestor_fkey"]],
                    ),
                ],
                comment="Flattened, transitive closure of nested DatasetsInDatasets relationship.",
            )

        if 'dataset_ancestor' not in self.cat_model_root.schemas['CFDE'].tables:
            self.cat_model_root.schemas['CFDE'].create_table(tdef("dataset_ancestor"))
            self.cat_model_root.schemas['CFDE'].create_table(tdef("dataset_ancestor_reflexive"))

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
                            ["dataset"], "CFDE", "dataset", ["id"],
                            constraint_names=[["CFDE", "%s_ds_fkey" % tname]],
                        )
                    ] +  [
                        ForeignKey.define(
                            [srccolumn.name], 'CFDE', fkey.referenced_columns[0].table.name, [ c.name for c in fkey.referenced_columns ],
                            constraint_names=[['CFDE', '%s_prop_fkey' % tname]]
                        )
                        for fkey in srctable.foreign_keys
                        if {srccolumn.name} == set([ c.name for c in fkey.foreign_key_columns ])
                    ],
                )
            )

        for tname, cname in [
                ('data_event', 'protocol'),
                ('bio_sample', 'sample_type'),
        ]:
            tab = self.cat_model_root.table('CFDE', tname)
            col = tab.column_definitions.elements[cname]
            tname, tdef = dataset_property(tab, col)
            if tname not in self.cat_model_root.schemas['CFDE'].tables:
                self.cat_model_root.schemas['CFDE'].create_table(tdef)

    def provision(self):
        if 'CFDE' not in self.cat_model_root.schemas:
            # blindly load the whole model on an apparently empty catalog
            self.catalog.post('/schema', json=self.model_doc).raise_for_status()
        else:
            # do some naively idempotent model definitions on existing catalog
            # adding missing tables and missing columns
            need_tables = []
            need_columns = []
            hazard_fkeys = {}
            for ntable in self.doc_cfde_schema.tables.values():
                table = self.cat_cfde_schema.tables.get(ntable.name)
                if table is not None:
                    for ncolumn in ntable.column_definitions:
                        column = table.column_definitions.elements.get(ncolumn.name)
                        if column is not None:
                            # TODO: check existing columns for compatibility?
                            pass
                        else:
                            cdoc = ncolumn.prejson()
                            cdoc.update({'table_name': tname, 'nullok': True})
                            need_columns.append(cdoc)
                    # TODO: check existing table keys/foreign keys for compatibility?
                else:
                    tdoc = ntable.prejson()
                    tdoc['schema_name'] = 'CFDE'
                    need_tables.append(tdoc)

            if need_tables:
                logger.debug("Added tables %s" % ([tdoc['table_name'] for tdoc in need_tables]))
                self.catalog.post('/schema', json=need_tables).raise_for_status()

            for cdoc in need_columns:
                self.catalog.post(
                    '/schema/CFDE/table/%s/column' % urlquote(cdoc['table_name']),
                    json=cdoc
                ).raise_for_status()
                logger.debug("Added column %s.%s" % (cdoc['table_name'], cdoc['name']))

        self.get_model()
        self.provision_dataset_ancestor_tables()
        self.provision_denorm_tables()

    def apply_custom_config(self):
        self.get_model()

        for schema in self.cat_model_root.schemas.values():
            for table in schema.tables.values():
                if table.is_association():
                    for cname in {'RCB', 'RMB'}:
                        for fkey in table.fkeys_by_columns([cname], raise_nomatch=False):
                            print('Dropping %s' % fkey.uri_path)
                            fkey.drop()

        # keep original catalog ownership
        # since ERMrest will prevent a client from discarding ownership rights
        acls = dict(self.catalog_acls)
        acls['owner'] = list(set(acls['owner']).union(self.cat_model_root.acls['owner']))
        self.cat_model_root.acls.update(acls)
        self.cat_model_root.table('public', 'ERMrest_Client').acls.update(self.ermrestclient_acls)
        self.cat_model_root.table('public', 'ERMrest_Group').acls.update(self.ermrestclient_acls)

        # set custom chaise configuration values for this catalog
        self.cat_model_root.annotations[tag.chaise_config] = {
            #"navbarBrandText": "CFDE Data Browser",
            "navbarMenu": {
                "children": [
                    {
                        "name": "Browse",
                        "children": [
                            { "name": "Dataset", "url": "/chaise/recordset/#%s/CFDE:dataset" % self.catalog._catalog_id },
                            { "name": "Data Event", "url": "/chaise/recordset/#%s/CFDE:data_event" % self.catalog._catalog_id },
                            { "name": "File", "url": "/chaise/recordset/#%s/CFDE:file" % self.catalog._catalog_id },
                            { "name": "Biosample", "url": "/chaise/recordset/#%s/CFDE:bio_sample" % self.catalog._catalog_id },
                            { "name": "Subject", "url": "/chaise/recordset/#%s/CFDE:subject" % self.catalog._catalog_id },
                            { "name": "Organization", "url": "/chaise/recordset/#%s/CFDE:organization" % self.catalog._catalog_id },
                            { "name": "Common Fund Program", "url": "/chaise/recordset/#%s/CFDE:common_fund_program" % self.catalog._catalog_id },
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
            self.cat_cfde_schema.display,
            "name_style",
            {"underline_space": True, "title_case": True}
        )

        def compact_visible_columns(table):
            """Emulate Chaise heuristics while hiding system metadata"""
            # hacks for CFDE:
            # - assume we have an app-level primary key (besides RID)
            # - ignore possibility of compound or overlapping fkeys
            fkeys_by_col = {
                fkey.foreign_key_columns[0].name: fkey.names[0]
                for fkey in table.foreign_keys
            }
            return [
                fkeys_by_col.get(col.name, col.name)
                for col in table.column_definitions
                if col.name not in {"RID", "RCT", "RMT", "RCB", "RMB"}
            ]

        def visible_foreign_keys(table):
            """Emulate Chaise heuristics while hiding denorm tables"""
            # hack: we use a fixed prefix for these tables
            return [
                fkey.names[0]
                for fkey in table.referenced_by
                if not fkey.table.name.startswith("dataset_denorm")
            ]

        for table in self.cat_cfde_schema.tables.values():
            ntable = self.doc_cfde_schema.tables.get(table.name)
            if ntable is None:
                continue
            table.comment = ntable.comment
            table.display.update(ntable.display)
            for column in table.column_definitions:
                if column.name in {'id', 'url', 'md5'}:
                    # set these acronyms to all-caps
                    column.display["name"] = column.name.upper()
                ncolumn = ntable.column_definitions.elements.get(column.name)
                if ncolumn is None:
                    continue
                column.comment = ncolumn.comment
                column.display.update(ncolumn.display)
            for fkey in table.foreign_keys:
                try:
                    npktable = self.doc_model_root.table(fkey.pk_table.schema.name, fkey.pk_table.name)
                    nfkey = ntable.fkey_by_column_map({
                        ntable.column_definitions[fk_col.name]: npktable.column_definitions[pk_col.name]
                        for fk_col, pk_col in fkey.column_map.items()
                    }) 
                    fkey.foreign_key.update(nfkey.foreign_key)
                except KeyError:
                    continue
            table.visible_columns = {'compact': compact_visible_columns(table)}
            table.visible_foreign_keys = {'*': visible_foreign_keys(table)}

        # prettier display of built-in ERMrest_Client table entries
        _update(
            self.cat_model_root.table('public', 'ERMrest_Client').table_display,
            'row_name',
            {"row_markdown_pattern": "{{{Full_Name}}} ({{{Display_Name}}})"}
        )

        def find_fkey(from_tname, from_cnames):
            from_table = self.cat_model_root.table("CFDE", from_tname)
            if isinstance(from_cnames, str):
                from_cnames = [from_cnames]
            fkeys = list(from_table.fkeys_by_columns(from_cnames))
            if len(fkeys) > 1:
                raise ValueError('found multiple fkeys for %s %s' % (from_table, from_cnames))
            return fkeys[0]

        def assoc_source(markdown_name, assoc_table, left_columns, right_columns):
            return {
                "source": [
                    {"inbound": find_fkey(assoc_table, left_columns).names[0]},
                    {"outbound": find_fkey(assoc_table, right_columns).names[0]},
                    "RID"
                ],
                "markdown_name": markdown_name,
            }

        dsa_to_dsd = [
            {"inbound": find_fkey("dataset_ancestor", "ancestor").names[0]},
            {"outbound": find_fkey("dataset_ancestor", "descendant").names[0]},
        ]

        dsa_to_dsd_r = [
            {"inbound": find_fkey("dataset_ancestor_reflexive", "ancestor").names[0]},
            {"outbound": find_fkey("dataset_ancestor_reflexive", "descendant").names[0]},
        ]

        ds_to_file_flat = [
            {"inbound": find_fkey("files_in_datasets", "dataset_id").names[0]},
            {"outbound": find_fkey("files_in_datasets", "file_id").names[0]},
        ]

        ds_to_file = dsa_to_dsd_r + ds_to_file_flat

        ds_to_devent = ds_to_file_flat + [
            {"inbound": find_fkey("generated_by", "file_id").names[0]},
            {"outbound": find_fkey("generated_by", "data_event_id").names[0]},
        ]

        ds_to_bsamp = ds_to_devent + [
            {"inbound": find_fkey("assayed_by", "data_event_id").names[0]},
            {"outbound": find_fkey("assayed_by", "bio_sample_id").names[0]},
        ]
        
        # improve Dataset with pseudo columns?
        orgs = {
            "source": [
                {"inbound": find_fkey("produced_by", ["dataset_id"]).names[0]},
                {"outbound": find_fkey("produced_by", ["organization_id"]).names[0]},
                "RID"
            ],
            "markdown_name": "Organization",
            "aggregate": "array_d",
            "array_display": "ulist",
        }

        program = {
            "source": [
                {"outbound": find_fkey("dataset", ["data_source"]).names[0]},
                "RID"
            ],
            "markdown_name": "Common Fund Program",
            "open": True,
        }
        self.cat_model_root.table('CFDE', 'dataset').visible_columns = {
            "compact": ["title", program, orgs, "description", "url"],
            "filter": {"and": [
                program,
                {
                    "markdown_name": "Data Method",
                    "source": [
                        {"inbound": ["CFDE", "dataset_denorm_method_ds_fkey"]},
                        {"outbound": ["CFDE", "dataset_denorm_method_prop_fkey"]},
                        "RID",
                    ],
                    "open": True,
                },
                {
                    "markdown_name": "Data Platform",
                    "source": [
                        {"inbound": ["CFDE", "dataset_denorm_platform_ds_fkey"]},
                        {"outbound": ["CFDE", "dataset_denorm_platform_prop_fkey"]},
                        "RID",
                    ],
                },
                {
                    "markdown_name": "Data Protocol",
                    "source": [
                        {"inbound": ["CFDE", "dataset_denorm_protocol_ds_fkey"]},
                        {"outbound": ["CFDE", "dataset_denorm_protocol_prop_fkey"]},
                        "RID",
                    ],
                },
                {
                    "markdown_name": "Biosample Type",
                    "source": [
                        {"inbound": ["CFDE", "dataset_denorm_sample_type_ds_fkey"]},
                        {"outbound": ["CFDE", "dataset_denorm_sample_type_prop_fkey"]},
                        "RID",
                    ],
                    "open": True,
                },
                assoc_source("Containing Dataset", "dataset_ancestor", ["descendant"], ["ancestor"]),
                assoc_source("Contained Dataset", "dataset_ancestor", ["ancestor"], ["descendant"]),
                assoc_source("Contained File", "files_in_datasets", ["dataset_id"], ["file_id"]),
                orgs,
            ]}
        }

        orgs_e = dict(orgs)
        del orgs_e['aggregate']
        del orgs_e['array_display']
        self.cat_model_root.table('CFDE', 'dataset').visible_foreign_keys = {
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

        self.cat_model_root.column('CFDE', 'dataset', 'url').column_display["*"] = {
            "markdown_pattern": "[{{{url}}}]({{{url}}})"
        }

        ## apply the above ACL and annotation changes to server
        self.cat_model_root.apply()
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
            return fkey.referenced_columns[0].table.name
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        return topo_sorted({
            table.name: [
                target_tname(fkey)
                for fkey in table.foreign_keys
                if target_tname(fkey) != table.name and target_tname(fkey) in tables_doc
            ]
            for table in self.cat_cfde_schema.tables.values()
            if table.name in tables_doc
        })

    def load_dataset_ancestor_tables(self):
        assoc_rows = self.catalog.get('/entity/datasets_in_datasets').json()
        ds_ids = [ row['id'] for row in self.catalog.get('/attributegroup/dataset/id').json() ]

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
            child = row['contained_dataset_id']
            parent = row['containing_dataset_id']
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
            '/entity/dataset_ancestor_reflexive',
            json=[
                {"descendant": descendant, "ancestor": ancestor}
                for descendant, ancestor in da_pairs
            ],
        )

        self.catalog.post(
            '/entity/dataset_ancestor',
            json=[
                {"descendant": descendant, "ancestor": ancestor}
                for descendant, ancestor in da_pairs
                # drop reflexive pairs
                if descendant != ancestor
            ],
        )

    def load_denorm_tables(self):
        query_prefix = '/attributegroup/D:=dataset/files_in_datasets/F:=file/generated_by/DE:=data_event'
        for tname, cname, query in [
                ('data_event', 'protocol', '/dataset:=D:id,DE:protocol'),
                ('bio_sample', 'sample_type', '/assayed_by/B:=bio_sample/dataset:=D:id,B:sample_type'),
        ]:
            rows = self.catalog.get("%s%s" % (query_prefix, query)).json()
            self.catalog.post("/entity/dataset_denorm_%s" % cname, json=rows).raise_for_status()
            logger.debug("Denormalization table for %s.%s loaded." % (tname, cname))

    def load_data_files(self):
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        for tname in self.data_tnames_topo_sorted():
            # we are doing a clean load of data in fkey dependency order
            table = self.cat_model_root.table("CFDE", tname)
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
                        logger.info("Table %s data loaded from %s." % (table.name, fname))
                    except Exception as e:
                        logger.error("Table %s data load FAILED from "
                                     "%s: %s" % (table.name, fname, e))
                        raise

def main(args):
    """Basic C2M2 catalog setup

    Examples:

    python3 -m cfde_deriva.datapackage \
     ./table-schema/cfde-core-model.json \
     /path/to/GTEx.v7.C2M2_preload.bdbag/data/GTEx_C2M2_instance.json

    When multiple files are specified, they are loaded in the order given.
    Earlier files take precedence in configuring the catalog model, while
    later files can merely augment it.

    When the JSON includes "path" attributes for the resources, the data
    files (TSV assumed) are loaded for each resource after the schema is
    provisioned.

    Environment variable parameters (with defaults):

    DERIVA_SERVERNAME=demo.derivacloud.org
    DERIVA_CATALOGID=

    Setting a non-empty DERIVA_CATALOGID causes reconfiguration of an
    existing catalog's presentation tweaks. It does not load data.
    
    """
    # this is the deriva server where we will create a catalog
    servername = os.getenv('DERIVA_SERVERNAME', 'demo.derivacloud.org')

    # this is an existing catalog we just want to re-configure!
    catid = os.getenv('DERIVA_CATALOGID')

    ## bind to server
    credentials = get_credential(servername)
    server = DerivaServer('https', servername, credentials)

    # ugly quasi CLI...
    if len(args) < 1:
        raise ValueError('At least one data package JSON filename required as argument')

    # pre-load all JSON files and convert to models
    # in order to abort early on basic usage errors
    datapackages = [
        CfdeDataPackage(fname)
        for fname in args
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

        print("Try visiting 'https://%s/chaise/recordset/#%s/CFDE:dataset'" % (
            servername,
            newcat.catalog_id,
        ))
    else:
        ## reconfigure existing catalog
        oldcat = server.connect_ermrest(catid)
        datapackages[0].set_catalog(oldcat)
        datapackages[0].apply_custom_config()
        print('Policies and presentation configured for %s.' % (oldcat._server_uri,))

if __name__ == '__main__':
    exit(main(sys.argv[1:]))
