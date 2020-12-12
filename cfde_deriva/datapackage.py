#!/usr/bin/python3

import os
import io
import sys
import json
import csv
import logging
import pkgutil
from collections import UserString

from deriva.core import DerivaServer, get_credential, urlquote, topo_sorted, tag
from deriva.core.ermrest_model import Model, Table, Column, Key, ForeignKey, builtin_types

from . import tableschema
from .configs import portal, registry

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
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))

if 'history_capture' not in tag:
    tag['history_capture'] = 'tag:isrd.isi.edu,2020:history-capture'

# some special singleton strings...
class _PackageDataName (object):
    def __init__(self, package, filename):
        self.package = package
        self.filename = filename

    def __str__(self):
        return self.filename

    def get_data(self, key=None):
        """Get named content as raw buffer

        :param key: Alternate name to lookup in package instead of self
        """
        if key is None:
            key = self.filename
        return pkgutil.get_data(self.package.__name__, key)

    def get_data_str(self, key=None):
        """Get named content as unicode decoded str

        :param key: Alternate name to lookup in package instead of self
        """
        return self.get_data(key).decode()

    def get_data_stringio(self, key=None):
        """Get named content as unicode decoded StringIO buffer object

        :param key: Alternate name to lookup in package instead of self
        """
        return io.StringIO(self.get_data_str(key))

portal_schema_json = _PackageDataName(portal, 'c2m2-level1-portal-model.json')
registry_schema_json = _PackageDataName(registry, 'cfde-registry-model.json')

def sql_identifier(s):
    return '"%s"' % (s.replace('"', '""'),)

def sql_literal(s):
    if isinstance(s, str):
        return "'%s'" % (s.replace("'", "''"),)
    elif isinstance(s, (int, float)):
        return s
    else:
        raise TypeError('Unexpected type %s in sql_literal(%r)' % (type(s), s))

class CfdeDataPackage (object):
    # the translation stores frictionless table resource metadata under this annotation
    resource_tag = 'tag:isrd.isi.edu,2019:table-resource'
    # the translation leaves extranneous table-schema stuff under this annotation
    # (i.e. stuff that perhaps wasn't translated to deriva equivalents)
    schema_tag = 'tag:isrd.isi.edu,2019:table-schema-leftovers'

    batch_size = 10000 # how may rows we'll send to ermrest

    def __init__(self, package_filename, configurator=None):
        """Construct CfdeDataPackage from given package definition filename.

        Special singletons in this module select built-in data:
          - portal_schema_json
          - registry_schema_json
        """
        if not isinstance(package_filename, (str, _PackageDataName)):
            raise TypeError('package_filename must be a str filepath or built-in package data name')
        if not isinstance(configurator, (tableschema.CatalogConfigurator, type(None))):
            raise TypeError('configurator must be an instance of tableschema.CatalogConfigurator or None')

        if configurator is None:
            self.configurator = tableschema.ReviewConfigurator()
        else:
            self.configurator = configurator
        self.package_filename = package_filename
        self.catalog = None
        self.cat_model_root = None
        self.cat_cfde_schema = None
        self.cat_has_history_control = None

        # load 2 copies... first is mutated during translation
        if isinstance(package_filename, _PackageDataName):
            package_def = json.loads(package_filename.get_data_str())
            self.package_def = json.loads(package_filename.get_data_str())
        else:
            with open(self.package_filename, 'r') as f:
                package_def = json.load(f)
            with open(self.package_filename, 'r') as f:
                self.package_def = json.load(f)

        self.model_doc = tableschema.make_model(package_def, configurator=self.configurator, trusted=isinstance(self.package_filename, _PackageDataName))
        self.doc_model_root = Model(None, self.model_doc)
        self.doc_cfde_schema = self.doc_model_root.schemas.get('CFDE')

        if set(self.model_doc['schemas']) != {'CFDE'}:
            raise ValueError('Unexpected schema set in data package: %s' % (set(self.model_doc['schemas']),))

    def set_catalog(self, catalog):
        self.catalog = catalog
        self.configurator.set_catalog(catalog)
        self.get_model()
        self.cat_has_history_control = catalog.get('/').json().get("features", {}).get("history_control", False)

    def get_model(self):
        self.cat_model_root = self.catalog.getCatalogModel()
        self.cat_cfde_schema = self.cat_model_root.schemas.get('CFDE')

    def _compare_model_docs(self, candidate, absent_table_ok=True, absent_column_ok=True, extra_table_ok=False, extra_column_ok=False, extra_fkey_ok=False):
        """General-purpose model comparison to serve validation functions.

        :param candidate: A CfdeDatapackage instance being evaluated with self as baseline.
        :param absent_table_ok: Whether candidate is allowed to omit tables.
        :param absent_column_ok: Whether candidate is allowed to omit non-critical columns.
        :param extra_table_ok: Whether candidate is allowed to include tables.
        :param extra_column_ok: Whether candidate is allowed to include non-critical columns.
        :param extra_fkey_ok: Whether candidate is allowed to include foreign keys on extra, non-critical columns.

        For model comparisons, a non-critical column is one which is
        allowed to contain NULL values.

        Raises IncompatibleDatapackageModel if candidate fails validation tests.

        """
        baseline_tnames = set(self.doc_cfde_schema.tables.keys())
        if self.package_filename is portal_schema_json:
            # we have extra vocab tables not in the offical C2M2 schema
            # where it uses enumeration!
            baseline_tnames.difference_update({
                'subject_granularity',
                'subject_role',
            })
        candidate_tnames = set(candidate.doc_cfde_schema.tables.keys())

        missing_tnames = baseline_tnames.difference(candidate_tnames)
        extra_tnames = candidate_tnames.difference(baseline_tnames)
        if missing_tnames and not absent_table_ok:
            raise IncompatibleDatapackageModel(
                'Missing resources: %s' % (','.join(missing_tnames),)
            )
        if extra_tnames and not extra_table_ok:
            raise IncompatibleDatapackageModel(
                'Extra resources: %s' % (','.join(extra_tnames),)
            )

        for tname in baseline_tnames.intersection(candidate_tnames):
            baseline_table = self.doc_cfde_schema.tables[tname]
            candidate_table = candidate.doc_cfde_schema.tables[tname]
            baseline_cnames = set(baseline_table.columns.elements.keys())
            candidate_cnames = set(candidate_table.columns.elements.keys())
            missing_cnames = baseline_cnames.difference(candidate_cnames)
            extra_cnames = candidate_cnames.difference(baseline_cnames)
            missing_nonnull_cnames = [ cname for cname in missing_cnames if not baseline_table.columns[cname].nullok ]
            extra_nonnull_cnames = [ cname for cname in extra_cnames if not candidate_table.columns[cname].nullok ]
            if missing_cnames and not absent_column_ok:
                raise IncompatibleDatapackageModel(
                    'Missing columns in resource %s: %s' % (tname, ','.join(missing_cnames),)
                )
            if missing_nonnull_cnames:
                raise IncompatibleDatapackageModel(
                    'Missing non-nullable columns in resource %s: %s' % (tname, ','.join(missing_nonnull_cnames),)
                )
            if extra_cnames and not extra_column_ok:
                raise IncompatibleDatapackageModel(
                    'Extra columns in resource %s: %s' % (tname, ','.join(extra_cnames),)
                )
            if extra_nonnull_cnames:
                raise IncompatibleDatapackageModel(
                    'Extra non-nullable columns in resource %s: %s' % (tname, ','.join(extra_nonnull_cnames),)
                )

            # TBD: should this be a method in deriva-py ermrest_model.Type class?
            def type_equal(t1, t2):
                if t1.typename != t2.typename:
                    return False
                if t1.is_domain != t2.is_domain or t1.is_array != t2.is_array:
                    return False
                if t1.is_domain or t1.is_array:
                    return type_equal(t1.base_type, t2.base_type)
                return True

            for cname in baseline_cnames.intersection(candidate_cnames):
                baseline_col = baseline_table.columns[cname]
                candidate_col = candidate_table.columns[cname]
                if not type_equal(baseline_col.type, candidate_col.type):
                    raise IncompatibleDatapackageModel(
                        'Type mismatch for resource %s column %s' % (tname, cname)
                    )
                if not baseline_col.nullok and candidate_col.nullok:
                    # candidate can be more strict but not more relaxed?
                    raise IncompatibleDatapackageModel(
                        'Inconsistent nullability for resource %s column %s' % (tname, cname)
                    )

            # TBD: do any constraint comparisons here?
            #
            # for now, defer such constraint checks to DB load time...

    def validate_model_subset(self, subset):
        """Check that subset's model is a compatible subset of self.

        :param subset: A CfdeDatapackage instance which should be a subset of self.

        Raises IncompatibleDatapackageModel if supplied datapackage is not a compliant subset.
        """
        self._compare_model_docs(subset)

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
                            cdoc.update({'table_name': table.name, 'nullok': True})
                            need_columns.append(cdoc)
                    # TODO: check existing table keys/foreign keys for compatibility?
                else:
                    tdoc = ntable.prejson()
                    tdoc['schema_name'] = 'CFDE'
                    need_tables.append(tdoc)

            if need_tables:
                self.catalog.post('/schema', json=need_tables).raise_for_status()
                logger.debug("Added tables %s" % ([tdoc['table_name'] for tdoc in need_tables]))

            for cdoc in need_columns:
                self.catalog.post(
                    '/schema/CFDE/table/%s/column' % urlquote(cdoc['table_name']),
                    json=cdoc
                ).raise_for_status()
                logger.debug("Added column %s.%s" % (cdoc['table_name'], cdoc['name']))

        self.get_model()

    def apply_custom_config(self):
        self.get_model()

        self.cat_model_root.annotations.update(self.doc_model_root.annotations)
        for schema in self.cat_model_root.schemas.values():
            doc_schema = self.doc_model_root.schemas.get(schema.name)
            for table in schema.tables.values():
                doc_table = doc_schema.tables.get(table.name) if doc_schema is not None else None
                if doc_table is not None:
                    table.annotations.update(doc_table.annotations)
                for column in table.columns:
                    doc_column = doc_table.columns.elements.get(column.name) if doc_table is not None else None
                    if doc_column is not None:
                        column.annotations.update(doc_column.annotations)
                if True or table.is_association():
                    for cname in {'RCB', 'RMB'}:
                        for fkey in table.fkeys_by_columns([cname], raise_nomatch=False):
                            print('Dropping %s' % fkey.uri_path)
                            fkey.drop()

        # get appropriate policies for this catalog scenario
        self.configurator.apply_to_model(self.cat_model_root)

        # set custom chaise configuration values for this catalog
        if tag.chaise_config not in self.cat_model_root.annotations:
            self._set_default_chaise_config()

        def _update(parent, key, d):
            if key not in parent:
                parent[key] = dict()
            parent[key].update(d)

        # have Chaise display underscores in model element names as whitespace
        _update(
            self.cat_cfde_schema.display,
            "name_style",
            {
                "underline_space": True,
                "title_case": True,
            }
        )
        # turn off clutter of many links in tabular views
        _update(
            self.cat_cfde_schema.display,
            "show_foreign_key_link",
            {
                "compact": False
            }
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
                #if not fkey.table.name.startswith("dataset_denorm")
            ]

        for table in self.cat_cfde_schema.tables.values():
            ntable = self.doc_cfde_schema.tables.get(table.name)
            if ntable is None:
                continue
            table.comment = ntable.comment
            table.display.update(ntable.display)
            for column in table.column_definitions:
                if column.name in {'id', 'url', 'md5', 'sha256'}:
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
            #table.visible_columns = {'compact': compact_visible_columns(table)}
            #table.visible_foreign_keys = {'*': visible_foreign_keys(table)}

        # prettier display of built-in ERMrest_Client table entries
        _update(
            self.cat_model_root.table('public', 'ERMrest_Client').table_display,
            'row_name',
            {"row_markdown_pattern": "{{{Full_Name}}} ({{{Display_Name}}})"}
        )

        ## apply the above ACL and annotation changes to server
        self.cat_model_root.apply()
        self.get_model()

    def _set_default_chaise_config(self):
        self.cat_model_root.annotations[tag.chaise_config] = {
            #"navbarBrandText": "CFDE Data Browser",
            "SystemColumnsDisplayCompact": [],
            "SystemColumnsDisplayDetailed": [],
            "navbarMenu": {
                "children": [
                    {
                        "name": "Browse All Data",
                        "children": [
                            { "name": "Collection", "url": "/chaise/recordset/#%s/CFDE:collection" % self.catalog._catalog_id },
                            { "name": "File", "url": "/chaise/recordset/#%s/CFDE:file" % self.catalog._catalog_id },
                            { "name": "Biosample", "url": "/chaise/recordset/#%s/CFDE:biosample" % self.catalog._catalog_id },
                            { "name": "Subject", "url": "/chaise/recordset/#%s/CFDE:subject" % self.catalog._catalog_id },
                            { "name": "Project", "url": "/chaise/recordset/#%s/CFDE:project" % self.catalog._catalog_id },
                            {
                                "name": "Vocabulary",
                                "children": [
                                    { "name": "Anatomy", "url": "/chaise/recordset/#%s/CFDE:anatomy" % self.catalog._catalog_id },
                                    { "name": "Assay Type", "url": "/chaise/recordset/#%s/CFDE:assay_type" % self.catalog._catalog_id },
                                    { "name": "Data Type", "url": "/chaise/recordset/#%s/CFDE:data_type" % self.catalog._catalog_id },
                                    { "name": "File Format", "url": "/chaise/recordset/#%s/CFDE:file_format" % self.catalog._catalog_id },
                                    { "name": "NCBI Taxonomy", "url": "/chaise/recordset/#%s/CFDE:ncbi_taxonomy" % self.catalog._catalog_id },
                                    { "name": "Subject Granularity", "url": "/chaise/recordset/#%s/CFDE:subject_granularity" % self.catalog._catalog_id },
                                    { "name": "Subject Role", "url": "/chaise/recordset/#%s/CFDE:subject_role" % self.catalog._catalog_id },
                                ]
                            },
                            { "name": "ID Namespace", "url": "/chaise/recordset/#%s/CFDE:id_namespace" % self.catalog._catalog_id },
                        ]
                    },
                    { "name": "Technical Documentation", "markdownName": ":span:Technical Documentation:/span:{.external-link-icon}", "url": "https://cfde-published-documentation.readthedocs-hosted.com/en/latest/" },
                    { "name": "User Guide", "markdownName": ":span:User Guide:/span:{.external-link-icon}", "url": "https://cfde-published-documentation.readthedocs-hosted.com/en/latest/about/portalguide/" },
                    { "name": "About CFDE", "markdownName": ":span:About CFDE:/span:{.external-link-icon}", "url": "https://cfde-published-documentation.readthedocs-hosted.com/en/latest/about/CODEOFCONDUCT/"},
                    { "name": "|" },
                    { "name": "Dashboard", "url": "/dashboard.html" },
                    { "name": "Data Review", "url": "/dcc_review.html" }
                ]
            }
        }

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

    def data_tnames_topo_sorted(self, source_schema=None):
        if source_schema is None:
            source_schema = self.cat_cfde_schema
        def target_tname(fkey):
            return fkey.referenced_columns[0].table.name
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        return topo_sorted({
            table.name: [
                target_tname(fkey)
                for fkey in table.foreign_keys
                if target_tname(fkey) != table.name and target_tname(fkey) in tables_doc
            ]
            for table in source_schema.tables.values()
            if table.name in tables_doc
        })

    def load_data_files(self, onconflict='abort', table_done_callback=None):
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        for tname in self.data_tnames_topo_sorted():
            # we are doing a clean load of data in fkey dependency order
            table = self.cat_model_root.table("CFDE", tname)
            resource = tables_doc[tname]["annotations"].get(self.resource_tag, {})
            if "path" in resource:
                def open_package():
                    if isinstance(self.package_filename, _PackageDataName):
                        return self.package_filename.get_data_stringio(resource["path"])
                    else:
                        fname = "%s/%s" % (os.path.dirname(self.package_filename), resource["path"])
                        return open(fname, 'r')
                with open_package() as f:
                    # translate TSV to python dicts
                    reader = csv.reader(f, delimiter="\t")
                    row2dict = self.make_row2dict(table, next(reader))
                    entity_url = "/entity/CFDE:%s?onconflict=%s" % (urlquote(table.name), urlquote(onconflict))
                    # Batch catalog ingests; too-large ingests will hang and fail
                    # Largest known CFDE ingest has file with >5m rows
                    batch = []
                    for raw_row in reader:
                        # Collect full batch, then post at once
                        batch.append(row2dict(raw_row))
                        if len(batch) >= self.batch_size:
                            try:
                                r = self.catalog.post(entity_url, json=batch)
                                logger.debug("Batch of rows for %s loaded" % table.name)
                                skipped = len(batch) - len(r.json())
                                if skipped:
                                    logger.warning("Batch contained %d rows which were skipped (i.e. duplicate keys)" % skipped)
                            except Exception as e:
                                logger.error("Table %s data load FAILED from "
                                             "%s: %s" % (table.name, self.package_filename, e))
                                raise
                            else:
                                batch.clear()
                    # After reader exhausted, ingest final batch
                    if len(batch) > 0:
                        try:
                            r = self.catalog.post(entity_url, json=batch)
                            logger.debug("Batch of rows for %s loaded" % table.name)
                            skipped = len(batch) - len(r.json())
                            if skipped:
                                logger.warning("Batch contained %d rows which were skipped (i.e. duplicate keys)" % skipped)
                        except Exception as e:
                            logger.error("Table %s data load FAILED from "
                                         "%s: %s" % (table.name, self.package_filename, e))
                            raise
                    logger.info("All data for table %s loaded from %s." % (table.name, self.package_filename))
                    if table_done_callback:
                        table_done_callback(table.name, resource["path"])

    def sqlite_import_data_files(self, conn, onconflict='abort'):
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        for tname in self.data_tnames_topo_sorted(source_schema=self.doc_cfde_schema):
            # we are doing a clean load of data in fkey dependency order
            table = self.doc_cfde_schema.tables[tname]
            resource = tables_doc[tname]["annotations"].get(self.resource_tag, {})
            if "path" in resource:
                def open_package():
                    if isinstance(self.package_filename, _PackageDataName):
                        return self.package_filename.get_data_stringio(resource["path"])
                    else:
                        fname = "%s/%s" % (os.path.dirname(self.package_filename), resource["path"])
                        return open(fname, 'r')
                with open_package() as f:
                    # translate TSV to python dicts
                    reader = csv.reader(f, delimiter="\t")
                    header = next(reader)
                    missing = set(table.annotations[self.schema_tag].get("missingValues", []))
                    for cname in header:
                        if cname not in table.column_definitions.elements:
                            raise ValueError("header column %s not found in table %s" % (cname, table.name))
                    # Largest known CFDE ingest has file with >5m rows
                    batch = []
                    def insert_batch():
                        sql = "INSERT INTO %(table)s (%(cols)s) VALUES %(values)s %(upsert)s" % {
                            'table': sql_identifier(table.name),
                            'cols': ', '.join([ sql_identifier(c) for c in header ]),
                            'values': ', '.join([
                                '(%s)' % (', '.join([ 'NULL' if x in missing else sql_literal(x) for x in row ]))
                                for row in batch
                            ]),
                            'upsert': 'ON CONFLICT DO NOTHING' if onconflict == 'skip' else '',
                        }
                        conn.execute(sql)
                        logger.debug("Batch of rows for %s loaded" % table.name)

                    for raw_row in reader:
                        # Collect full batch, then insert at once
                        batch.append(raw_row)
                        if len(batch) >= self.batch_size:
                            try:
                                insert_batch()
                            except Exception as e:
                                logger.error("Table %s data load FAILED from "
                                             "%s: %s" % (table.name, self.package_filename, e))
                                raise
                            else:
                                batch.clear()
                    # After reader exhausted, ingest final batch
                    if len(batch) > 0:
                        try:
                            insert_batch()
                        except Exception as e:
                            logger.error("Table %s data load FAILED from "
                                         "%s: %s" % (table.name, self.package_filename, e))
                            raise
                    logger.info("All data for table %s loaded from %s." % (table.name, self.package_filename))

    def load_sqlite_etl_tables(self, conn, onconflict='abort'):
        if not self.package_filename is portal_schema_json:
            raise ValueError('load_sqlite_etl_tables() is only valid for built-in portal datapackage')
        for resource in self.package_def['resources']:
            if 'derivation_sql_path' not in resource:
                continue
            # we are copying sqlite table content to catalog under same table name
            table = self.cat_cfde_schema.tables[resource['name']]
            entity_url = "/entity/CFDE:%s?onconflict=%s" % (urlquote(table.name), urlquote(onconflict))
            cols = [ col for col in table.columns if col.name not in {'RID', 'RCT', 'RMT', 'RCB', 'RMB'} ]
            colnames = [ col.name for col in cols ]
            cur = conn.cursor()
            position = None

            def get_batch(cur):
                nonlocal position
                sql = 'SELECT %(cols)s FROM %(table)s %(where)s ORDER BY "RID" ASC LIMIT %(batchsize)s' % {
                    'cols': ', '.join([ "RID" ] + [ sql_identifier(cname) for cname in colnames ]),
                    'table': sql_identifier(table.name),
                    'where': '' if position is None else ('WHERE "RID" > %s' % sql_literal(position)),
                    'batchsize': '%d' % self.batch_size,
                }
                cur.execute(sql)
                batch = list(cur)
                if batch:
                    position = batch[-1][0]
                return batch

            def get_batches(cur):
                batch = get_batch(cur)
                while batch:
                    yield batch
                    batch = get_batch(cur)

            for batch in get_batches(cur):
                batch = [
                    dict(zip( colnames, row[1:]))
                    for row in batch
                ]
                r = self.catalog.post(entity_url, json=batch)
                logger.debug("Batch of rows for %s loaded" % table.name)
                skipped = len(batch) - len(r.json())
                if skipped:
                    logger.warning("Batch contained %d rows which were skipped (i.e. duplicate keys)" % skipped)
            logger.info("All data for table %s loaded." % (table.name,))

    def sqlite_do_etl(self, conn):
        """Do ETL described in our customized datapackage"""
        if not self.package_filename is portal_schema_json:
            raise ValueError('sqlite_do_etl() is only valid for built-in datapackages')
        for resource in self.package_def['resources']:
            if 'derivation_sql_path' in resource:
                sql = self.package_filename.get_data_str(resource['derivation_sql_path'])
                conn.execute('DELETE FROM %s' % sql_identifier(resource['name']),)
                conn.execute(sql)

    def provision_sqlite(self, conn):
        """Provision this datapackage schema into provided SQLite db

        :param conn: Connection to an already opened SQLite db.

        Trivial idempotence... use CREATE TABLE IF NOT EXISTS

        Caller should manage transactions if desired.
        """
        for tname in self.data_tnames_topo_sorted(source_schema=self.doc_cfde_schema):
            for sql in self.table_sqlite_ddl(self.doc_cfde_schema.tables[tname]):
                conn.execute(sql)

    def table_sqlite_ddl(self, table):
        """Yield SQLite DDL for table

        May yield multiple statements, each of which must be executed in order.
        """
        parts = [
            self.column_sqlite_ddl(col)
            for col in table.column_definitions
            # ignore ermrest system columns
            if col.name not in {'RCT', 'RCB', 'RMT', 'RMB'}
        ]
        parts.extend([
            self.key_sqlite_ddl(key)
            for key in table.keys
            if len(key.unique_columns) > 1
        ])
        parts.extend([
            self.fkey_sqlite_ddl(fkey)
            for fkey in table.foreign_keys
            # drop cross-schema fkeys and those using system columns
            if fkey.pk_table.schema is table.schema \
            and all([ col.name not in {'RCT', 'RCB', 'RMT', 'RMB'} for col in fkey.foreign_key_columns ])
        ])
        yield ("""
CREATE TABLE IF NOT EXISTS %(tname)s (
  %(list)s
);
""" % {
    'tname': sql_identifier(table.name),
    'list': ',\n'.join(parts),
})
        for fkey in table.foreign_keys:
            # drop cross-schema fkeys and those using system columns
            if fkey.pk_table.schema is table.schema \
               and all([ col.name not in {'RCT', 'RCB', 'RMT', 'RMB'} for col in fkey.foreign_key_columns ]):
                yield self.fkey_index_sqlite_ddl(fkey)

    def column_sqlite_ddl(self, col):
        """Output SQLite DDL for column (as part of CREATE TABLE statement)"""
        if col.name == 'RID':
            # special mapping to help with our ETL scripts...
            return '"RID" INTEGER PRIMARY KEY AUTOINCREMENT'
        parts = [ sql_identifier(col.name), self.type_sqlite_ddl(col.type) ]
        if not col.nullok:
            parts.append('NOT NULL')
        key = col.table.key_by_columns({col}, raise_nomatch=False)
        if key is not None:
            parts.append('UNIQUE')
        return ' '.join(parts)

    def type_sqlite_ddl(self, typeobj):
        """Output SQLite type-name for type"""
        # raise KeyError if we encounter an unmapped type!
        return {
            'text': 'text',
            'timestamptz': 'datetime',
            'date': 'date',
            'int8': 'int8',
            'float8': 'real',
        }[typeobj.typename]

    def key_sqlite_ddl(self, key):
        """Output SQLite DDL for key (as part of CREATE TABLE statement)"""
        return "UNIQUE (%(cols)s)" % {
            'cols': ', '.join([ sql_identifier(c.name) for c in key.columns ]),
        }

    def fkey_sqlite_ddl(self, fkey):
        """Output SQLite DDL for fkey (as part of CREATE TABLE statment)"""
        items = list(fkey.column_map.items())
        return "FOREIGN KEY (%(fromcols)s) REFERENCES %(totable)s (%(tocols)s)" % {
            'totable': sql_identifier(fkey.pk_table.name),
            'fromcols': ', '.join([ sql_identifier(e[0].name) for e in items ]),
            'tocols': ', '.join([ sql_identifier(e[1].name) for e in items ]),
        }

    def fkey_index_sqlite_ddl(self, fkey):
        """Output SQLite DDL for index covering fkey columns (to complement CREATE TABLE statement)"""
        # figure out canonical ordering to match key constraint
        key = fkey.pk_table.key_by_columns(fkey.referenced_columns)
        refcol_ranks = {
            refcol: key.unique_columns.index(refcol)
            for refcol in fkey.referenced_columns
        }
        fkcol_ranks = []
        for fkcol, pkcol in fkey.column_map.items():
            fkcol_ranks.append( (fkcol, refcol_ranks[pkcol]) )
        fkcol_ranks.sort(key=lambda e: (e[1], e[0].name))
        cols = [ col for col, rank in fkcol_ranks ]
        return "CREATE INDEX IF NOT EXISTS %(idxname)s ON %(tname)s (%(cols)s);" % {
            'idxname': sql_identifier('%s_idx' % fkey.name[1]),
            'tname': sql_identifier(fkey.table.name),
            'cols': ', '.join([ sql_identifier(c.name) for c in cols ]),
        }

def main(args):
    """Basic C2M2 catalog setup

    Examples:

    python3 -m cfde_deriva.datapackage \
     PORTAL_SCHEMA \
     /path/to/GTEx.v7.C2M2_preload.bdbag/data/GTEx_C2M2_instance.json

    The special names PORTAL_SCHEMA and REGISTRY_SCHEMA select the
    builtin C2M2 portal or CFDE registry schema embedded as package
    data in cfde_deriva, respectively.

    When multiple files are specified, they are loaded in the order given.
    Earlier files take precedence in configuring the catalog model, while
    later files can merely augment it.

    When the JSON includes "path" attributes for the resources, the data
    files (TSV assumed) are loaded for each resource after the schema is
    provisioned.

    Environment variable parameters (with defaults):

    DERIVA_SERVERNAME=app-dev.nih-cfde.org
    DERIVA_CATALOGID=
    DERIVA_ONCONFLICT=abort
    DERIVA_INCREMENTAL_LOAD=false
    CFDE_CATALOG_TYPE=review

    Setting a non-empty DERIVA_CATALOGID causes reconfiguration of an
    existing catalog's presentation tweaks. It does not load data
    unless combined with DERIVA_INCREMENTAL_LOAD=true.

    """
    # this is the deriva server where we will create a catalog
    servername = os.getenv('DERIVA_SERVERNAME', 'app-dev.nih-cfde.org')

    # this is an existing catalog we just want to re-configure!
    catid = os.getenv('DERIVA_CATALOGID')

    ## bind to server
    credentials = get_credential(servername)
    server = DerivaServer('https', servername, credentials)

    onconflict = os.getenv('DERIVA_ONCONFLICT', 'abort')
    incremental_load = os.getenv('DERIVA_INCREMENTAL_LOAD', 'false').lower() == 'true'

    configurator_class = {
        'review': tableschema.ReviewConfigurator,
        'release': tableschema.ReleaseConfigurator,
        'registry': tableschema.RegistryConfigurator,
    }[os.getenv('CFDE_CATALOG_TYPE', 'review')]

    # ugly quasi CLI...
    if len(args) < 1:
        raise ValueError('At least one data package JSON filename required as argument')

    # pre-load all JSON files and convert to models
    # in order to abort early on basic usage errors
    datapackages = [
        CfdeDataPackage(
            # map magic filenames to internal singletons
            {
                'PORTAL_SCHEMA': portal_schema_json,
                'REGISTRY_SCHEMA': registry_schema_json,
            }.get(fname, fname),
            configurator_class()
        )
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
                print("Model deployed for %s." % (dp.package_filename,))

            ## customize catalog policy/presentation (only need to do once)
            datapackages[0].apply_custom_config()
            print("Policies and presentation configured.")

            ## load some sample data?
            for dp in datapackages:
                dp.load_data_files(onconflict=onconflict)

            print("All data packages loaded.")
        except Exception as e:
            print('Provisioning failed: %s.\nDeleting catalog...' % e)
            newcat.delete_ermrest_catalog(really=True)
            raise

        print("Try visiting 'https://%s/chaise/recordset/#%s/CFDE:collection'" % (
            servername,
            newcat.catalog_id,
        ))
    else:
        ## work with existing catalog
        oldcat = server.connect_ermrest(catid)
        if incremental_load:
            for dp in datapackages:
                dp.set_catalog(oldcat)
                dp.provision()
                dp.load_data_files(onconflict=onconflict)
        ## reconfigure
        datapackages[0].set_catalog(oldcat)
        datapackages[0].apply_custom_config()
        print('Policies and presentation configured for %s.' % (oldcat._server_uri,))

if __name__ == '__main__':
    exit(main(sys.argv[1:]))
