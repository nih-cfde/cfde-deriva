
import os
import io
import sys
import re
import json
import csv
import logging
import pkgutil
import itertools
from collections import UserString

from deriva.core import DerivaServer, get_credential, urlquote, topo_sorted, tag
from deriva.core.ermrest_model import Model, Table, Column, Key, ForeignKey, builtin_types
import requests

from . import tableschema
from .configs import portal, registry
from .exception import IncompatibleDatapackageModel, InvalidDatapackage

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

    def set_catalog(self, catalog, registry=None):
        self.catalog = catalog
        self.configurator.set_catalog(catalog, registry)
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

    def provision(self, alter=False):
        if 'CFDE' not in self.cat_model_root.schemas:
            # blindly load the whole model on an apparently empty catalog
            self.catalog.post('/schema', json=self.model_doc).raise_for_status()
        else:
            # do some naively idempotent model definitions on existing catalog
            # adding missing tables and missing columns
            need_tables = []
            need_columns = []
            alter_columns = []
            for ntable in self.doc_cfde_schema.tables.values():
                table = self.cat_cfde_schema.tables.get(ntable.name)
                if table is not None:
                    for ncolumn in ntable.column_definitions:
                        column = table.column_definitions.elements.get(ncolumn.name)
                        if ncolumn.name in {'RID', 'RCT', 'RMT', 'RCB', 'RMB'}:
                            # consider schema upgrades except on system columns...
                            pass
                        elif column is not None:
                            change = {}
                            if ncolumn.nullok != column.nullok:
                                if alter:
                                    change['nullok'] = ncolumn.nullok
                                elif column.nullok:
                                    # existing column can accept this data
                                    pass
                                else:
                                    raise ValueError('Incompatible nullok settings for %s.%s' % (table.name, column.name))
                            def defeq(d1, d2):
                                if d1 is None:
                                    if d2 is not None:
                                        return False
                                return d1 == d2
                            if not defeq(ncolumn.default, column.default):
                                if alter:
                                    change['default'] = ncolumn.default
                                else:
                                    # no compatibility model for defaults?
                                    pass
                            def typeeq(t1, t2):
                                if t1.typename != t2.typename:
                                    return False
                                if t1.is_domain != t2.is_domain:
                                    return False
                                if t1.is_array != t2.is_array:
                                    return False
                                if t1.is_domain or t1.is_array:
                                    return typeeq(t1.base_type, t2.base_type)
                                return True
                            if not typeeq(ncolumn.type, column.type):
                                if alter:
                                    change['type'] = ncolumn.type
                                else:
                                    raise ValueError('Mismatched type settings for %s.%s' % (table.name, column.name))
                            if change:
                                alter_columns.append((column, change))
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

            for pair in alter_columns:
                column, cdoc = pair
                column.alter(**cdoc)
                logger.debug("Altered column %s.%s %s" % (column.table.name, column.name, cdoc))

        logger.info('Provisioned model in catalog %s' % self.catalog.get_server_uri())
        self.get_model()

    def apply_custom_config(self):
        self.get_model()

        # get appropriate policies for this catalog scenario
        self.configurator.apply_to_model(self.cat_model_root)
        self.configurator.apply_to_model(self.doc_model_root)

        self.cat_model_root.annotations.update(self.doc_model_root.annotations)
        for schema in self.cat_model_root.schemas.values():
            doc_schema = self.doc_model_root.schemas.get(schema.name)
            if doc_schema is None:
                continue
            schema.acls.clear()
            schema.acls.update(doc_schema.acls)
            for table in schema.tables.values():
                doc_table = doc_schema.tables.get(table.name)
                if doc_table is None:
                    continue
                table.annotations.update(doc_table.annotations)
                table.acls.clear()
                table.acl_bindings.clear()
                table.acls.update(doc_table.acls)
                table.acl_bindings.update(doc_table.acl_bindings)
                for column in table.columns:
                    doc_column = doc_table.columns.elements.get(column.name)
                    if doc_column is None:
                        continue
                    column.annotations.update(doc_column.annotations)
                    column.acls.clear()
                    column.acl_bindings.clear()
                    column.acls.update(doc_column.acls)
                    column.acl_bindings.update(doc_column.acl_bindings)
                if True or table.is_association():
                    for cname in {'RCB', 'RMB'}:
                        for fkey in table.fkeys_by_columns([cname], raise_nomatch=False):
                            logger.info('Dropping %s' % fkey.uri_path)
                            fkey.drop()

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

        ## apply the above ACL and annotation changes to server
        self.cat_model_root.apply()
        logger.info('Applied custom config to catalog %s' % self.catalog.get_server_uri())
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

    def dump_data_files(self, resources=None, dump_dir=None):
        """Dump resources to TSV files (inverse of normal load process)

        :param resources: List of resources from datapackage or None (default) to get automatic list.
        :param dump_dir: Path to a directory to write files or None (default) to use current working directory.

        The automatic list of dump resources is derived from the
        datapackage, including those tables that specified an input
        file via the "path" attribute.  Other resources are skipped.

        """
        if resources is None:
            resources = [
                resource
                for resource in self.package_def['resources']
                if 'path' in resource
            ]

        for resource in resources:
            if 'path' in resource:
                fname = os.path.basename(resource['path'])
            else:
                # allow dumping of unusual resources?
                fname = '%s.tsv' % (resource['name'],)

            if dump_dir is not None:
                fname = '%s/%s' % (dump_dir.rstrip('/'), fname)

            # dump the same TSV columns included in package def (not extra DERIVA fields)
            cnames = [ field['name'] for field in resource['schema']['fields'] ]

            def get_data():
                r = self.catalog.get(
                    '/entity/CFDE:%s@sort(RID)?limit=%d' % (
                        urlquote(resource['name']),
                        self.batch_size,
                    ))
                rows = r.json()
                yield rows

                while rows:
                    last = rows[-1]['RID']
                    r = self.catalog.get(
                        '/entity/CFDE:%s@sort(RID)@after(%s)?limit=%d' % (
                            urlquote(resource['name']),
                            urlquote(last),
                            self.batch_size,
                    ))
                    rows = r.json()
                    yield rows

            with open(fname, 'w') as f:
                writer = csv.writer(f, delimiter='\t', lineterminator='\n')
                writer.writerow(tuple(cnames))
                for rows in get_data():
                    for row in rows:
                        writer.writerow(tuple([
                            row[cname] if row is not None else ''
                            for cname in cnames
                        ]))
                del writer
            logger.info('Dumped resource "%s" as "%s"' % (resource['name'], fname))

    def sqlite_import_data_files(self, conn, onconflict='abort', table_error_callback=None):
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        for tname in self.data_tnames_topo_sorted(source_schema=self.doc_cfde_schema):
            # we are doing a clean load of data in fkey dependency order
            table = self.doc_cfde_schema.tables[tname]
            resource = tables_doc[tname]["annotations"].get(self.resource_tag, {})
            logger.debug('Loading table "%s"...' % tname)
            if "path" not in resource:
                continue
            def open_package():
                if isinstance(self.package_filename, _PackageDataName):
                    return self.package_filename.get_data_stringio(resource["path"])
                else:
                    fname = "%s/%s" % (os.path.dirname(self.package_filename), resource["path"])
                    return open(fname, 'r')
            try:
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
            except UnicodeDecodeError as e:
                if table_error_callback:
                    table_error_callback(resource["name"], resource["path"], str(e))
                raise InvalidDatapackage('Resource file "%s" is not valid UTF-8 data: %s' % (resource["path"], e))

    def load_sqlite_tables(self, conn, onconflict='abort', table_done_callback=None, table_error_callback=None):
        if not self.package_filename is portal_schema_json:
            raise ValueError('load_sqlite_tables() is only valid for built-in portal datapackage')
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        for tname in self.data_tnames_topo_sorted(source_schema=self.doc_cfde_schema):
            # we are copying sqlite table content to catalog under same table name
            table = self.doc_cfde_schema.tables[tname]
            resource = tables_doc[tname]["annotations"].get(self.resource_tag, {})
            logger.debug('Loading table "%s"...' % tname)
            entity_url = "/entity/CFDE:%s?onconflict=%s" % (urlquote(table.name), urlquote(onconflict))
            cols = [ col for col in table.columns if col.name not in {'RID', 'RCT', 'RMT', 'RCB', 'RMB'} ]
            colnames = [ col.name for col in cols ]
            valfuncs = [
                # f(x) does json decoding or is identify func, depending on column def
                (lambda x: json.loads(x)) if col.type.typename == 'text[]' else lambda x: x
                for col in cols
            ]
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

            try:
                for batch in get_batches(cur):
                    batch = [
                        # generate per-row dict { colname: f(x), ... } with transcoded row values
                        dict(zip( colnames, [ f(x) for f, x in zip (valfuncs, row[1:]) ]))
                        for row in batch
                    ]
                    r = self.catalog.post(entity_url, json=batch)
                    logger.info("Batch of rows for %s loaded" % table.name)
                    skipped = len(batch) - len(r.json())
                    if skipped:
                        logger.debug("Batch contained %d rows which were skipped (i.e. duplicate keys)" % skipped)
                logger.info("All data for table %s loaded." % (table.name,))
                if table_done_callback:
                    table_done_callback(table.name, resource.get("path", None))
            except Exception as e:
                logger.error("Error while loading data for table %s: %s" % (table.name, e))
                if table_error_callback:
                    table_error_callback(table.name, resource.get("path", None), str(e))
                raise

    def sqlite_do_etl(self, conn, do_etl_tables=True, do_etl_columns=True):
        """Do ETL described in our customized datapackage

        :param do_etl_tables: Do normal ETL table processing (default True)
        :param do_etl_columns: Do normal ETL column processing (default True)

        Suppression of a processing step by the optional parameters
        requires that the caller ensure any prerequisites are already
        satisfied in the supplied database.

        We assume that all ETL tables (resources w/ table-level
        derivation_sql_path) are appropriate to process in document
        order, assuming regular data tables (resources w/ path) are
        already loaded.  Specifically, ETL queries may consume content
        of ETL tables listed earlier in the resource list.

        We assume that ETL columns (fields w/ field-level
        derivation_sql_path) are appropriate to load in arbitrary
        order, assuming regular data tables and ETL tables are already
        prepared.  Specifically, ETL queries should not attempt to
        consume content prepared in other ETL columns.

        """
        if not self.package_filename is portal_schema_json:
            raise ValueError('sqlite_do_etl() is only valid for built-in datapackages')
        for resource in self.package_def['resources']:
            if 'derivation_sql_path' in resource and do_etl_tables:
                sql = self.package_filename.get_data_str(resource['derivation_sql_path'])
                conn.execute('DELETE FROM %s' % sql_identifier(resource['name']),)
                logger.debug('Running table-generating ETL for %s...' % sql_identifier(resource['name']))
                conn.execute(sql)
                logger.info('ETL complete for %s' % sql_identifier(resource['name']))
        for resource in self.package_def['resources']:
            for column in resource['schema']['fields']:
                if 'derivation_sql_path' in column and do_etl_columns:
                    sql = self.package_filename.get_data_str(column['derivation_sql_path'])
                    conn.execute('UPDATE %s SET %s = NULL' % (sql_identifier(resource['name']), sql_identifier(column['name'])))
                    logger.debug('Running column-generating ETL for %s.%s...' % (sql_identifier(resource['name']), sql_identifier(column['name'])))
                    conn.execute(sql)
                    logger.info('ETL complete for %s.%s' % (sql_identifier(resource['name']), sql_identifier(column['name'])))

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
            'text[]': 'json',
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
