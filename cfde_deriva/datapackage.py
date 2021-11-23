
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
import sqlite3

from deriva.core import DerivaServer, get_credential, urlquote, topo_sorted, tag, DEFAULT_SESSION_CONFIG
from deriva.core.ermrest_model import Model, Table, Column, Key, ForeignKey, builtin_types
import requests

from . import tableschema
from .configs import submission, portal_prep, portal, registry
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

submission_schema_json = _PackageDataName(submission, 'c2m2-datapackage.json')
portal_prep_schema_json = _PackageDataName(portal_prep, 'cfde-portal-prep.json')
portal_schema_json = _PackageDataName(portal, 'cfde-portal.json')
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

def make_session_config():
    """Return custom requests session_config for our data submission scenarios
    """
    session_config = DEFAULT_SESSION_CONFIG.copy()
    session_config.update({
        # our PUT/POST to ermrest is idempotent
        "allow_retry_on_all_methods": True,
        # do more retries before aborting
        "retry_read": 8,
        "retry_connect": 5,
        # increase delay factor * 2**(n-1) for Nth retry
        "retry_backoff_factor": 5,
    })
    return session_config

def tnames_topo_sorted(tables):
    """Return table names from model topologically sorted to put dependant after references tables.

    :param tables: dict-like map of table instances
    """
    def target_tname(fkey):
        return fkey.referenced_columns[0].table.name
    return topo_sorted({
        table.name: [
            target_tname(fkey)
            for fkey in table.foreign_keys
            if target_tname(fkey) != table.name and target_tname(fkey) in tables
        ]
        for table in tables.values()
    })

class CfdeDataPackage (object):
    # the translation stores frictionless table resource metadata under this annotation
    resource_tag = 'tag:isrd.isi.edu,2019:table-resource'
    # the translation leaves extranneous table-schema stuff under this annotation
    # (i.e. stuff that perhaps wasn't translated to deriva equivalents)
    schema_tag = 'tag:isrd.isi.edu,2019:table-schema-leftovers'

    batch_size = 4000 # how may rows we'll send to ermrest

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

        if not set(self.model_doc['schemas']).issubset({'CFDE', 'public'}):
            raise ValueError('Unexpected schema set in data package: %s' % (set(self.model_doc['schemas']),))

    def set_catalog(self, catalog, registry=None):
        self.catalog = catalog
        self.configurator.set_catalog(catalog, registry)
        self.get_model()
        self.cat_has_history_control = catalog.get('/').json().get("features", {}).get("history_control", False)

    def get_model(self):
        self.cat_model_root = self.catalog.getCatalogModel()
        self.cat_cfde_schema = self.cat_model_root.schemas.get('CFDE')

    def _compare_model_docs(self, candidate, absent_table_ok=True, absent_column_ok=True, absent_nonnull_ok=True, extra_table_ok=False, extra_column_ok=False, extra_fkey_ok=False, extra_nonnull_ok=True):
        """General-purpose model comparison to serve validation functions.

        :param candidate: A CfdeDatapackage instance being evaluated with self as baseline.
        :param absent_table_ok: Whether candidate is allowed to omit tables.
        :param absent_column_ok: Whether candidate is allowed to omit non-critical columns.
        :param extra_table_ok: Whether candidate is allowed to include tables.
        :param extra_column_ok: Whether candidate is allowed to include non-critical columns.
        :param extra_fkey_ok: Whether candidate is allowed to include foreign keys on extra, non-critical columns.
        :param absent_nonnull_ok: Whether candidate is allowed to omit a non-null constraint.
        :param extra_nonnull_ok: Whether candidate is allowed to include extra non-null constraints.

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
                'sex',
                'race',
                'ethnicity',
                'disease_association_type',
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
            missing_nonnull_cnames = [
                cname for cname in missing_cnames
                if (not baseline_table.columns[cname].nullok) and (baseline_table.columns[cname].default is None)
            ]
            extra_nonnull_cnames = [ cname for cname in extra_cnames if not candidate_table.columns[cname].nullok ]
            if missing_cnames and not absent_column_ok:
                raise IncompatibleDatapackageModel(
                    'Missing columns in resource %s: %s' % (tname, ','.join(missing_cnames),)
                )
            if missing_nonnull_cnames and not absent_nonnull_ok:
                raise IncompatibleDatapackageModel(
                    'Missing non-nullable columns in resource %s: %s' % (tname, ','.join(missing_nonnull_cnames),)
                )
            if extra_cnames and not extra_column_ok:
                raise IncompatibleDatapackageModel(
                    'Extra columns in resource %s: %s' % (tname, ','.join(extra_cnames),)
                )
            if extra_nonnull_cnames and not extra_nonnull_ok:
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
                if not baseline_col.nullok and candidate_col.nullok and not extra_nonnull_ok:
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
        """Provision model idempotently in self.catalog"""
        need_parts = []

        # create empty schemas if missing
        for nschema in self.doc_model_root.schemas.values():
            if nschema.name not in self.cat_model_root.schemas:
                sdoc = nschema.prejson()
                del sdoc['tables']
                sdoc.update({"schema_name": nschema.name})
                need_parts.append(sdoc)

        if need_parts:
            self.catalog.post('/schema', json=need_parts).raise_for_status()
            logger.info("Added empty schemas %r" % ([ sdoc["schema_name"] for sdoc in need_parts ]))
            need_parts.clear()
            self.get_model()

        # create tables if missing, but stripped of fkeys and acl-bindings which might be incoherent
        for nschema in self.doc_model_root.schemas.values():
            schema = self.cat_model_root.schemas[nschema.name]
            for ntable in nschema.tables.values():
                if ntable.name not in schema.tables:
                    tdoc = ntable.prejson()
                    tdoc.pop("foreign_keys")
                    tdoc.pop("acl_bindings")
                    for cdoc in tdoc["column_definitions"]:
                        cdoc.pop("acl_bindings")
                    tdoc.update({"schema_name": nschema.name, "table_name": ntable.name})
                    need_parts.append(tdoc)

        if need_parts:
            self.catalog.post('/schema', json=need_parts).raise_for_status()
            logger.info("Added base tables %r" % ([ (tdoc["schema_name"], tdoc["table_name"]) for tdoc in need_parts ]))
            need_parts.clear()
            self.get_model()

        # create and/or upgrade columns, but stripped of acl-bindings which might be incoherent
        for nschema in self.doc_model_root.schemas.values():
            schema = self.cat_model_root.schemas[nschema.name]
            for ntable in nschema.tables.values():
                table = schema.tables[ntable.name]
                for ncolumn in ntable.columns:
                    if ncolumn.name in {'nid', 'RID', 'RCT', 'RMT', 'RCB', 'RMB'}:
                        # don't consider patching system nor special CFDE columns...
                        pass
                    elif ncolumn.name not in table.columns.elements:
                        cdoc = ncolumn.prejson()
                        cdoc.pop('acl_bindings')
                        
                        # HACK: prepare to allow registry upgrades w/ new non-null columns
                        cdoc_orig = dict(cdoc)
                        upgrade_data_path = cdoc.get('annotations', {}).get(self.schema_tag, {}).get('schema_upgrade_data_path')
                        if upgrade_data_path and isinstance(self.package_filename, _PackageDataName):
                            cdoc['nullok'] = True

                        self.catalog.post(
                            '/schema/%s/table/%s/column' % (urlquote(nschema.name), urlquote(ntable.name)),
                            json=cdoc
                        ).raise_for_status()
                        logger.info("Added column %s.%s.%s" % (nschema.name, ntable.name, ncolumn.name))

                        # apply built-in upgrade data to new column
                        if upgrade_data_path and isinstance(self.package_filename, _PackageDataName):
                            with self.package_filename.get_data_stringio(upgrade_data_path) as upgrade_tsv:
                                reader = csv.reader(upgrade_tsv, delimiter='\t')
                                header = next(reader)
                                # we expect TSV to have key column(s) and then this new target column
                                if header[-1] != ncolumn.name:
                                    raise ValueError('New column %s.%s.%s upgrade data final column %r should be %r' % (
                                        nschema.name, ntable.name, ncolumn.name, header[-1], ncolumn.name,
                                    ))
                                for cname in header[0:-1]:
                                    if cname not in table.column_definitions.elements:
                                        raise ValueError('Unexpected column %s in new column %s.%s.%s upgrade data' % (
                                            cname, nschema.name, ntable.name, ncolumn.name,
                                        ))
                                # allow upgrade data to include extra rows not present in target catalog
                                # e.g. for single source to work on dev/staging/prod VPC w/ data variations
                                def key_exists(row):
                                    r = self.catalog.get(
                                        '/entity/%s:%s/%s' % (
                                            urlquote(nschema.name),
                                            urlquote(ntable.name),
                                            '/'.join([ '%s=%s' % (urlquote(cname), urlquote(value)) for cname, value in zip(header[0:-1], row[0:-1]) ]),
                                        )
                                    )
                                    return len(r.json()) > 0
                                upgrade_data = [
                                    dict(zip(header, row))
                                    for row in reader
                                    if key_exists(row)
                                ]
                                self.catalog.put(
                                    '/attributegroup/%s:%s/%s;%s' % (
                                        urlquote(nschema.name),
                                        urlquote(ntable.name),
                                        ','.join([ urlquote(cname) for cname in header[0:-1] ]),
                                        urlquote(header[-1]),
                                    ),
                                    json=upgrade_data,
                                )
                                logger.info("Applied new column %s.%s.%s upgrade data" % (nschema.name, ntable.name, ncolumn.name))
                                if cdoc_orig.get('nullok', True) is False:
                                    self.catalog.put(
                                        '/schema/%s/table/%s/column/%s' % (
                                            urlquote(nschema.name),
                                            urlquote(ntable.name),
                                            urlquote(ncolumn.name),
                                        ),
                                        json={'nullok': False},
                                    )
                                    logger.info("Altered new column %s.%s.%s to nullok=false" % (nschema.name, ntable.name, ncolumn.name))
                    else:
                        # consider column upgrade
                        column = table.columns[ncolumn.name]
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
                            column.alter(**change)
                            logger.info("Altered column %s.%s.%s with changes %r" % (
                                nschema.name, ntable.name, ncolumn.name, change,
                            ))
        self.get_model()

        # create and/or upgrade keys
        for nschema in self.doc_model_root.schemas.values():
            schema = self.cat_model_root.schemas[nschema.name]
            for ntable in nschema.tables.values():
                table = schema.tables[ntable.name]
                for nkey in ntable.keys:
                    cnames = { c.name for c in nkey.unique_columns }
                    key = table.key_by_columns(cnames, raise_nomatch=False)
                    if key is None:
                        key = table.create_key(nkey.prejson())
                        logger.info("Created key %s" % (key.constraint_name,))
        self.get_model()

        # create and/or upgrade fkeys, stripping acl-bindings which may be incoherent
        for nschema in self.doc_model_root.schemas.values():
            schema = self.cat_model_root.schemas[nschema.name]
            for ntable in nschema.tables.values():
                table = schema.tables[ntable.name]
                for nfkey in ntable.foreign_keys:
                    if { c.name for c in nfkey.foreign_key_columns }.issubset({'RCB', 'RMB'}) and not nfkey.annotations.get(tag.noprune, False):
                        # skip built-in RCB/RMB fkeys we don't want
                        continue
                    pktable = schema.model.table(nfkey.pk_table.schema.name, nfkey.pk_table.name)
                    cmap = {
                        table.columns[nfkc.name]: pktable.columns[npkc.name]
                        for nfkc, npkc in nfkey.column_map.items()
                    }
                    fkey = table.fkey_by_column_map(cmap, raise_nomatch=False)
                    if fkey is None:
                        fkdoc = nfkey.prejson()
                        fkdoc["foreign_key_columns"][0].update({"schema_name": nschema.name, "table_name": ntable.name})
                        fkdoc.pop("acl_bindings")
                        need_parts.append(fkdoc)
                    else:
                        # consider fkey upgrade
                        change = {}
                        if nfkey.constraint_name != fkey.constraint_name:
                            change['constraint_name'] = nfkey.constraint_name
                        if nfkey.on_delete != fkey.on_delete:
                            change['on_delete'] = nfkey.on_delete
                        if nfkey.on_update != fkey.on_update:
                            change['on_update'] = nfkey.on_update
                        if change:
                            fkey.alter(**change)
                            logger.info("Altered foreign key %s.%s with changes %r" % (
                                nschema.name, fkey.constraint_name, change
                            ))

        if need_parts:
            self.catalog.post('/schema', json=need_parts).raise_for_status()
            logger.info("Added foreign-keys %r" % ([ tuple(fkdoc["names"][0]) for fkdoc in need_parts ]))
            need_parts.clear()

        self.get_model()

        # restore acl-bindings we stripped earlier
        self.apply_custom_config()
        logger.info('Provisioned model in catalog %s' % self.catalog.get_server_uri())

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
                table.annotations.clear()
                table.annotations.update(doc_table.annotations)
                table.acls.clear()
                table.acl_bindings.clear()
                table.acls.update(doc_table.acls)
                table.acl_bindings.update(doc_table.acl_bindings)
                for column in table.columns:
                    doc_column = doc_table.columns.elements.get(column.name)
                    if doc_column is None:
                        continue
                    column.annotations.clear()
                    column.annotations.update(doc_column.annotations)
                    column.acls.clear()
                    column.acl_bindings.clear()
                    column.acls.update(doc_column.acls)
                    column.acl_bindings.update(doc_column.acl_bindings)
                if True or table.is_association():
                    for cname in {'RCB', 'RMB'}:
                        if cname not in table.columns.elements:
                            continue
                        for fkey in table.fkeys_by_columns([cname], raise_nomatch=False):
                            if fkey.annotations.get(tag.noprune, False):
                                continue
                            logger.info('Dropping %s' % fkey.uri_path)
                            fkey.drop()
                    for fkey in table.foreign_keys:
                        doc_fkey = doc_table.foreign_keys.elements.get( (doc_schema, fkey.name[1]) )
                        if doc_fkey is None:
                            continue
                        fkey.annotations.clear()
                        fkey.annotations.update(doc_fkey.annotations)
                        fkey.acls.clear()
                        fkey.acl_bindings.clear()
                        fkey.acls.update(doc_fkey.acls)
                        fkey.acl_bindings.update(doc_fkey.acl_bindings)

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
                if col.name not in {"nid", "RID", "RCT", "RMT", "RCB", "RMB"}
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
        missingValues = set(table.annotations.get(cls.schema_tag, {}).get("missingValues", []))

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
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        return tnames_topo_sorted({
            tname: table
            for tname, table in source_schema.tables.items()
            if tname in tables_doc
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

            table = self.cat_cfde_schema.tables[resource['name']]
            if 'nid' in table.columns.elements:
                kcol = 'nid'
            elif 'RID' in table.columns.elements:
                kcol = 'RID'
            else:
                raise ValueError('Cannot dump data for table %s with neither "nid" nor "RID" key columns!' % (table.name,))

            def get_data():
                r = self.catalog.get(
                    '/entity/CFDE:%s@sort(%s)?limit=%d' % (
                        urlquote(resource['name']),
                        kcol,
                        self.batch_size,
                    ))
                rows = r.json()
                yield rows

                while rows:
                    last = rows[-1][kcol]
                    r = self.catalog.get(
                        '/entity/CFDE:%s@sort(%s)@after(%s)?limit=%d' % (
                            urlquote(resource['name']),
                            kcol,
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

    def load_data_files(self, onconflict='abort'):
        """Load tabular data from files into catalog table.

        :param onconflict: ERMrest onconflict query parameter to emulate (default abort)
        """
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
                    path = resource["path"]
                    if self.package_filename is registry_schema_json and path.startswith("/submission/"):
                        # allow absolute path to reference submission package
                        path = path[len("/submission/"):]
                        return submission_schema_json.get_data_stringio(path)
                    else:
                        return self.package_filename.get_data_stringio(resource["path"])
                else:
                    fname = "%s/%s" % (os.path.dirname(self.package_filename), resource["path"])
                    return open(fname, 'r')
            try:
                with open_package() as f:
                    # translate TSV to python dicts
                    reader = csv.reader(f, delimiter="\t")
                    header = next(reader)
                    missing = set(table.annotations.get(self.schema_tag, {}).get("missingValues", []))
                    for cname in header:
                        if cname not in table.column_definitions.elements:
                            raise ValueError("header column %s not found in table %s" % (cname, table.name))
                    if onconflict == 'update':
                        def has_key(cols):
                            if set(cols).issubset(set(header)):
                                return table.key_by_columns(cols, raise_nomatch=False) is not None
                            return False
                            
                        keycols = None
                        if has_key(('id',)):
                            keycols = ('id',)
                        elif has_key(('id_namespace', 'local_id')):
                            keycols = ('id_namespace', 'local_id')
                        else:
                            for key in table.keys:
                                if has_key([ c.name for c in key.unique_columns ]):
                                    keycols = [ c.name for c in key.unique_columns ]
                                    break
                            if keycols is None:
                                raise NotImplementedError('Table %s TSV columns %r do not cover a key' % (table.name, header))

                        updcols = [ cname for cname in header if cname not in keycols ]
                        update_sig = ','.join([ urlquote(cname) for cname in keycols ])
                        if updcols:
                            update_sig = ';'.join([ update_sig, ','.join([ urlquote(cname) for cname in updcols ])])
                        else:
                            update_sig = False
                    # Largest known CFDE ingest has file with >5m rows
                    batch = []
                    def store_batch():
                        def row_to_json(row):
                            row = [ None if v in missing else v for v in row ]
                            res = dict(zip(header, row))
                            for cname in header:
                                if table.columns[cname].type.typename in ('text[]', 'json', 'jsonb'):
                                    res[cname] = json.loads(res[cname]) if res[cname] is not None else None
                            return res
                        payload = [ row_to_json(row) for row in batch ]
                        if onconflict == 'update':
                            # emulate as two passes
                            rj = self.catalog.post(
                                "/entity/CFDE:%s?onconflict=skip" % (urlquote(table.name),),
                                json=payload
                            ).json()
                            if update_sig:
                                self.catalog.put(
                                    "/attributegroup/CFDE:%s/%s" % (urlquote(table.name), update_sig),
                                    json=payload
                                ).json() # drain response body...
                        else:
                            entity_url = "/entity/CFDE:%s?onconflict=%s" % (urlquote(table.name), urlquote(onconflict))
                            rj = self.catalog.post(
                                entity_url,
                                json=payload
                            ).json()
                        logger.info("Batch of rows for %s loaded" % table.name)
                        skipped = len(batch) - len(rj)
                        if skipped:
                            logger.debug("Batch contained %d rows with existing keys" % skipped)

                    for raw_row in reader:
                        # Collect full batch, then insert at once
                        batch.append(raw_row)
                        if len(batch) >= self.batch_size:
                            try:
                                store_batch()
                            except Exception as e:
                                logger.error("Table %s data load FAILED from "
                                             "%s: %s" % (table.name, self.package_filename, e))
                                raise
                            else:
                                batch.clear()
                    # After reader exhausted, ingest final batch
                    if len(batch) > 0:
                        try:
                            store_batch()
                        except Exception as e:
                            logger.error("Table %s data load FAILED from "
                                         "%s: %s" % (table.name, self.package_filename, e))
                            raise
                    logger.info("All data for table %s loaded from %s." % (table.name, self.package_filename))
            except UnicodeDecodeError as e:
                raise InvalidDatapackage('Resource file "%s" is not valid UTF-8 data: %s' % (resource["path"], e))

    def sqlite_import_data_files(self, conn, onconflict='abort', table_error_callback=None, progress=None):
        """Load tabular data from files into sqlite table.

        :param conn: Existing sqlite3 connection to use as data destination.
        :param onconflict: ERMrest onconflict query parameter to emulate (default abort)
        :param table_error_callback: Optional callback to signal table loading errors, lambda rname, rpath, msg: ...
        :param progress: Optional, mutable progress/restart-marker dictionary
        """
        if progress is None:
            progress = dict()
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        for tname in self.data_tnames_topo_sorted(source_schema=self.doc_cfde_schema):
            # we are doing a clean load of data in fkey dependency order
            table = self.doc_cfde_schema.tables[tname]
            resource = tables_doc[tname]["annotations"].get(self.resource_tag, {})
            if "path" not in resource:
                continue
            if progress.get(tname):
                logger.info("Skipping sqlite import for %s due to existing progress marker" % tname)
                continue
            logger.debug('Importing table "%s" into sqlite...' % tname)
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
                    missing = set(table.annotations.get(self.schema_tag, {}).get("missingValues", []))
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
                        try:
                            conn.execute(sql)
                        except sqlite3.IntegrityError as e:
                            msg = str(e)
                            if msg.find('NOT NULL constraint failed') >= 0:
                                # find offending row to give better error details
                                error_cname = msg.split('.')[-1] # HACK works because of simple C2M2 column naming
                                try:
                                    pos = header.index(error_cname)
                                    for row in batch:
                                        if row[pos] is None or row[pos] in missing:
                                            raise InvalidDatapackage('Resource file "%s" missing required value for column %r, row %r' % (
                                                resource["path"],
                                                error_cname,
                                                dict(zip(header, row)),
                                            ))
                                except ValueError:
                                    raise InvalidDatapackage('Resource file "%s" does not supply required column %r' % (
                                        resource["path"],
                                        error_cname,
                                    ))
                            # re-raise if we don't have a better idea
                            raise
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
                    progress[tname] = True
                    logger.info("All data for table %s loaded from %s." % (table.name, self.package_filename))
            except UnicodeDecodeError as e:
                if table_error_callback:
                    table_error_callback(resource["name"], resource["path"], str(e))
                raise InvalidDatapackage('Resource file "%s" is not valid UTF-8 data: %s' % (resource["path"], e))

    def check_sqlite_tables(self, conn, source=None, table_error_callback=None, tablenames=None, progress=None):
        """Validate tabular data from sqlite table according to model.

        :param conn: Existing sqlite3 connection to use as data source.
        :param source: Another CfdeDatapackage representing source data, otherwise use self.
        :param table_error_callback: Optional callback to signal error for one table, lambda tname, tpath, msg: ...
        :param tablenames: Optional set of tablenames to check (default None means check all tables)
        :param progress: Optional, mutable progress/restart-marker dictionary
        """
        if progress is None:
            progress = dict()
        if not self.package_filename in (submission_schema_json, portal_schema_json):
            raise ValueError('check_sqlite_tables() is only valid for built-in datapackages')
        if source is None:
            source = self
        tables_doc = source.model_doc['schemas']['CFDE']['tables']
        if tablenames is None:
            tablenames = set(tables_doc.keys())

        # collect errors and defer exception so we can give more detailed feedback
        errors_by_tname = {}
        first_error_mesg = None

        cur = None
        try:
            cur = conn.cursor()
            for tname in self.data_tnames_topo_sorted(source_schema=self.doc_cfde_schema):
                if tname not in tablenames:
                    continue

                table = self.doc_cfde_schema.tables[tname]
                sql_checks = self.doc_cfde_schema.tables[tname].annotations.get(self.resource_tag, {}).get('check_sql_paths', [])
                skip_checks = json.loads(os.getenv('CFDE_SKIP_SQL_CHECKS', '{}'))
                resource = tables_doc[tname]["annotations"].get(self.resource_tag, {})

                progress.setdefault(tname, {})

                for path in sql_checks:
                    if skip_checks is True or skip_checks.get(path[0:-4]):
                        logger.info('Skipping custom SQL check %r for table %r due to CFDE_SKIP_SQL_CHECKS env. var.' % (path, tname))
                        continue
                    progress[tname].setdefault(path, None)
                    if progress[tname][path] is not None:
                        logger.info('Skipping custom SQL check %r for table %r due to progress marker' % (path, tname))
                        continue
                    logger.info('Running custom SQL check %r for table %r' % (path, tname,))
                    sql = self.package_filename.get_data_str(path)
                    cur.execute(sql)
                    row = cur.fetchone()
                    if row:
                        description, count, example_nid, example_data = row
                        mesg = '%s in row %s %s%s' % (
                            description,
                            example_nid,
                            example_data,
                            (' (and %s others...)' % (count - 1)) if count > 1 else '',
                        )
                        errors_by_tname.setdefault(tname, []).append(mesg)
                        if first_error_mesg is None:
                            first_error_mesg = 'Table %s %s' % (tname, mesg)
                        logger.error('custom SQL check %r for table %r ERROR: %s' % (path, tname, mesg))
                        progress[tname][path] = False
                    else:
                        logger.info('custom SQL check %r for table %r OK' % (path, tname))
                        progress[tname][path] = True

                if table.foreign_keys:
                    logger.info('Checking foreign keys for table %r' % (tname,))
                for fkey in table.foreign_keys:
                    if fkey.pk_table.schema.name != table.schema.name:
                        # only consider single schema in this sqite db
                        progress[tname][fkey.constraint_name] = 'skip'
                        logger.info("Skipping foreign key %r which references outside schema %r" % (fkey.constraint_name, table.schema.name))
                        continue
                    progress[tname].setdefault(fkey.constraint_name, None)
                    if progress[tname][fkey.constraint_name] is not None:
                        logger.info("Skipping foreign key %r due to progress marker" % (fkey.constraint_name,))
                        continue
                    col_map = list(fkey.column_map.items())
                    sql = """
WITH fk AS (
  SELECT fk.nid, %(fkcols)s
  FROM %(fk_table)s fk
  LEFT JOIN %(pk_table)s pk ON ( %(on)s )
  WHERE (%(nonnulls)s) AND (%(nulls)s)
)
SELECT
  (SELECT count(*) FROM fk) AS num_errors,
  nid,
  %(fkcols)s
FROM fk
ORDER BY nid
LIMIT 1;
""" % {
    "fk_table": sql_identifier(table.name),
    "fkcols": ", ".join([
        'fk.%s' % (sql_identifier(fkc.name),)
        for fkc, pkc in col_map
    ]),
    "pk_table": sql_identifier(fkey.pk_table.name),
    "on": " AND ".join([
        'fk.%s = pk.%s' % (sql_identifier(fkc.name), sql_identifier(pkc.name))
        for fkc, pkc in col_map
    ]),
    "nonnulls": " AND ".join([
        "fk.%s IS NOT NULL" % (sql_identifier(fkc.name),)
        for fkc, pkc in col_map
    ]),
    "nulls": " OR ".join([
        "pk.%s IS NULL" % (sql_identifier(pkc.name),)
        for fkc, pkc in col_map
    ]),
}
                    cur.execute(sql)
                    row = cur.fetchone()
                    if row:
                        mesg = 'foreign key %r in row %s%s not present in referenced table %r' % (
                            dict(zip([ fkc.name for fkc, pkc in col_map ], row[2:])), # first bad fkey
                            row[1], # nid for first bad row
                            (' (and %s others...)' % (row[0] - 1)) if row[0] > 1 else '',
                            fkey.pk_table.name,
                        )
                        errors_by_tname.setdefault(tname, []).append(mesg)
                        if first_error_mesg is None:
                            first_error_mesg = 'Table %s %s' % (tname, mesg)
                        logger.error('foreign key %r ERROR: %s' % (fkey.constraint_name, mesg))
                        progress[tname][fkey.constraint_name] = False
                    else:
                        logger.info("foreign key %r OK" % (fkey.constraint_name,))
                        progress[tname][fkey.constraint_name] = True

                if tname in errors_by_tname:
                    if table_error_callback:
                        table_error_callback(tname, resource.get('path'), '; '.join(errors_by_tname[tname]))

            if errors_by_tname:
                raise InvalidDatapackage('Errors found in %s tables %r. First error: %s' % (len(errors_by_tname), list(errors_by_tname), first_error_mesg))
        finally:
            if cur is not None:
                cur.close()

    def load_sqlite_tables(self, conn, onconflict='abort', table_done_callback=None, table_error_callback=None, tablenames=None, progress=None, table_queries={}, skip_cols={'RID', 'RCT', 'RMT', 'RCB', 'RMB'}):
        """Load tabular data from sqlite table into corresponding catalog table.

        :param conn: Existing sqlite3 connection to use as data source.
        :param onconflict: ERMrest onconflict query parameter for entity POST (default 'abort')
        :param table_done_callback: Optional callback to signal completion of one table, lambda tname, tpath: ...
        :param table_error_callback: Optional callback to signal error for one table, lambda tname, tpath, msg: ...
        :param tablenames: Optional set of tablenames to load (default None means load all tables)
        :param progress: Optional, mutable progress/restart-marker dictionary
        :param table_queries: Optional, override source SQL query for specific table names
        :param skip_cols: Optiona, override set of column names to ignore (default ERMrest system columns)
        """
        if progress is None:
            progress = dict()
        if not self.package_filename is portal_schema_json:
            raise ValueError('load_sqlite_tables() is only valid for built-in portal datapackage')
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        if tablenames is None:
            tablenames = set(tables_doc.keys())
        cur = conn.cursor()
        for tname in self.data_tnames_topo_sorted(source_schema=self.doc_cfde_schema):
            # we are copying sqlite table content to catalog under same table name
            table = self.doc_cfde_schema.tables[tname]
            resource = tables_doc[tname]["annotations"].get(self.resource_tag, {})

            if tname not in tablenames:
                # skip tables excluded in caller-supplied tablenames list
                continue

            cur.execute("SELECT true FROM sqlite_master WHERE type = 'table' AND name = %s" % (sql_literal(tname),))
            found = cur.fetchone()
            if found is None:
                # skip tables that don't exist in sqlite
                continue

            logger.debug('Loading table "%s" from sqlite to catalog...' % tname)
            entity_url = "/entity/CFDE:%s?onconflict=%s" % (
                urlquote(table.name),
                {
                    'abort': 'abort',
                    'skip': 'skip',
                    'update': 'skip', # we will synthesize update below...
                }[onconflict],
            )

            cols = [ col for col in table.columns if col.name not in skip_cols ]
            colnames = [ col.name for col in cols ]
            # HACK: this ONLY works for our vocab-like tables keyed by 'id'
            update_url = "/attributegroup/CFDE:%s/id;%s" % (
                urlquote(table.name),
                ",".join([ cname for cname in colnames if cname != 'id']),
            )
            valfuncs = [
                # f(x) does json decoding or is identify func, depending on column def
                (lambda x: json.loads(x) if x is not None else x) if col.type.typename in ('text[]', 'json', 'jsonb') else lambda x: x
                for col in cols
            ]
            position = progress.get(tname, None)

            if position is not None:
                logger.info("Restarting after %r due to existing restart marker" % (position,))

            def get_batch(cur):
                nonlocal position
                sql = 'SELECT %(cols)s FROM %(table)s %(where)s ORDER BY "nid" ASC LIMIT %(batchsize)s' % {
                    'cols': ', '.join([ "nid" ] + [ sql_identifier(cname) for cname in colnames ]),
                    'table': table_queries.get(table.name, sql_identifier(table.name)),
                    'where': '' if position is None else ('WHERE "nid" > %s' % sql_literal(position)),
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
                existing = None
                if onconflict == 'update':
                    # fetch a local copy of registered terms for use below
                    eposition = None
                    def get_existing_batch():
                        nonlocal eposition
                        r = self.catalog.get(
                            "/attribute/CFDE:%s/%s@sort(id)%s?limit=%d" % (
                                urlquote(table.name),
                                ",".join([ urlquote(cname) for cname in colnames ]),
                                ("@after(%s)" % urlquote(eposition)) if eposition is not None else "",
                                self.batch_size,
                            ))
                        batch = r.json()
                        if batch:
                            eposition = batch[-1]['id']
                        return batch

                    existing = {}
                    batch = get_existing_batch()
                    while batch:
                        existing.update({
                            row['id']: row
                            for row in batch
                        })
                        batch = get_existing_batch()
                    logger.debug("Retrieved local copy of existing table %s with %d rows" % (table.name, len(existing)))

                nrows = 0
                for batch in get_batches(cur):
                    marker = batch[-1][0]
                    orig_batch = [
                        # generate per-row dict { colname: f(x), ... } with transcoded row values
                        dict(zip( colnames, [ f(x) for f, x in zip (valfuncs, row[1:]) ]))
                        for row in batch
                    ]

                    if onconflict == 'update':
                        # only need to POST rows that don't exist
                        batch = [ row for row in orig_batch if row['id'] not in existing ]
                    else:
                        batch = orig_batch

                    blen = len(batch)
                    if blen:
                        result = self.catalog.post(entity_url, json=batch).json()
                        logger.debug("POST /entity/ sent for %d new rows" % blen)

                    if onconflict == 'update':
                        def needs_update(row):
                            erow = existing.get(row['id'])
                            if erow is None:
                                return False
                            for cname in colnames:
                                if row[cname] != erow[cname]:
                                    return True
                            return False
                        # only update rows that show differences
                        batch = [ row for row in orig_batch if needs_update(row) ]
                        if batch:
                            r = self.catalog.put(update_url, json=batch).json()
                            logger.debug("PUT /attributegroup/ sent for %d existing rows" % len(batch))

                    progress[tname] = marker
                    nrows += blen
                    logger.info("Batch of %d rows loaded for %s (%d cumulative)" % (blen, table.name, nrows))

                logger.info("Table %s loaded %s rows." % (table.name, nrows,))
                if table_done_callback:
                    table_done_callback(table.name, resource.get("path", None))
            except Exception as e:
                logger.error("Error while loading data for table %s: %s" % (table.name, e))
                if table_error_callback:
                    table_error_callback(table.name, resource.get("path", None), str(e))
                raise

    def generate_resource_etl_sql(self, source_dp, source_sql_schema, resource):
        """Return SQL to perform ETL for resource/table

        :param source_dp: the source model, e.g. a submission datapackage
        :param source_sql_schema: the source schema as attached, e.g. 'submission'
        :param resource: the tabular resource in self.package_def
        """
        path = resource['derivation_sql_path']
        tname = resource["name"]
        dst_table = self.doc_cfde_schema.tables[tname]

        if path is not None:
            # use the custom SQL embedded in the package
            return self.package_filename.get_data_str(path)

        core_fact_assoc_arrays = {
            'project': 'projects',
            'dcc': 'dccs',
            'anatomy': 'anatomies',
            'disease': 'diseases',
            'substance': 'substances',
            'gene': 'genes',
            'sex': 'sexes',
            'race': 'races',
            'ethnicity': 'ethnicities',
            'subject_role': 'subject_roles',
            'subject_granularity': 'subject_granularities',
            'subject_species': 'subject_species',
            'ncbi_taxonomy': ('ncbi_taxon', 'ncbi_taxons'),
            'assay_type': 'assay_types',
            'analysis_type': 'analysis_types',
            'file_format': 'file_formats',
            'compression_format': 'compression_formats',
            'data_type': 'data_types',
            'mime_type': 'mime_types',
        }
        if tname.startswith('core_fact_') and tname[10:] in core_fact_assoc_arrays:
            # use built-in template to unpack core_fact arrays as associations
            acol = core_fact_assoc_arrays[tname[10:]]
            if isinstance(acol, tuple):
                # use custom vcol,acol pair for this assoc table
                vcol, acol = acol
            else:
                # vcol is encoded in assoc table name
                vcol = tname[10:]
            return """
INSERT INTO %(tname)s (core_fact, %(vcol)s)
SELECT s.nid, j.value
FROM core_fact s
JOIN json_each(s.%(acol)s) j
WHERE True;
""" % {
    "tname": tname,
    "vcol": vcol,
    "acol": acol,
}

        if tname in source_dp.doc_cfde_schema.tables:
            # build default SQL to copy with fkey translation
            # basic dst columns must exist in src table
            # every dst foreign-key must exist in src with matching constraint-name
            src_table = source_dp.doc_cfde_schema.tables[tname]

            # some reusable bits for templating
            parts = {
                "srcschema": sql_identifier(source_sql_schema),
                "tname": sql_identifier(tname),
            }

            # build a map of select expressions for dst columns governed by fkeys
            src_fkeys = { fkey.constraint_name for fkey in src_table.foreign_keys }
            selects = {
                fkc.name: "%(talias)s.%(pkcname)s" % {
                    "talias": sql_identifier(fkey.constraint_name),
                    "pkcname": sql_identifier(pkc.name),
                }
                for fkey in dst_table.foreign_keys
                for fkc, pkc in fkey.column_map.items()
                if fkey.constraint_name in src_fkeys
            }
            # add custom SQL expressions embedded in dst column def
            for field in resource['schema']['fields']:
                if "derivation_sql_select" in field:
                    selects[field["name"]] = field["derivation_sql_select"]
            # add regular column copies by default
            for c in dst_table.columns:
                if c.name in src_table.columns.elements:
                    selects.setdefault(c.name, "src.%s" % sql_identifier(c.name))

            def fkey_join(fkey):
                # generate join clause for a foreign key in source model
                fkparts = dict(**parts, **{
                    "pktname": sql_identifier(fkey.pk_table.name),
                    "talias": sql_identifier(fkey.constraint_name),
                })
                return "LEFT JOIN %(srcschema)s.%(pktname)s %(talias)s ON (%(conds)s)" % dict(
                    **fkparts,
                    **{
                        "conds": " AND ".join([
                            "src.%(fkcname)s = %(talias)s.%(pkcname)s" % dict(**fkparts, **{
                                "fkcname": sql_identifier(fkc.name),
                                "pkcname": sql_identifier(pkc.name),
                            })
                            for fkc, pkc in fkey.column_map.items()
                        ])
                    }
                )

            return """
INSERT INTO %(tname)s (
  %(dstcolumns)s
)
SELECT
  %(selects)s
FROM %(srcschema)s.%(tname)s src
%(joins)s;
""" % {
    "dstcolumns": ",\n  ".join([
        sql_identifier(c.name)
        for c in dst_table.columns
        if c.name in selects
    ]),
    "selects": ",\n  ".join([
        selects[c.name]
        for c in dst_table.columns
        if c.name in selects
    ]),
    "srcschema": sql_identifier(source_sql_schema),
    "tname": sql_identifier(tname),
    "joins": "\n".join([
        fkey_join(fkey)
        for fkey in src_table.foreign_keys
    ] + [
        field["derivation_sql_join"]
        for field in resource["schema"]["fields"]
        if "derivation_sql_join" in field
    ]),
}
        #
        else:
            raise NotImplementedError('cannot determine ETL SQL for resource %(name)s' % resource)

    def sqlite_do_etl(self, conn, source_dp, source_sql_schema, do_etl_tables=True, do_etl_columns=True, progress=None):
        """Do ETL described in self, e.g. a portal-prep datapackage

        :param source_dp: the source model, e.g. a submission datapackage
        :param source_sql_schema: the source schema as attached, e.g. 'submission'
        :param do_etl_tables: Do normal ETL table processing (default True)
        :param do_etl_columns: Do normal ETL column processing (default True)
        :param progress: Dictionary to mutate with progress/restart markers (default None)

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
        if progress is None:
            progress = dict()
        if not self.package_filename in { portal_prep_schema_json }:
            raise ValueError('sqlite_do_etl() is only valid for built-in datapackages')
        for resource in self.package_def['resources']:
            if 'derivation_sql_path' in resource and do_etl_tables:
                if progress.setdefault("tables", {}).get(resource["name"], False):
                    logger.info('Skipping table-generating ETL for %s due to restart marker' % resource['name'])
                    continue
                sql = self.generate_resource_etl_sql(source_dp, source_sql_schema, resource)
                conn.execute('DELETE FROM %s' % sql_identifier(resource['name']),)
                logger.debug('Running table-generating ETL for %s...' % sql_identifier(resource['name']))
                try:
                    conn.executescript(sql)
                except Exception as e:
                    logger.error('Failed to run table-generating ETL for %s: %s' % (sql_identifier(resource['name']), e))
                    raise
                progress["tables"][resource["name"]] = True
                logger.info('ETL complete for %s' % sql_identifier(resource['name']))
        tables_map = {
            resource['name']: resource
            for resource in self.package_def['resources']
        }
        for tname in self.data_tnames_topo_sorted(source_schema=self.doc_cfde_schema):
            resource = tables_map[tname]
            for column in resource['schema']['fields']:
                if 'derivation_sql_path' in column and do_etl_columns:
                    if progress.setdefault("columns", {}).setdefault(resource["name"], {}).get(column["name"], False):
                        logger.info('Skipping column-generating ETL for %s.%s due to restart marker' % (resource['name'], column['name']))
                        continue
                    sql = self.package_filename.get_data_str(column['derivation_sql_path'])
                    conn.execute('UPDATE %s SET %s = NULL' % (sql_identifier(resource['name']), sql_identifier(column['name'])))
                    logger.debug('Running column-generating ETL for %s.%s...' % (sql_identifier(resource['name']), sql_identifier(column['name'])))
                    try:
                        conn.executescript(sql)
                    except Exception as e:
                        logger.error('Failed to run column-generating ETL for %s.%s: %s' % (resource['name'], sql_identifier(column['name']), e))
                        raise
                    progress["columns"][resource["name"]][column["name"]] = True
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
            if col.name not in {'RID', 'RCT', 'RCB', 'RMT', 'RMB'}
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
        if col.name == 'nid':
            # special mapping to help with our ETL scripts...
            return '"nid" INTEGER PRIMARY KEY AUTOINCREMENT'
        parts = [ sql_identifier(col.name), self.type_sqlite_ddl(col.type) ]
        if not col.nullok:
            parts.append('NOT NULL')
        key = col.table.key_by_columns({col}, raise_nomatch=False)
        if key is not None:
            parts.append('UNIQUE')
        if col.default is not None:
            parts.append('DEFAULT %s' % sql_literal(col.default))
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
            'boolean': 'boolean',
            'text[]': 'json',
            'jsonb': 'json',
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
