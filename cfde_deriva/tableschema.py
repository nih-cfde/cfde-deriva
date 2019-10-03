#!/usr/bin/python3

"""Translate basic Frictionless Table-Schema table definitions to Deriva."""

import os
import sys
import json
from deriva.core.ermrest_model import builtin_types, Table, Column, Key, ForeignKey
from deriva.core.ermrest_config import tag

schema_tag = 'tag:isrd.isi.edu,2019:table-schema-leftovers'
resource_tag = 'tag:isrd.isi.edu,2019:table-resource'

# translate table-schema definitions into deriva definitions
schema_name = 'CFDE'

def make_type(type, format):
    """Choose appropriate ERMrest column types..."""
    if type == "string":
        return builtin_types.text
    if type == "datetime":
        return builtin_types.timestamptz
    if type == "date":
        return builtin_types.date
    if type == "integer":
        return builtin_types.int8
    if type == "number":
        return builtin_types.float8
    if type == "list":
        # assume a list is a list of strings for now...
        return builtin_types["text[]"]
    raise ValueError('no mapping defined yet for type=%s format=%s' % (type, format))

def make_column(cdef):
    cdef = dict(cdef)
    constraints = cdef.get("constraints", {})
    cdef_name = cdef.pop("name")
    nullok = not constraints.pop("required", False)
    description = cdef.pop("description", None)
    return Column.define(
        cdef_name,
        make_type(
            cdef.get("type", "string"),
            cdef.get("format", "default"),
        ),
        nullok=nullok,
        comment=description,
        annotations={
            schema_tag: cdef,
        }
    )

def make_key(tname, cols):
    return Key.define(
        cols,
        constraint_names=[ [schema_name, "%s_%s_key" % (tname, "_".join(cols))] ],
    )

def make_fkey(tname, fkdef):
    fkcols = fkdef.pop("fields")
    fkcols = [fkcols] if isinstance(fkcols, str) else fkcols
    reference = fkdef.pop("reference")
    pktable = reference.pop("resource")
    pktable = tname if pktable == "" else pktable
    to_name = reference.pop("title", None)
    pkcols = reference.pop("fields")
    pkcols = [pkcols] if isinstance(pkcols, str) else pkcols
    annotations = {
        schema_tag: fkdef,
    }
    if to_name is not None:
        annotations[tag.foreign_key] = {"to_name": to_name}
    return ForeignKey.define(
        fkcols,
        schema_name,
        pktable,
        pkcols,
        constraint_names=[ [schema_name, "%s_%s_fkey" % (tname, "_".join(fkcols))] ],
        annotations=annotations
    )

def make_table(tdef):
    provide_system = not (os.getenv('SKIP_SYSTEM_COLUMNS', 'false').lower() == 'true')
    tname = tdef["name"]
    if provide_system:
        system_columns = Table.system_column_defs()
        system_keys = Table.system_key_defs()
        # customize the system column templates...
        for col in system_columns:
            cname = col['name']
            col['comment'] = {
                'RID': 'Immutable record identifier (system-generated).',
                'RCT': 'Record creation time (system-generated).',
                'RMT': 'Record last-modification time (system-generated).',
                'RCB': 'Record created by (system-generated).',
                'RMB': 'Record last-modified by (system-generated).',
            }[cname]
            display_names = {
                'RCT': 'Creation Time',
                'RMT': 'Modification Time',
                'RCB': 'Created By',
                'RMB': 'Modified By',
            }
            if cname != 'RID':
                col['annotations'] = {tag.display: {"name": display_names[cname]}}
        system_fkeys = [
            ForeignKey.define(
                [cname], 'public', 'ERMrest_Client', ['ID'],
                constraint_names=[['CFDE', '%s_%s_fkey' % (tname, cname)]]
            )
            for cname in ['RCB', 'RMB']
        ]
    else:
        system_columns = []
        system_keys = []
        system_fkeys = []
    tcomment = tdef.get("description")
    tdef_resource = tdef
    tdef = tdef_resource.pop("schema")
    keys = []
    keysets = set()
    pk = tdef.pop("primaryKey", None)
    if isinstance(pk, str):
        pk = [pk]
    if isinstance(pk, list):
        keys.append(make_key(tname, pk))
        keysets.add(frozenset(pk))
    tdef_fields = tdef.pop("fields", None)
    for cdef in tdef_fields:
        if cdef.get("constraints", {}).pop("unique", False):
            kcols = [cdef["name"]]
            if frozenset(kcols) not in keysets:
                keys.append(make_key(tname, kcols))
                keysets.add(frozenset(kcols))
    tdef_fkeys = tdef.pop("foreignKeys", [])
    title = tdef_resource.get("title", None)
    annotations = {
        resource_tag: tdef_resource,
        schema_tag: tdef,
    }
    if title is not None:
        annotations[tag.display] = {"name": title}
    return Table.define(
        tname,
        column_defs=system_columns + [
            make_column(cdef)
            for cdef in tdef_fields
        ],
        key_defs=system_keys + keys,
        fkey_defs=system_fkeys + [
            make_fkey(tname, fkdef)
            for fkdef in tdef_fkeys
        ],
        comment=tcomment,
        provide_system=False,
        annotations=annotations,
    )

def make_model(tableschema):
    resources = tableschema['resources']
    return {
        "schemas": {
            schema_name: {
                "schema_name": schema_name,
                "tables": {
                    tdef["name"]: make_table(tdef)
                    for tdef in resources
                }
            }
        }
    }

def main():
    """Translate basic Frictionless Table-Schema table definitions to Deriva.

    - Reads table-schema JSON on standard input
    - Writes deriva schema JSON on standard output

    The output JSON is suitable for POST to an /ermrest/catalog/N/schema
    resource on a fresh, empty catalog.

    Example:

    python3 -m cfde_deriva.tableschema < table-schema/cfde-core-model.json

    Optionally:

    run with SKIP_SYSTEM_COLUMNS=true to suppress generation of ERMrest
    system columns RID,RCT,RCB,RMT,RMB for each table.

"""
    json.dump(make_model(json.load(sys.stdin)), sys.stdout, indent=2)
    return 0

if __name__ == '__main__':
    exit(main())
