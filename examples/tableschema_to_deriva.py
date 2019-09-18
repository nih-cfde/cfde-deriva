#!/usr/bin/python3

"""Translate basic Frictionless Table-Schema table definitions to Deriva.

- Reads table-schema JSON on standard input
- Writes deriva schema JSON on standard output

The output JSON is suitable for POST to an /ermrest/catalog/N/schema
resource on a fresh, empty catalog.

Example:

   cd cfde-deriva
   python3 examples/tableschema_to_deriva.py \
     < table-schema/cfde-core-model.json

Optionally:

   run with SKIP_SYSTEM_COLUMNS=true to suppress generation of ERMrest
   system columns RID,RCT,RCB,RMT,RMB for each table.

"""

import os
import sys
import json
from deriva.core.ermrest_model import builtin_types, Table, Column, Key, ForeignKey

schema_tag = 'tag:isrd.isi.edu,2019:table-schema-leftovers'
resource_tag = 'tag:isrd.isi.edu,2019:table-resource'

tableschema = json.load(sys.stdin)
resources = tableschema['resources']

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
    pkcols = reference.pop("fields")
    pkcols = [pkcols] if isinstance(pkcols, str) else pkcols
    return ForeignKey.define(
        fkcols,
        schema_name,
        pktable,
        pkcols,
        constraint_names=[ [schema_name, "%s_%s_fkey" % (tname, "_".join(fkcols))] ],
        annotations={
            schema_tag: fkdef,
        }
    )

def make_table(tdef):
    tname = tdef["name"]
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
    return Table.define(
        tname,
        column_defs=[
            make_column(cdef)
            for cdef in tdef_fields
        ],
        key_defs=keys,
        fkey_defs=[
            make_fkey(tname, fkdef)
            for fkdef in tdef_fkeys
        ],
        comment=tcomment,
        provide_system=not (os.getenv('SKIP_SYSTEM_COLUMNS', 'false').lower() == 'true'),
        annotations={
            resource_tag: tdef_resource,
            schema_tag: tdef,
        }
    )

deriva_schema = {
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

json.dump(deriva_schema, sys.stdout, indent=2)
