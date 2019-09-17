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
    if type == "integer":
        return builtin_types.int8
    if type == "number":
        return builtin_types.float8
    if type == "list":
        # assume a list is a list of strings for now...
        return builtin_types["text[]"]
    raise ValueError('no mapping defined yet for type=%s format=%s' % (type, format))

def make_column(cdef):
    constraints = cdef.get("constraints", {})
    return Column.define(
        cdef["name"],
        make_type(
            cdef.get("type", "string"),
            cdef.get("format", "default"),
        ),
        nullok=not constraints.get("required", False),
        comment=cdef.get("description"),
    )

def make_key(tname, cols):
    return Key.define(
        cols,
        constraint_names=[ [schema_name, "%s_%s_key" % (tname, "_".join(cols))] ],
    )

def make_fkey(tname, fkdef):
    fkcols = fkdef["fields"]
    fkcols = [fkcols] if isinstance(fkcols, str) else fkcols
    reference = fkdef["reference"]
    pktable = reference["resource"]
    pktable = tname if pktable == "" else pktable
    pkcols = reference["fields"]
    pkcols = [pkcols] if isinstance(pkcols, str) else pkcols
    return ForeignKey.define(
        fkcols,
        schema_name,
        pktable,
        pkcols,
        constraint_names=[ [schema_name, "%s_%s_fkey" % (tname, "_".join(fkcols))] ],
    )

def make_table(tdef):
    tname = tdef["name"]
    tcomment = tdef.get("description")
    tdef = tdef["schema"]
    keys = []
    keysets = set()
    pk = tdef.get("primaryKey")
    if isinstance(pk, str):
        pk = [pk]
    if isinstance(pk, list):
        keys.append(make_key(tname, pk))
        keysets.add(frozenset(pk))
    return Table.define(
        tname,
        column_defs=[
            make_column(cdef)
            for cdef in tdef["fields"]
        ],
        key_defs=([ make_key(tname, pk) ] if pk else []) + [
            make_key(tname, [cdef["name"]])
            for cdef in tdef["fields"]
            if cdef.get("constraints", {}).get("unique", False)
            and frozenset([cdef["name"]]) not in keysets
        ],
        fkey_defs=[
            make_fkey(tname, fkdef)
            for fkdef in tdef.get("foreignKeys", [])
        ],
        comment=tcomment,
        provide_system=not (os.getenv('SKIP_SYSTEM_COLUMNS', 'false').lower() == 'true')
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
