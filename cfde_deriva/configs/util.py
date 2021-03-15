
import sys
import pkgutil
import json

from . import portal

def sql_identifier(name):
    return '"%s"' % (name.replace('"', '""'),)

def print_portal_etl_sql():
    """Output SQL script to (re-)compute derived data in a C2M2 catalog.

    This is a replacement for prior generate_demo_etl_sql shell
    script, now that files have become package data.

    """
    portal_schema = json.loads(
        pkgutil.get_data(portal.__name__, 'c2m2-level1-portal-model.json').decode()
    )

    etl_parts = [
        res
        for res in portal_schema["resources"]
        if "derivation_sql_path" in res
    ]

    etl_col_parts = [
        (res,col)
        for res in portal_schema["resources"]
        for col in res["schema"]["fields"]
        if "derivation_sql_path" in col
    ]
    
    sys.stdout.write("""
BEGIN;

SET search_path = "CFDE";

-- delete in case we have previous ETL results?
""")

    for res in reversed(etl_parts):
        sys.stdout.write("DELETE FROM %s ;\n" % sql_identifier(res["name"]))

    for res, col in etl_col_parts:
        sys.stdout.write("UPDATE %s SET %s = NULL;\n" % (sql_identifier(res["name"]), sql_identifier(col["name"]),))
        
    for res in etl_parts:
        sys.stdout.write("""
-- [[[ begin %(fname)s
%(sql)s
-- ]]] end %(fname)s

""" % {
    "fname": res["derivation_sql_path"],
    "sql": pkgutil.get_data(portal.__name__, res["derivation_sql_path"]).decode(),
})

    for res, col in etl_col_parts:
        sys.stdout.write("""
-- [[[ begin %(fname)s
%(sql)s
-- ]]] end %(fname)s

""" % {
    "fname": col["derivation_sql_path"],
    "sql": pkgutil.get_data(portal.__name__, col["derivation_sql_path"]).decode(),
})

    sys.stdout.write("""
COMMIT;
ANALYZE;
""")

def print_portal_col_etl_upgrade_sql():
    """Output SQL script to (re-)compute derived columns in a C2M2 catalog.

    This is a special purpose upgrade path to run on a
    postgres/ermrest catalog via local DB connection.

    """
    portal_schema = json.loads(
        pkgutil.get_data(portal.__name__, 'c2m2-level1-portal-model.json').decode()
    )

    etl_col_parts = [
        (res,col)
        for res in portal_schema["resources"]
        for col in res["schema"]["fields"]
        if "derivation_sql_path" in col
    ]
    
    sys.stdout.write("""
BEGIN;

SET search_path = "CFDE";

-- upgrade prior catalog assumed to already have table ETL but not column ETL
""")

    # HACK: only works for our 2021-03 added keyword text[] columns...
    for res, col in etl_col_parts:
        sys.stdout.write("ALTER TABLE %s ADD COLUMN %s text[];\n" % (sql_identifier(res["name"]), sql_identifier(col["name"]),))

    sys.stdout.write("""
SELECT _ermrest.model_change_event();
""")

    # HACK: rewrite sqlite SQL agg func name to postgres agg func name
    for res, col in etl_col_parts:
        sys.stdout.write("""
-- [[[ begin %(fname)s
%(sql)s
-- ]]] end %(fname)s

""" % {
    "fname": col["derivation_sql_path"],
    "sql": pkgutil.get_data(portal.__name__, col["derivation_sql_path"]).decode().replace('json_group_array', 'array_agg'),
})

    sys.stdout.write("""
COMMIT;
ANALYZE;
""")

def main(args):
    """Trivial configs.util CLI

    """
    try:
        if len(args) == 0:
            raise ValueError("Missing required sub-command as first argument")

        if args[0] == 'print_portal_col_etl_upgrade':
            print_portal_col_etl_upgrade_sql()
        else:
            raise ValueError("Unknown sub-command '%s'" % args[0])

        return 0
    except ValueError as e:
        print("""ERROR: %s

Usage: python3 -m cfde_deriva.configs <subcommand> [ <arg>... ]

Supported sub-command signatures:

  print_portal_col_etl_upgrade

Obsoleted sub-commands:

  print_portal_etl_sql: Such ETL now handled internally by release module!

""" % (e,))
        return 1

if __name__ == '__main__':
    exit(main(sys.argv[1:]))
