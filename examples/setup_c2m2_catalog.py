#!/usr/bin/python3

import os
import sys
import subprocess
import json
import csv

from deriva.core import DerivaServer, get_credential, urlquote, AttrDict
from deriva.core.ermrest_config import tag

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

## ugly: use subprocess to acquire ERMrest model definitions
try:
    datapackage_filename = sys.argv[1]
    datapackage_dirname = os.path.dirname(datapackage_filename)
except IndexError:
    print("Error: data package filename required as sole argument")

with open(datapackage_filename, 'r') as f:
    mdoc = json.loads(
        subprocess.run(
            ['python3', './examples/tableschema_to_deriva.py',],
            stdin=f,
            stdout=subprocess.PIPE
        ).stdout
    )

## create catalog
catalog = server.create_ermrest_catalog()
print('New catalog has catalog_id=%s' % catalog.catalog_id)
print("Don't forget to delete it if you are done with it!")

## provision a model and basic ACLs/configuration

# provision whole schema in one low-level REST call
catalog.post('/schema', json=mdoc).raise_for_status()

# get catalog's model and configuration management API
model_root = catalog.getCatalogModel()
cfde_schema = model_root.schemas['CFDE']

print("Model deployed.")

# set some reasonable catalog-wide ACLs for demo...

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

model_root.acls.update({
    "owner": [grp.demo_admin, grp.demo_creator],
    "insert": writers,
    "update": writers,
    "delete": writers,
    "select": [grp.demo_reader, grp.isrd_testers, grp.isrd_staff],
    "enumerate": ["*"],
})

# set custom chaise configuration values for this catalog
model_root.annotations[tag.chaise_config] = {
    # hide system metadata by default in tabular listings, to focus on CFDE-specific content
    "SystemColumnsDisplayCompact": [],
}

# have Chaise display underscores in model element names as whitespace
cfde_schema.display.name_style = {"underline_space": True}

# prettier display of built-in ERMrest_Client table entries
model_root.table('public', 'ERMrest_Client').table_display.row_name = {
    "row_markdown_pattern": "{{{Full_Name}}} ({{{Display_Name}}})"
}

## apply the above ACL and annotation changes to server
model_root.apply(catalog)


## TODO: load some sample data?

print("Policies and presentation configured.")

print("Try visiting 'https://%s/chaise/recordset/#%s/CFDE:Dataset'" % (servername, catalog.catalog_id))

## to re-bind to the same catalog in the future, extract catalog_id from URL

# server = DerivaServer('https', servername, credentials)
# catalog_id = '1234'
# catalog = server.connect_ermrest(catalog_id)

## after binding to your catalog, you can delete it too
## but we force you to be explicit:

# catalog.delete_ermrest_catalog(really=True)

