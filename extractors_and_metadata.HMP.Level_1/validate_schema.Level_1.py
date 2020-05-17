#!/usr/bin/env python3

###################################################################################
# Please install tableschema before running: https://pypi.org/project/tableschema/
###################################################################################

import tableschema

from datapackage import Package, Resource, validate, exceptions

instanceDir = 'HMP_C2M2_Level_1_preBag_ETL_instance_TSV_files'

schemaFile = 'C2M2_Level_1.datapackage.json'

c2m2_level_1_schema = '%s/%s' % (instanceDir, schemaFile)

try:
   valid = validate(c2m2_level_1_schema)
except exceptions.ValidationError as exception:
   for error in exception.errors:
      print(error)


