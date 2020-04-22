#!/usr/bin/env python3

###################################################################################
# Please install tableschema before running: https://pypi.org/project/tableschema/
###################################################################################

import tableschema

from datapackage import Package, Resource, validate, exceptions

c2m2_level_0_schema = '002_HMP__C2M2_Level_0_preload__preBag_output_files/C2M2_Level_0.datapackage.json'

try:
   valid = validate(c2m2_level_0_schema)
except exceptions.ValidationError as exception:
   for error in exception.errors:
      print(error)


