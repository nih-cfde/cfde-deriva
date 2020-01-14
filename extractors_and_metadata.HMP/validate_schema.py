#!/usr/bin/env python3

###################################################################################
# Please install tableschema before running: https://pypi.org/project/tableschema/
###################################################################################

import tableschema

from datapackage import Package, Resource, validate, exceptions

c2m2_schema = '004_HMP__C2M2_preload__preBag_output_files/datapackage.json'

try:
   valid = validate(c2m2_schema)
except exceptions.ValidationError as exception:
   for error in exception.errors:
      print(error)


