#!/usr/bin/env python3

##################################################################################################
# 
# DEPENDENCY: install tableschema before running: https://pypi.org/project/tableschema/
# 
# Arthur Brady (Univ. of MD Inst. for Genome Sciences) wrote this script to validate
# the canonical C2M2 Level 1 JSON Schema specification against its underlying
# frictionless.io "Data Package" specification (*) prior to use in validating
# C2M2 ETL instance data prior to submission of said instance data to CFDE for
# ingestion into the central C2M2 database.
# 
# (*) https://frictionlessdata.io/data-package/
# 
# Creation date: 2020-05
# Lastmod date unless I forgot to change it: 2020-05-27
#
# contact email: abrady@som.umaryland.edu
# 
##################################################################################################

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


