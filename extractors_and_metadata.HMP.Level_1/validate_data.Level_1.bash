#!/bin/bash

##################################################################################################
# Please install goodtables before running: https://github.com/frictionlessdata/goodtables-py
# 
# NOTE: 'MISSING HEADER' ERRORS WILL BE GENERATED for all TSVs with no records in them (i.e. TSVs
# containing only header lines). If the tables associated with the offending TSVs are supposed to
# be empty, then these errors can and should be ignored; the table format is valid despite
# goodtables' complaints.
##################################################################################################

instanceDir=HMP_C2M2_Level_1_preBag_ETL_instance_TSV_files

schemaFile=C2M2_Level_1.datapackage.json

goodtables validate -o data_validation_report.HMP.Level_1.txt $instanceDir/$schemaFile


