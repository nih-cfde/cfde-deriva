#!/usr/bin/env python3

##########################################################################################
# AUTHOR INFO
##########################################################################################

# Arthur Brady (Univ. of MD Inst. for Genome Sciences) wrote this script to extract
# HMP experimental data and transform it to conform to the draft C2M2 Level 0 data
# specification prior to ingestion into a central CFDE database.

# Creation date: 2020-04-22

# contact email: abrady@som.umaryland.edu

import os
import json
import re
import sys

##########################################################################################
# SUBROUTINES
##########################################################################################

# Create and return a new unique ID (optionally prefixed via argument).

def getNewID( prefix ):
   
   global uniqueNumericIndex

   if prefix == '':
      
      die('getNewID() called with no ID prefix; aborting.')

   newID = '%s%08d' % (prefix, uniqueNumericIndex)

   uniqueNumericIndex = uniqueNumericIndex + 1

   return newID

# end sub: getNewID( prefix )

# Halt program and report why.

def die( errorMessage ):
   
   print('\n   FATAL: %s\n' % errorMessage, file=sys.stderr)

   sys.exit(-1)

# end sub: die( errorMessage )

# Print a logging message to STDERR.

def progressReport( message ):
   
   print('%s' % message, file=sys.stderr)

# end sub: progressReport( message )

# Get banned nodeIDs for HMP DB: ignore test nodes and other such noise.

def loadBannedIDs(  ):
   
   global banList, bannedIDs

   with open(banList, 'r') as IN:
      
      for line in IN:
         
         bannedIDs.add(line.rstrip('\r\n'))

      # end for ( line iterator on banned-node file )

   # end with ( banned-node file opened as 'IN' )

# end sub: loadBannedIDs(  )

# Scan a list object during data ingest (flatten prior to rearrangement).

def scanList( objectID, objectType, listName, listObject ):
   
   global flatObjects, scalarTypes, nativeTypeToNativeColNames

   fieldIndex = 0

   for element in listObject:
      
      colName = "%s__%09d" % ( listName, fieldIndex )

      fieldIndex = fieldIndex + 1

      if type( element ) in scalarTypes:
         
         flatObjects[objectID][colName] = re.sub(r'\s+', r' ', str(element))

         if colName not in nativeTypeToNativeColNames[objectType]:
            
            nativeTypeToNativeColNames[objectType].add(colName)

      elif type( element ) == list:
         
         scanList( objectID, objectType, colName, element )

      elif type( element ) == dict:
         
         scanDict( objectID, objectType, colName, element )

      else:
         
         die("scanList(): Unexpected data type detected for docs[%s]['meta'][%s]: '%s'; aborting." % ( objectID, propertyName, type( element ).__name__ ))

      # end if ( switch on element type )

   # end for ( element in listObject )

# end sub: scanList( objectID, objectType, listName, listObject )

# Scan a dict object during data ingest (flatten prior to rearrangement).

def scanDict( objectID, objectType, dictName, dictObject ):
   
   global flatObjects, scalarTypes, nativeTypeToNativeColNames

   for keyword in sorted(dictObject.keys()):
      
      colName = "%s__%s" % ( dictName, keyword )

      element = dictObject[keyword]

      if type( element ) in scalarTypes:
         
         flatObjects[objectID][colName] = re.sub(r'\s+', r' ', str(element))

         if colName not in nativeTypeToNativeColNames[objectType]:
            
            nativeTypeToNativeColNames[objectType].add(colName)

      elif type( element ) == list:
         
         scanList( objectID, objectType, colName, element )

      elif type( element ) == dict:
         
         scanDict( objectID, objectType, colName, element )

      else:
         
         die("scanDict(): Unexpected data type detected for docs[%s]['meta'][%s][%s]: '%s'; aborting." % ( objectID, dictName, keyword, type( element ).__name__ ))

      # end if ( switch on element type )

   # end for ( keyword in sorted(dictObject.keys()) )

# end sub: scanDict( objectID, objectType, dictName, dictObject )

# Flatten JSON-ingested data for controlled manipulation prior to serialization.

def flattenData(  ):
   
   global HMPdata, allowableNodeTypes, bannedIDs, nativeTypeToNodeID, nodeIDToNativeType, nativeTypeToNativeColNames, flatObjects, scalarTypes

   # Flatten the loaded JSON data prior to rearrangement and C2M2 serialization.

   for docObject in HMPdata['docs']:
      
      if ( '_id' in docObject ) and ( re.search(r'_hist$', docObject['_id']) is None ) and ( 'node_type' in docObject ) and ( docObject['node_type'] in allowableNodeTypes ):
         
         currentID = docObject['_id']

         if currentID not in bannedIDs:
            
            nodeType = docObject['node_type']

            if nodeType not in nativeTypeToNodeID:
               
               nativeTypeToNodeID[nodeType] = set()

            nativeTypeToNodeID[nodeType].add(currentID)

            if nodeType not in nativeTypeToNativeColNames:
               
               nativeTypeToNativeColNames[nodeType] = set()

            if currentID not in flatObjects:
               
               # Initialize the main dict and the type lookup for the current object.

               flatObjects[currentID] = {}

               nodeIDToNativeType[currentID] = nodeType

            else:
               
               # We've seen this object ID before; fatal.

               die('Duplicate ID: %s; aborting.' % docObject['_id'])

            # end if ( ensure no previous observations of current object ID )

            if 'meta' in docObject:
               
               for propertyName in docObject['meta']:
                  
                  if docObject['meta'][propertyName] is None:
                     
                     flatObjects[currentID][propertyName] = ''

                     if propertyName not in nativeTypeToNativeColNames[nodeType]:
                        
                        nativeTypeToNativeColNames[nodeType].add(propertyName)
                     
                  elif type( docObject['meta'][propertyName] ) in scalarTypes:
                     
                     # Some of these fields contain unacceptable whitespace monkey wrenches. Handle it.

                     flatObjects[currentID][propertyName] = re.sub(r'\s+', r' ', str(docObject['meta'][propertyName]))

                     if propertyName not in nativeTypeToNativeColNames[nodeType]:
                        
                        nativeTypeToNativeColNames[nodeType].add(propertyName)
                     
                  elif type( docObject['meta'][propertyName] ) == list:
                     
                     scanList( currentID, nodeType, propertyName, docObject['meta'][propertyName] )

                  elif type( docObject['meta'][propertyName] ) == dict:
                     
                     scanDict( currentID, nodeType, propertyName, docObject['meta'][propertyName] )

                  else:
                     
                     die("Unexpected data type detected for docs[%s]['meta'][%s]: '%s'; aborting." % ( currentID, propertyName, type( docObject['meta'][propertyName] ).__name__ ))

                  # end if ( switch on docObject['meta'][propertyName] type check

               # end for ( each propertyName in the 'meta' block for this object )

            # end if ( we have a 'meta' block for this object )

            if 'linkage' in docObject:
               
               scanDict( currentID, nodeType, 'linkage', docObject['linkage'] )

            # end if ( we have a 'linkage' block for this object )

         # end if ( the current node ID isn't on the 'banned' list )

      # end if ( we're looking at a valid object )

   # end for ( docObject in HMPdata['docs'] )

# end sub flattenData(  )

# Populate new C2M2 'file' objects with relevant data.

def populateFiles(  ):
   
   global objectsToWrite, flatObjects, nativeTypeToNodeID, baseURL, enumMap, FileNodeTypes, parents, processedBy, producedBy, dummyProtocolID, fullURL

   if 'file' not in objectsToWrite:
      
      objectsToWrite['file'] = {}

   # end if ( we haven't yet created a 'file' substructure in objectsToWrite )

   for nodeType in FileNodeTypes:
      
      if nodeType not in nativeTypeToNodeID:
         
         die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

      # end if ( nodeType not in nativeTypeToNodeID )

      for currentID in nativeTypeToNodeID[nodeType]:
         
         # 'file': [
         #    'id_namespace',
         #    'local_id',
         #    'size_in_bytes',
         #    'sha256',
         #    'md5',
         #    'persistent_id',
         #    'filename'
         # ]

         objectsToWrite['file'][currentID] = {}

         objectsToWrite['file'][currentID]['id_namespace'] = id_namespace

         objectsToWrite['file'][currentID]['persistent_id'] = getNewID('minid.test:FAKE_MINID_')

         ######################################################################
         ######################## nodeType: proteome ##########################
         ######################################################################

         if nodeType == 'proteome':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['raw_url__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

            objectsToWrite['file'][currentID]['size_in_bytes'] = ''

         ######################################################################
         ######################## nodeType: metabolome ########################
         ######################################################################

         elif nodeType == 'metabolome':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

            objectsToWrite['file'][currentID]['size_in_bytes'] = ''

         ######################################################################
         ######################## nodeType: serology ##########################
         ######################################################################

         elif nodeType == 'serology':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

            objectsToWrite['file'][currentID]['size_in_bytes'] = ''

         ######################################################################
         ######################## nodeType: cytokine ##########################
         ######################################################################

         elif nodeType == 'cytokine':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

            objectsToWrite['file'][currentID]['size_in_bytes'] = ''

         ######################################################################
         ######################## nodeType: lipidome ##########################
         ######################################################################

         elif nodeType == 'lipidome':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

            objectsToWrite['file'][currentID]['size_in_bytes'] = ''

         ######################################################################
         ######################## nodeType: proteome_nonpride #################
         ######################################################################

         elif nodeType == 'proteome_nonpride':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['raw_url__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['md5'] = ''

            objectsToWrite['file'][currentID]['sha256'] = ''

            objectsToWrite['file'][currentID]['size_in_bytes'] = ''

         ######################################################################
         ######################## nodeType: clustered_seq_set #################
         ######################################################################

         elif nodeType == 'clustered_seq_set':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['size_in_bytes'] = flatObjects[currentID]['size']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

         ######################################################################
         ######################## nodeType: annotation ########################
         ######################################################################

         elif nodeType == 'annotation':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['size_in_bytes'] = flatObjects[currentID]['size']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

         ######################################################################
         ######################## nodeType: host_variant_call #################
         ######################################################################

         elif nodeType == 'host_variant_call':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['size_in_bytes'] = flatObjects[currentID]['size']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

         ######################################################################
         ######################## nodeType: alignment ########################
         ######################################################################

         elif nodeType == 'alignment':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['size_in_bytes'] = flatObjects[currentID]['size']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

         ######################################################################
         ######################## nodeType: viral_seq_set #####################
         ######################################################################

         elif nodeType == 'viral_seq_set':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['size_in_bytes'] = ''

            objectsToWrite['file'][currentID]['sha256'] = ''

         ######################################################################
         ######################## nodeType: *_seq_set (not caught above) ######
         ######################################################################

         elif re.search(r'_seq_set$', nodeType) is not None:
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['size_in_bytes'] = flatObjects[currentID]['size']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

         ######################################################################
         ######################## nodeType: abundance_matrix ##################
         ######################################################################

         elif nodeType == 'abundance_matrix':
            
            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['size_in_bytes'] = flatObjects[currentID]['size']

            # Not stored anywhere.

            objectsToWrite['file'][currentID]['sha256'] = ''

         # end if ( nodeType switch )

         # Fix bad (stub-value) data from a few otherwise good OSDF nodes.

         if objectsToWrite['file'][currentID]['size_in_bytes'] == 0 or objectsToWrite['file'][currentID]['size_in_bytes'] == 1 or objectsToWrite['file'][currentID]['size_in_bytes'] == '0' or objectsToWrite['file'][currentID]['size_in_bytes'] == '1':
            
            objectsToWrite['file'][currentID]['size_in_bytes'] = ''

         if re.search( r'^0000000', objectsToWrite['file'][currentID]['md5'] ) is not None:
            
            objectsToWrite['file'][currentID]['md5'] = ''

      # end for ( currentID in nativeTypeToNodeID[nodeType] )

   # end for ( nodeType in FileNodeTypes )

# end sub populateFiles( nodeType )

def writeTable( objectName ):
   
   global outDir, objectsToWrite, outputColumns

   if objectName not in objectsToWrite:
      
      die("No data loaded for output table '%s'; aborting." % objectName)

   # end if ( we have no data loaded into $objectsToWrite for output type $objectName )

   if objectName not in outputColumns:
      
      die("Can't find an 'outputColumns' list for C2M2 output type '%s'; aborting." % objectName)

   # end if ( objectName doesn't have a corresponding list of output column names )

   outFile = '%s/%s.tsv' % ( outDir, objectName )

   with open(outFile, 'w') as OUT:
      
      OUT.write( '\t'.join(outputColumns[objectName]) + '\n' )

      for currentID in sorted( objectsToWrite[objectName] ):
         
         first = True

         for colName in outputColumns[objectName]:
            
            if not first:
               
               OUT.write('\t')

            first = False

            if colName == 'local_id':
               
               # Special case. Will need to change this if 'local_id' fields are eliminated or renamed.

               OUT.write( currentID )

            elif colName in objectsToWrite[objectName][currentID]:
               
               OUT.write( '%s' % objectsToWrite[objectName][currentID][colName] )

            # end if ( switch on colName )

         # end for ( loop through column names for this object type in order )

         OUT.write( '\n' )

      # end for ( each objectID of this object type )

   # end with ( outFile opened as 'OUT' )

# end sub writeTable( tableName )

##########################################################################################
# PARAMETERS
##########################################################################################

##########################################################################################
# The value of the `id_namespace` field required by C2M2 Level 0 for all file objects;
# to be clash-cleared by CFDE (we're just making one up, here).

id_namespace = 'cfde_id_namespace:2'

##########################################################################################
# Location of the Table-Schema JSON file describing the output set.

tableSchemaLoc = '000_C2M2_Level_0_JSON_Schema/C2M2_Level_0.datapackage.json'

##########################################################################################
# Disambiguator for on-the-fly ID generation.

uniqueNumericIndex = 0

##########################################################################################
# List of banned nodeIDs for HMP DB: ignore test nodes and other such noise.

banList = '001_raw_couchDB_JSON_backup_dump__2019_10_20/banned_IDs.txt'

bannedIDs = set()

##########################################################################################
# Raw JSON dump of full HMP couchDB database.

inFile = '001_raw_couchDB_JSON_backup_dump__2019_10_20/osdf-ihmp.igs.umaryland.edu_couchdb_2019-10-20.json'

##########################################################################################
# Output directory.

outDir = '002_HMP__C2M2_Level_0_preload__preBag_output_files'

##########################################################################################
# Reference set structures.

# Native HMP object types expected during ingest.

allowableNodeTypes = {
   
   '16s_raw_seq_set': 'file',
   '16s_trimmed_seq_set': 'file',
   'abundance_matrix': 'file',
   'alignment': 'file',
   'annotation': 'file',
   'clustered_seq_set': 'file',
   'cytokine': 'file',
   'host_epigenetics_raw_seq_set': 'file',
   'host_transcriptomics_raw_seq_set': 'file',
   'host_variant_call': 'file',
   'host_wgs_raw_seq_set': 'file',
   'lipidome': 'file',
   'metabolome': 'file',
   'microb_transcriptomics_raw_seq_set': 'file',
   'proteome': 'file',
   'proteome_nonpride': 'file',
   'serology': 'file',
   'viral_seq_set': 'file',
   'wgs_assembled_seq_set': 'file',
   'wgs_raw_seq_set': 'file'
}

# Native HMP object types that map to C2M2 'file' objects.

FileNodeTypes = {
   
   '16s_raw_seq_set',
   '16s_trimmed_seq_set',
   'abundance_matrix',
   'alignment',
   'annotation',
   'clustered_seq_set',
   'cytokine',
   'host_epigenetics_raw_seq_set',
   'host_transcriptomics_raw_seq_set',
   'host_variant_call',
   'host_wgs_raw_seq_set',
   'lipidome',
   'metabolome',
   'microb_transcriptomics_raw_seq_set',
   'proteome',
   'proteome_nonpride',
   'serology',
   'viral_seq_set',
   'wgs_assembled_seq_set',
   'wgs_raw_seq_set'
}

# Python data types for ingested values that can be passed to the
# output as indivisible/atomic items (i.e. things that aren't
# containers for other objects).

scalarTypes = {
   
   int,
   float,
   str,
   bool
}

# Sequences specifying order of output columns for C2M2 serialization (by table).

outputColumns = {
   
   'file': [
      'id_namespace',
      'local_id',
      'persistent_id',
      'size_in_bytes',
      'sha256',
      'md5',
      'filename'
   ]
}

##########################################################################################
# Serialization objects: initial load of HMP metadata

flatObjects = {}
nativeTypeToNodeID = {}
nodeIDToNativeType = {}
nativeTypeToNativeColNames = {}

termsUsed = {}

##########################################################################################
# Serialization objects: C2M2 output data structures

objectsToWrite = {}
objectsInDatasets = {}

parents = {}

processedBy = {}
producedBy = {}

fullURL = {}

##########################################################################################
# EXECUTION
##########################################################################################

# Make the output directory if it doesn't yet exist.

progressReport("Creating output directory...")

if not os.path.isdir(outDir) and os.path.exists(outDir):
   
   die('%s exists but is not a directory; aborting.' % outDir)

elif not os.path.isdir(outDir):
   
   os.mkdir(outDir)

# Load HMP DB node IDs to skip (test/junk nodes, etc.)

progressReport("Loading banned node IDs...")

loadBannedIDs()

# Load the raw HMP metadata DB from its JSON dump file.

progressReport("Loading HMP OSDF dump...")

with open(inFile, 'r') as IN:
   
   HMPdata = json.load(IN)

# end with (open inFile for JSON loading)

# Flatten the data from the JSON dump for pre-serialization processing.

progressReport("Flattening loaded JSON data...")

flattenData()

# Gather all data needed to serialize 'file' objects.

progressReport("Building file table...")

populateFiles()

# Serialize all 'file' objects into a TSV.

progressReport("Writing file table...")

writeTable('file')

# Include the Table-Schema JSON document in the output for reference.

progressReport("Copying JSON tableschema to bdbag data store...")

os.system('cp ' + tableSchemaLoc + ' ' + outDir)

# Make a BDBag for final delivery and rename it to remove local indexing info.

progressReport("Making bdbag...")

bagDir = re.sub(r'preBag_output_files', r'bdbag', re.sub(r'^\d+_', r'', outDir))

os.system('mv ' + outDir + ' ' + bagDir);

os.system('bdbag --quiet --archiver tgz ' + bagDir);

# Revert the intermediate output directory from BDBag format to avoid
# chaos and despair when running this script multiple times without
# clearing outputs.

os.system('bdbag --quiet --revert ' + bagDir);

os.system('mv ' + bagDir + ' ' + outDir);

progressReport("done!")

