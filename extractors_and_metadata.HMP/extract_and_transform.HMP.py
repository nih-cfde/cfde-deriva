#!/usr/bin/env python3

##########################################################################################
# AUTHOR INFO
##########################################################################################

# Arthur Brady (Univ. of MD Inst. for Genome Sciences) wrote this script to extract
# HMP experimental data and transform it to conform to the draft C2M2 data
# specification prior to ingestion into a central CFDE database.

# Creation date: 2019-10-23
# Lastmod date unless I forgot to change it: 2020-01-14

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

# Load functions mapping value-constrained DCC-internal metadata field values
# to third-party CV term IDs. Map filenames encode source file & field, target
# ontology name, and the name of the target data structure in the output model.

def loadEnumMaps(  ):
   
   global mapFiles, enumMap

   for targetField in sorted( mapFiles.keys() ):
      
      mapFile = mapFiles[targetField]

      enumMap[targetField] = {}

      with open( mapFile, 'r' ) as IN:
         
         header = IN.readline()

         for line in IN:
            
            dccFieldVal, dccFieldDesc, cvTermID, cvTermName = re.split(r'\t', line.rstrip('\r\n'))

            enumMap[targetField][dccFieldVal] = cvTermID

         # end for ( line iterator on mapping file )

      # end with ( mapping file opened as 'IN' )

   # end for ( iterator on mapfiles.keys() )

# end sub loadEnumMaps(  )

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

# Populate new C2M2 'dataset' objects with relevant data.

def populateDatasets(  ):
   
   global objectsToWrite, objectsInDatasets, flatObjects, nativeTypeToNodeID, enumMap, DatasetNodeTypes, hmpCFProgramID

   if 'dataset' not in objectsToWrite:
      
      objectsToWrite['dataset'] = {}

   # end if ( we haven't yet created a 'dataset' substructure in objectsToWrite )

   if 'auxiliary_data' not in objectsToWrite:
      
      objectsToWrite['auxiliary_data'] = {}
      objectsToWrite['auxiliary_data']['dataset'] = {}

   elif 'dataset' not in objectsToWrite['auxiliary_data']:
      
      objectsToWrite['auxiliary_data']['dataset'] = {}

   # end if ( we haven't yet created an 'auxiliary_data.dataset' substructure in objectsToWrite )

   for nodeType in DatasetNodeTypes:
      
      if nodeType not in nativeTypeToNodeID:
         
         die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

      # end if ( nodeType not in nativeTypeToNodeID )

      for currentID in nativeTypeToNodeID[nodeType]:
         
         objectsToWrite['dataset'][currentID] = {}

         doNotReprocess = set()

         ######################################################################
         ######################## nodeTypes: project, study ###################
         ######################################################################

         if nodeType == 'project' or nodeType == 'study':
            
            doNotReprocess |= {'name', 'description'}

            #########################
            # Simple-import metadata:

            objectsToWrite['dataset'][currentID]['title'] = flatObjects[currentID]['name']

            objectsToWrite['dataset'][currentID]['description'] = flatObjects[currentID]['description']

            objectsToWrite['dataset'][currentID]['data_source'] = hmpCFProgramID

         # end if ( nodeType switch )

         ############################################################################
         # Metadata not mapped to C2M2 objects: send to generic auxiliary data table.

         objectsToWrite['auxiliary_data']['dataset'][currentID] = {}

         for fieldName in sorted(flatObjects[currentID].keys()):
            
            if ( fieldName not in doNotReprocess ) and ( re.search(r'^linkage', fieldName) is None ):
               
               currentIndex = '0'

               currentValue = flatObjects[currentID][fieldName]

               matchResult = re.search(r'_+(\d+)$', fieldName)

               if not ( matchResult is None ):
                  
                  # Strip the encoded index from the end of the term and serialize as a (table-encoded) array.

                  currentIndex = str(int(matchResult.group(1)))

                  fieldName = re.sub(r'_+\d+$', r'', fieldName)

               # end if ( fieldName has a suffix-encoded index )

               if fieldName not in objectsToWrite['auxiliary_data']['dataset'][currentID]:
                  
                  objectsToWrite['auxiliary_data']['dataset'][currentID][fieldName] = { currentIndex: currentValue }

               else:
                  
                  objectsToWrite['auxiliary_data']['dataset'][currentID][fieldName][currentIndex] = currentValue

               # end if ( we've created an entry yet for currentID.fieldName )

            elif re.search(r'^linkage', fieldName) is not None:
               
               # The only linkages hanging off the 'study' nodeType are pointers to
               # supersets (other studies or projects). 'project' nodeTypes have
               # no linkages: they are top-level objects, and all links flow upward.

               linkedID = flatObjects[currentID][fieldName]

               if 'dataset' not in objectsInDatasets:
                  
                  objectsInDatasets['dataset'] = {}

               # end if ( setup check for objectsInDatasets['dataset']

               objectsInDatasets['dataset'][currentID] = set()

               objectsInDatasets['dataset'][currentID] |= { linkedID }

               if currentID not in parents:
                  
                  parents[currentID] = set()

               # end if ( setup check for parents[currentID] )

               parents[currentID] |= { linkedID }

            # end if ( we're looking at a fieldName we should be processing )

         # end for ( each fieldName in flatObjects for currentID )

      # end for ( currentID in nativeTypeToNodeID[nodeType] )

   # end for ( nodeType in DatasetNodeTypes )

# end sub populateDatasets(  )

# Populate new C2M2 'subject' objects with relevant data.

def populateSubjects(  ):
   
   global objectsToWrite, flatObjects, nativeTypeToNodeID, enumMap, SubjectNodeTypes, parents, singleOrgGranularityID

   if 'subject' not in objectsToWrite:
      
      objectsToWrite['subject'] = {}

   # end if ( we haven't yet created a 'subject' substructure in objectsToWrite )

   if 'auxiliary_data' not in objectsToWrite:
      
      objectsToWrite['auxiliary_data'] = {}
      objectsToWrite['auxiliary_data']['subject'] = {}

   elif 'subject' not in objectsToWrite['auxiliary_data']:
      
      objectsToWrite['auxiliary_data']['subject'] = {}

   # end if ( we haven't yet created an 'auxiliary_data.subject' substructure in objectsToWrite )

   for nodeType in SubjectNodeTypes:
      
      if nodeType not in nativeTypeToNodeID:
         
         die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

      # end if ( nodeType not in nativeTypeToNodeID )

      for currentID in nativeTypeToNodeID[nodeType]:
         
         objectsToWrite['subject'][currentID] = {}

         doNotReprocess = set()

         #########################
         # Simple-import metadata:

         objectsToWrite['subject'][currentID]['granularity'] = singleOrgGranularityID

         ############################################################################
         # Metadata not mapped to C2M2 objects: send to generic auxiliary data table.

         objectsToWrite['auxiliary_data']['subject'][currentID] = {}

         for fieldName in sorted(flatObjects[currentID].keys()):
            
            if ( fieldName not in doNotReprocess ) and ( re.search(r'^linkage', fieldName) is None ):
               
               currentIndex = '0'

               currentValue = flatObjects[currentID][fieldName]

               matchResult = re.search(r'_+(\d+)$', fieldName)

               if not ( matchResult is None ):
                  
                  # Strip the encoded index from the end of the term and serialize as a (table-encoded) array.

                  currentIndex = str(int(matchResult.group(1)))

                  fieldName = re.sub(r'_+\d+$', r'', fieldName)

               # end if ( fieldName has a suffix-encoded index )

               if fieldName not in objectsToWrite['auxiliary_data']['subject'][currentID]:
                  
                  objectsToWrite['auxiliary_data']['subject'][currentID][fieldName] = { currentIndex: currentValue }

               else:
                  
                  objectsToWrite['auxiliary_data']['subject'][currentID][fieldName][currentIndex] = currentValue

               # end if ( we've created an entry yet for currentID.fieldName )

            elif re.search(r'^linkage', fieldName) is not None:
               
               # subject participates_in study

               linkedID = flatObjects[currentID][fieldName]

               if currentID not in parents:
                  
                  parents[currentID] = set()

               # end if ( setup check for parents[currentID] )

               parents[currentID] |= { linkedID }

            # end if ( we're looking at a fieldName we should be processing )

         # end for ( each fieldName in flatObjects for currentID )

      # end for ( currentID in nativeTypeToNodeID[nodeType] )

   # end for ( nodeType in SubjectNodeTypes )

# end sub populateSubjects(  )

# Populate new C2M2 'subject' objects with relevant data.

def populateBioSamples(  ):
   
   global objectsToWrite, flatObjects, nativeTypeToNodeID, enumMap, BioSampleNodeTypes, parents, termsUsed, fullURL, dummyProtocolID

   if 'bio_sample' not in objectsToWrite:
      
      objectsToWrite['bio_sample'] = {}

   # end if ( we haven't yet created a 'bio_sample' substructure in objectsToWrite )

   if 'auxiliary_data' not in objectsToWrite:
      
      objectsToWrite['auxiliary_data'] = {}
      objectsToWrite['auxiliary_data']['bio_sample'] = {}

   elif 'bio_sample' not in objectsToWrite['auxiliary_data']:
      
      objectsToWrite['auxiliary_data']['bio_sample'] = {}

   # end if ( we haven't yet created an 'auxiliary_data.bio_sample' substructure in objectsToWrite )

   for nodeType in BioSampleNodeTypes:
      
      if nodeType not in nativeTypeToNodeID:
         
         die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

      # end if ( nodeType not in nativeTypeToNodeID )

      for currentID in nativeTypeToNodeID[nodeType]:
         
         objectsToWrite['bio_sample'][currentID] = {}

         doNotReprocess = set()

         ######################################################################
         ######################## nodeType: sample ############################
         ######################################################################

         if nodeType == 'sample':
            
            doNotReprocess |= { 'body_site', 'fma_body_site' }

            #########################
            # Simple-import metadata:

            if flatObjects[currentID]['fma_body_site'] == '':
               
               objectsToWrite['bio_sample'][currentID]['anatomy'] = ''

            else:
               
               fmaCode = re.sub( r'^.*(FMA:\d+).*$', r'\1', flatObjects[currentID]['fma_body_site'] )

               if fmaCode not in enumMap['bio_sample.anatomy']:
                  
                  objectsToWrite['bio_sample'][currentID]['anatomy'] = ''
                  # Nope.
                  # die("Unrecognized FMA code in sample object '%s': '%s'; aborting." % ( currentID, fmaCode ))

               else:
                  
                  uberonTerm = enumMap['bio_sample.anatomy'][fmaCode]

                  objectsToWrite['bio_sample'][currentID]['anatomy'] = baseURL['Uberon'] + uberonTerm

                  if 'anatomy' not in fullURL:
                     
                     fullURL['anatomy'] = {}

                  # end if ( setup check for fullURL['anatomy'] )

                  fullURL['anatomy'][uberonTerm] = baseURL['Uberon'] + uberonTerm

                  if 'anatomy' not in termsUsed:
                     
                     termsUsed['anatomy'] = {
                                                uberonTerm: {}
                     }

                  else:
                     
                     termsUsed['anatomy'][uberonTerm] = {}

                  # end if ( setup check for termsUsed['anatomy'] )

               # end if ( we recognize the current FMA anatomy code )

            # end if ( we have a non-null value in 'fma_body_site' )

            objectsToWrite['bio_sample'][currentID]['protocol'] = dummyProtocolID

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # bio_sample.sample_type

            typeTerm = enumMap['bio_sample.sample_type']['material']

            objectsToWrite['bio_sample'][currentID]['sample_type'] = baseURL['OBI'] + typeTerm

            if 'sample_type' not in termsUsed:
               
               termsUsed['sample_type'] = {}

            # end if ( setup check on termsUsed['sample_type'] )

            termsUsed['sample_type'][typeTerm] = {}

            if 'sample_type' not in fullURL:
               
               fullURL['sample_type'] = {}

            # end if ( setup check for fullURL['sample_type']

            fullURL['sample_type'][typeTerm] = baseURL['OBI'] + typeTerm

         ######################################################################
         ######################## nodeType: *_prep ############################
         ######################################################################

         elif ( nodeType == '16s_dna_prep' or nodeType == 'wgs_dna_prep' or nodeType == 'host_seq_prep' or nodeType == 'microb_assay_prep' or nodeType == 'host_assay_prep' ):
            
            #########################
            # Simple-import metadata:

            typeTerm = enumMap['bio_sample.sample_type']['library']

            objectsToWrite['bio_sample'][currentID]['sample_type'] = baseURL['OBI'] + typeTerm

            if 'sample_type' not in termsUsed:
               
               termsUsed['sample_type'] = {}

            # end if ( setup check on termsUsed['sample_type'] )

            termsUsed['sample_type'][typeTerm] = {}

            if 'sample_type' not in fullURL:
               
               fullURL['sample_type'] = {}

            # end if ( setup check for fullURL['sample_type']

            fullURL['sample_type'][typeTerm] = baseURL['OBI'] + typeTerm

            objectsToWrite['bio_sample'][currentID]['anatomy'] = ''

            objectsToWrite['bio_sample'][currentID]['protocol'] = dummyProtocolID

         # end if ( nodeType switch )

         ############################################################################
         # Metadata not mapped to C2M2 objects: send to generic auxiliary data table.

         objectsToWrite['auxiliary_data']['bio_sample'][currentID] = {}

         for fieldName in sorted(flatObjects[currentID].keys()):
            
            if ( fieldName not in doNotReprocess ) and ( re.search(r'^linkage', fieldName) is None ):
               
               currentIndex = '0'

               currentValue = flatObjects[currentID][fieldName]

               matchResult = re.search(r'_+(\d+)$', fieldName)

               if not ( matchResult is None ):
                  
                  # Strip the encoded index from the end of the term and serialize as a (table-encoded) array.

                  currentIndex = str(int(matchResult.group(1)))

                  fieldName = re.sub(r'_+\d+$', r'', fieldName)

               # end if ( fieldName has a suffix-encoded index )

               if fieldName not in objectsToWrite['auxiliary_data']['bio_sample'][currentID]:
                  
                  objectsToWrite['auxiliary_data']['bio_sample'][currentID][fieldName] = { currentIndex: currentValue }

               else:
                  
                  objectsToWrite['auxiliary_data']['bio_sample'][currentID][fieldName][currentIndex] = currentValue

               # end if ( we've created an entry yet for currentID.fieldName )

            elif re.search(r'^linkage', fieldName) is not None:
               
               # There's only ever one linkage field for any of the HMP nodeTypes
               # that map to C2M2.bio_sample: to a visit ID for 'sample' nodeType, and
               # to the ID of a generating 'sample' object for the '*_prep' nodeTypes.

               linkedID = flatObjects[currentID][fieldName]

               if currentID not in parents:
                  
                  parents[currentID] = set()

               # end if ( setup check for parents[currentID] )

               parents[currentID] |= { linkedID }

               if nodeType == 'sample':
                  
                  sampleToVisit[currentID] = linkedID

               else:
                  
                  prepToBioSample[currentID] = linkedID

               # end if ( nodeType switch to determine how link caching will take place )

            # end if ( we're looking at a fieldName we should be processing )

         # end for ( each fieldName in flatObjects for currentID )

      # end for ( currentID in nativeTypeToNodeID[nodeType] )

   # end for ( nodeType in BioSampleNodeTypes )

# end sub populateBioSamples(  )

# Populate new C2M2 'file' objects with relevant data.

def populateFiles(  ):
   
   global objectsToWrite, flatObjects, nativeTypeToNodeID, baseURL, enumMap, FileNodeTypes, parents, processedBy, producedBy, dummyProtocolID, fullURL

   if 'file' not in objectsToWrite:
      
      objectsToWrite['file'] = {}

   # end if ( we haven't yet created a 'file' substructure in objectsToWrite )

   if 'auxiliary_data' not in objectsToWrite:
      
      objectsToWrite['auxiliary_data'] = {}
      objectsToWrite['auxiliary_data']['file'] = {}

   elif 'file' not in objectsToWrite['auxiliary_data']:
      
      objectsToWrite['auxiliary_data']['file'] = {}

   # end if ( we haven't yet created an 'auxiliary_data.file' substructure in objectsToWrite )

   for nodeType in FileNodeTypes:
      
      if nodeType not in nativeTypeToNodeID:
         
         die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

      # end if ( nodeType not in nativeTypeToNodeID )

      for currentID in nativeTypeToNodeID[nodeType]:
         
         objectsToWrite['file'][currentID] = {}

         doNotReprocess = set()

         formatTerm = ''
         infoTerm = ''

         if 'file_format' not in fullURL:
            
            fullURL['file_format'] = {}

         # end if ( setup check on fullURL['file_format'] )

         if 'information_type' not in fullURL:
            
            fullURL['information_type'] = {}

         # end if ( setup check on fullURL['information_type'] )

         ######################################################################
         ######################## nodeType: proteome ##########################
         ######################################################################

         if nodeType == 'proteome':
            
            doNotReprocess |= { 'raw_url__000000000', 'checksums__md5' }

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['raw_url__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # This isn't stored anywhere.

            objectsToWrite['file'][currentID]['length'] = ''

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if re.search(r'\.raw\.gz$', baseName) is not None:
               
               formatTerm = enumMap['file.file_format']['thermoRaw']
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + formatTerm

            elif re.search(r'\.mzML\.gz', baseName) is not None:
               
               formatTerm = enumMap['file.file_format']['mzML']
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + formatTerm

            else:
               
               die("Can't identify format for proteome file basename '%s'; aborting." % baseName)

            # end if ( we have an identifiable filename extension )

            # file.information_type

            infoTerm = enumMap['file.information_type']['proteomicsData']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + infoTerm
         
         ######################################################################
         ######################## nodeType: metabolome ########################
         ######################################################################

         elif nodeType == 'metabolome':
            
            doNotReprocess |= { 'urls__000000000', 'checksums__md5', 'format' }

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # This isn't stored anywhere.

            objectsToWrite['file'][currentID]['length'] = ''

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if flatObjects[currentID]['format'] == 'raw':
               
               formatTerm = enumMap['file.file_format']['thermoRaw']
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format']['thermoRaw']

            elif flatObjects[currentID]['format'] == 'mzXML':
               
               formatTerm = enumMap['file.file_format']['mzXML']
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format']['mzXML']

            else:
               
               die("Can't identify format for metabolome file basename '%s'; aborting." % baseName)

            # end if ( we have an identifiable filename extension )

            # file.information_type

            infoTerm = enumMap['file.information_type']['metabolomicsData']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['metabolomicsData']
         
         ######################################################################
         ######################## nodeType: serology ##########################
         ######################################################################

         elif nodeType == 'serology':
            
            doNotReprocess |= { 'urls__000000000', 'checksums__md5', 'format' }

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # This isn't stored anywhere.

            objectsToWrite['file'][currentID]['length'] = ''

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if flatObjects[currentID]['format'] == 'tsv':
               
               formatTerm = enumMap['file.file_format']['tsv']
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format']['tsv']

            else:
               
               die("Can't identify format for serology file basename '%s'; aborting." % baseName)

            # end if ( we have an identifiable filename extension )

            # file.information_type

            infoTerm = enumMap['file.information_type']['serologyData']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['serologyData']
         
         ######################################################################
         ######################## nodeType: cytokine ##########################
         ######################################################################

         elif nodeType == 'cytokine':
            
            doNotReprocess |= {'urls__000000000', 'checksums__md5', 'format'}

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # This isn't stored anywhere.

            objectsToWrite['file'][currentID]['length'] = ''

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if flatObjects[currentID]['format'] == '':
               
               objectsToWrite['file'][currentID]['file_format'] = ''

            elif flatObjects[currentID]['format'] == 'tsv':
               
               formatTerm = enumMap['file.file_format']['tsv']
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format']['tsv']

            else:
               
               die("Can't identify format for cytokine file basename '%s'; aborting." % baseName)

            # end if ( we have an identifiable filename extension )

            # file.information_type

            infoTerm = enumMap['file.information_type']['cytokineData']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['cytokineData']
         
         ######################################################################
         ######################## nodeType: lipidome ##########################
         ######################################################################

         elif nodeType == 'lipidome':
            
            doNotReprocess |= {'urls__000000000', 'checksums__md5', 'format'}

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # This is always blank.

            objectsToWrite['file'][currentID]['file_format'] = ''

            # This isn't stored anywhere.

            objectsToWrite['file'][currentID]['length'] = ''

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.information_type

            infoTerm = enumMap['file.information_type']['lipidomeData']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['lipidomeData']
         
         ######################################################################
         ######################## nodeType: proteome_nonpride #################
         ######################################################################

         elif nodeType == 'proteome_nonpride':
            
            doNotReprocess |= {'raw_url__000000000'}

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['raw_url__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            # This isn't stored anywhere.

            objectsToWrite['file'][currentID]['md5'] = ''

            # This isn't stored anywhere.

            objectsToWrite['file'][currentID]['length'] = ''

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if re.search(r'\.mzML\.gz', baseName) is not None:
               
               formatTerm = enumMap['file.file_format']['mzML']
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format']['mzML']

            else:
               
               die("Can't identify format for proteome_nonpride file basename '%s'; aborting." % baseName)

            # end if ( we have an identifiable filename extension )

            # file.information_type

            infoTerm = enumMap['file.information_type']['proteomicsData']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['proteomicsData']
         
         ######################################################################
         ######################## nodeType: clustered_seq_set #################
         ######################################################################

         elif nodeType == 'clustered_seq_set':
            
            doNotReprocess |= {'checksums__md5', 'urls__000000000', 'format', 'sequence_type', 'size'}

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['length'] = flatObjects[currentID]['size']

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:

               formatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

            # file.information_type

            if flatObjects[currentID]['sequence_type'] == 'nucleotide':
               
               infoTerm = enumMap['file.information_type']['nucleotideSequence']
               objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['nucleotideSequence']
         
            elif flatObjects[currentID]['sequence_type'] == 'peptide':
               
               infoTerm = enumMap['file.information_type']['proteinSequence']
               objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['proteinSequence']

            else:
               
               die("Can't identify CV mapping for sequence_type '%s' of clustered_seq_set file basename '%s'; aborting." % ( flatObjects[currentID]['sequence_type'], baseName ) )

            # end if ( we have a usable 'sequence_type' value )
         
         ######################################################################
         ######################## nodeType: annotation ########################
         ######################################################################

         elif nodeType == 'annotation':
            
            doNotReprocess |= {'checksums__md5', 'urls__000000000', 'format', 'size'}

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['length'] = flatObjects[currentID]['size']

            infoTerm = enumMap['file.information_type']['sequenceFeatures']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['sequenceFeatures']

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               formatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

         ######################################################################
         ######################## nodeType: host_variant_call #################
         ######################################################################

         elif nodeType == 'host_variant_call':
            
            doNotReprocess |= {'checksums__md5', 'urls__000000000', 'format', 'size'}

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['length'] = flatObjects[currentID]['size']

            infoTerm = enumMap['file.information_type']['sequenceVariations']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['sequenceVariations']

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               formatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

         ######################################################################
         ######################## nodeType: alignment ########################
         ######################################################################

         elif nodeType == 'alignment':
            
            doNotReprocess |= {'checksums__md5', 'urls__000000000', 'format', 'size'}

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['length'] = flatObjects[currentID]['size']

            infoTerm = enumMap['file.information_type']['alignment']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['alignment']

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               formatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

         ######################################################################
         ######################## nodeType: viral_seq_set #####################
         ######################################################################

         elif nodeType == 'viral_seq_set':
            
            doNotReprocess |= {'checksums__md5', 'urls__000000000', 'format'}

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            # Not recorded for this node type.

            objectsToWrite['file'][currentID]['length'] = ''

            # Current data only has BAM files in this node type.

            infoTerm = enumMap['file.information_type']['alignment']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['alignment']

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               formatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

         ######################################################################
         ######################## nodeType: *_seq_set (not caught above) ######
         ######################################################################

         elif re.search(r'_seq_set$', nodeType) is not None:
            
            doNotReprocess |= {'checksums__md5', 'urls__000000000', 'format', 'size'}

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['length'] = flatObjects[currentID]['size']

            # These are all sequence files.

            infoTerm = enumMap['file.information_type']['sequence']
            objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type']['sequence']

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if flatObjects[currentID]['format'] == '':
               
               objectsToWrite['file'][currentID]['file_format'] = ''
               
            elif flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               formatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

         ######################################################################
         ######################## nodeType: abundance_matrix ##################
         ######################################################################

         elif nodeType == 'abundance_matrix':
            
            doNotReprocess |= {'urls__000000000', 'checksums__md5', 'size', 'format'}

            #########################
            # Simple-import metadata:

            currentURL = flatObjects[currentID]['urls__000000000']

            if re.search(r'<private>', currentURL) is not None:
               
               currentURL = ''

            objectsToWrite['file'][currentID]['location'] = currentURL

            baseName = re.sub(r'^.*\/', r'', currentURL)

            objectsToWrite['file'][currentID]['filename'] = baseName

            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

            objectsToWrite['file'][currentID]['length'] = flatObjects[currentID]['size']

            ################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               formatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]
               objectsToWrite['file'][currentID]['file_format'] = baseURL['EDAM'] + enumMap['file.file_format'][flatObjects[currentID]['format']]

            elif ( not flatObjects[currentID]['format'] is None ) and ( not flatObjects[currentID]['format'] == '' ):
               
               die("Did not load CV term information for (%s) %s.format '%s'; aborting." % ( nodeType, currentID, flatObjects[currentID]['format'] ))

            else:
               
               objectsToWrite['file'][currentID]['file_format'] = ''

            # end if ( switch on sanity of currentID.'format' )

            # file.information_type

            if flatObjects[currentID]['matrix_type'] in enumMap['file.information_type']:
               
               infoTerm = enumMap['file.information_type'][flatObjects[currentID]['matrix_type']]
               objectsToWrite['file'][currentID]['information_type'] = baseURL['EDAM'] + enumMap['file.information_type'][flatObjects[currentID]['matrix_type']]

            elif ( not flatObjects[currentID]['matrix_type'] is None ) and ( not flatObjects[currentID]['matrix_type'] == '' ):
               
               die("Did not load CV term information for (%s) %s.matrix_type '%s'; aborting." % ( nodeType, currentID, flatObjects[currentID]['matrix_type'] ))

            else:
               
               objectsToWrite['file'][currentID]['information_type'] = ''

            # end if ( switch on sanity of currentID.'matrix_type' )

         # end if ( nodeType switch )

         if 'file_format' not in termsUsed:
            
            termsUsed['file_format'] = {}

         # end if ( setup check on termsUsed['file_format'] )

         if 'information_type' not in termsUsed:
            
            termsUsed['information_type'] = {}

         # end if ( setup check on termsUsed['information_type'] )

         if formatTerm != '':
            
            termsUsed['file_format'][formatTerm] = {}
            fullURL['file_format'][formatTerm] = baseURL['EDAM'] + formatTerm

         # end if ( null check on formatTerm )

         if infoTerm != '':
            
            termsUsed['information_type'][infoTerm] = {}
            fullURL['information_type'][infoTerm] = baseURL['EDAM'] + infoTerm

         # end if ( null check on infoTerm )

         ############################################################################
         # Metadata not mapped to C2M2 objects: send to generic auxiliary data table.

         objectsToWrite['auxiliary_data']['file'][currentID] = {}

         for fieldName in sorted(flatObjects[currentID].keys()):
            
            if ( fieldName not in doNotReprocess ) and ( re.search(r'^linkage', fieldName) is None ):
               
               currentIndex = '0'

               currentValue = flatObjects[currentID][fieldName]

               matchResult = re.search(r'_+(\d+)$', fieldName)

               if not ( matchResult is None ):
                  
                  # Strip the encoded index from the end of the term and serialize as a (table-encoded) array.

                  currentIndex = str(int(matchResult.group(1)))

                  fieldName = re.sub(r'_+\d+$', r'', fieldName)

               # end if ( fieldName has a suffix-encoded index )

               if fieldName not in objectsToWrite['auxiliary_data']['file'][currentID]:
                  
                  objectsToWrite['auxiliary_data']['file'][currentID][fieldName] = { currentIndex: currentValue }

               else:
                  
                  objectsToWrite['auxiliary_data']['file'][currentID][fieldName][currentIndex] = currentValue

               # end if ( we've created an entry yet for currentID.fieldName )

            elif re.search(r'^linkage', fieldName) is not None:
               
               linkedID = flatObjects[currentID][fieldName]

               if currentID not in parents:
                  
                  parents[currentID] = set()

               # end if ( setup check for parents[currentID] )

               parents[currentID] |= { linkedID }

               # Hook up a data_event that generated this file and link the current target as a processing input.

               if 'file' not in producedBy:
                  
                  producedBy['file'] = {}

               # end if ( setup check for producedBy['file'] )

               if 'data_event' not in objectsToWrite:
                  
                  objectsToWrite['data_event'] = {}

               # end if ( we haven't yet created a 'data_event' substructure in objectsToWrite )

               if currentID not in producedBy['file']:

                  # This is the first input-linked target for currentID.

                  # Create a data_event to log the file's creation.

                  eventID = getNewID('data_event.')

                  objectsToWrite['data_event'][eventID] = {}

                  objectsToWrite['data_event'][eventID]['protocol'] = dummyProtocolID

                  objectsToWrite['data_event'][eventID]['rank'] = '0'

                  objectsToWrite['data_event'][eventID]['event_ts'] = ''

                  # Hook it up.

                  producedBy['file'][currentID] = eventID

                  # Register that the current link target was processed by the same event.

                  linkedNodeType = allowableNodeTypes[nodeIDToNativeType[linkedID]]

                  if linkedNodeType not in processedBy:
                     
                     processedBy[linkedNodeType] = {}
                     processedBy[linkedNodeType][linkedID] = { eventID }

                  elif linkedID not in processedBy[linkedNodeType]:
                     
                     processedBy[linkedNodeType][linkedID] = { eventID }

                  else:
                     
                     processedBy[linkedNodeType][linkedID] |= { eventID }

                  # end if ( setup check for saving eventID to processedBy[linkedNodeType][linkedID] )

               else:
                  
                  # A creation data_event has already been created for currentID. Add to it.

                  eventID = producedBy['file'][currentID]

                  # Register that the current link target was processed by the same event.

                  linkedNodeType = allowableNodeTypes[nodeIDToNativeType[linkedID]]

                  if linkedNodeType not in processedBy:
                     
                     processedBy[linkedNodeType] = {}
                     processedBy[linkedNodeType][linkedID] = { eventID }

                  elif linkedID not in processedBy[linkedNodeType]:
                     
                     processedBy[linkedNodeType][linkedID] = { eventID }

                  else:
                     
                     processedBy[linkedNodeType][linkedID] |= { eventID }

                  # end if ( setup check for saving eventID to processedBy[linkedNodeType][linkedID] )

               # end if ( a creation data_event exists for currentID )

            # end if ( we're looking at a fieldName we should be processing )

         # end for ( each fieldName in flatObjects for currentID )

      # end for ( currentID in nativeTypeToNodeID[nodeType] )

   # end for ( nodeType in FileNodeTypes )

# end sub populateFiles( nodeType )

# Process all native-HMP 'visit' data into data_event objects; save
# extra metadata to auxiliary_data..

def processVisits(  ):
   
   global flatObjects, objectsToWrite, nativeTypeToNodeID, parents, dummyProtocolID, visitToDataEvent, visitToSubject

   if 'data_event' not in objectsToWrite:
      
      objectsToWrite['data_event'] = {}

   # end if ( we haven't yet created a 'data_event' substructure in objectsToWrite )

   if 'auxiliary_data' not in objectsToWrite:
      
      objectsToWrite['auxiliary_data'] = {}
      objectsToWrite['auxiliary_data']['data_event'] = {}

   elif 'data_event' not in objectsToWrite['auxiliary_data']:
      
      objectsToWrite['auxiliary_data']['data_event'] = {}

   # end if ( we haven't yet created an 'auxiliary_data.file' substructure in objectsToWrite )

   nodeType = 'visit'

   if nodeType not in nativeTypeToNodeID:
      
      die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

   # end if ( nodeType not in nativeTypeToNodeID )

   for flatID in nativeTypeToNodeID[nodeType]:
      
      doNotReprocess = { 'visit_number', 'linkage__by__000000000' }

      visitToSubject[flatID] = flatObjects[flatID]['linkage__by__000000000']

      if flatID not in parents:
         
         parents[flatID] = set()

      # end if ( setup check for parents[flatID] )

      parents[flatID] |= { flatObjects[flatID]['linkage__by__000000000'] }

      currentID = getNewID('data_event.')

      visitToDataEvent[flatID] = currentID

      objectsToWrite['data_event'][currentID] = {}

      objectsToWrite['data_event'][currentID]['protocol'] = dummyProtocolID

      objectsToWrite['data_event'][currentID]['rank'] = flatObjects[flatID]['visit_number']

      objectsToWrite['data_event'][currentID]['event_ts'] = ''

      ############################################################################
      # Metadata not mapped to C2M2 objects: send to generic auxiliary data table.

      objectsToWrite['auxiliary_data']['data_event'][currentID] = {}

      for fieldName in sorted(flatObjects[flatID].keys()):
         
         if ( fieldName not in doNotReprocess ) and ( re.search(r'^linkage', fieldName) is None ):
            
            currentIndex = '0'

            currentValue = flatObjects[flatID][fieldName]

            matchResult = re.search(r'_+(\d+)$', fieldName)

            if not ( matchResult is None ):
               
               # Strip the encoded index from the end of the term and serialize as a (table-encoded) array.

               currentIndex = str(int(matchResult.group(1)))

               fieldName = re.sub(r'_+\d+$', r'', fieldName)

            # end if ( fieldName has a suffix-encoded index )

            if fieldName not in objectsToWrite['auxiliary_data']['data_event'][currentID]:
               
               objectsToWrite['auxiliary_data']['data_event'][currentID][fieldName] = { currentIndex: currentValue }

            else:
               
               objectsToWrite['auxiliary_data']['data_event'][currentID][fieldName][currentIndex] = currentValue

            # end if ( we've created an entry yet for flatID.fieldName )

         # end if ( we're looking at a fieldName we should be processing )

      # end for ( each fieldName in flatObjects for currentID )

   # end for ( flatID in nativeTypeToNodeID[nodeType] )

# end sub processVisits(  )

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

            if colName == 'id':
               
               # Special case. Will need to change this if 'id' fields are eliminated or renamed.

               OUT.write( currentID )

            elif colName in objectsToWrite[objectName][currentID]:
               
               OUT.write( '%s' % objectsToWrite[objectName][currentID][colName] )

            # end if ( switch on colName )

         # end for ( loop through column names for this object type in order )

         OUT.write( '\n' )

      # end for ( each objectID of this object type )

   # end with ( outFile opened as 'OUT' )

# end sub writeTable( tableName )

def writeAuxTable(  ):
   
   global outDir, objectsToWrite, outputColumns

   if 'auxiliary_data' not in objectsToWrite:
      
      die("No data loaded for output table 'auxiliary_data'; aborting.")

   # end if ( we have no data loaded into objectsToWrite for the auxiliary_data table )

   outFile = '%s/auxiliary_data.tsv' % outDir

   with open(outFile, 'w') as OUT:
      
      OUT.write( '\t'.join( outputColumns['auxiliary_data'] ) + '\n' )

      # objectsToWrite['auxiliary_data'][objectType][currentID][fieldName][currentIndex] = currentValue

      for objectType in sorted(objectsToWrite['auxiliary_data'].keys()):
         
         for currentID in sorted(objectsToWrite['auxiliary_data'][objectType].keys()):
            
            for fieldName in sorted(objectsToWrite['auxiliary_data'][objectType][currentID].keys()):
               
               for currentIndex in sorted(objectsToWrite['auxiliary_data'][objectType][currentID][fieldName].keys()):
                  
                  OUT.write( '\t'.join([objectType, currentID, fieldName, currentIndex, objectsToWrite['auxiliary_data'][objectType][currentID][fieldName][currentIndex]]) + '\n' )

               # end for ( each currentIndex in objectsToWrite['auxiliary_data'][objectType][currentID][fieldName] )

            # end for ( each field in objectsToWrite['auxiliary_data'][objectType][currentID] )

         # end for ( each currentID in objectsToWrite['auxiliary_data'][objectType] )

      # end for ( each objectType in objectsToWrite['auxiliary_data'] )

   # end with ( outFile opened as 'OUT' )

# end sub writeAuxTable(  )

# Process all parent[] links to establish dataset containment for all serialized objects.

def processDatasetContainment(  ):
   
   global parents, containedIn, objectsInDatasets

   for objectID in parents.keys():
      
      containingSets = set()

      containingSets |= findContainingSets(objectID, containingSets)

      # This structure will end up storing objectID. Remove it.

      containingSets -= { objectID }

      # Cache results to speed later lookups.

      containedIn[objectID] = containingSets.copy()

      targetType = allowableNodeTypes[nodeIDToNativeType[objectID]]

      if targetType != 'noType':
         
         # Store in objectsInDatasets

         if targetType not in objectsInDatasets:
            
            objectsInDatasets[targetType] = {}
            objectsInDatasets[targetType][objectID] = set()

         elif objectID not in objectsInDatasets[targetType]:
            
            objectsInDatasets[targetType][objectID] = set()

         # end if ( setup check for objectsInDatasets[targetType][objectID] )

         objectsInDatasets[targetType][objectID] |= containingSets

      # end if ( we should be ignoring this object when building an output data structure )

   # end for ( each ingested object )

# end sub 

# Recursively scan containment DAG to establish
# top-level dataset containment for a given object.

def findContainingSets( objectID, containingSets ):
   
   global parents, nodeIDToNativeType, DatasetNodeTypes, containedIn, allowableNodeTypes

   if objectID in containedIn:
      
      containingSets |= containedIn[objectID]

   else:
      
      if allowableNodeTypes[nodeIDToNativeType[objectID]] == 'dataset':
         
         containingSets |= { objectID }

      # end if ( this is a dataset type )
      
      if objectID in parents:
         
         for parent in parents[objectID]:
            
            containingSets |= findContainingSets(parent, containingSets)

         # end for ( each parent of objectID )

      # end if ( objectID has any parents )

   # end if ( containedIn already has a record for objectID )

   return containingSets

# end sub findContainingSets( objectID, containingSets )

def writeObjectsInDatasets(  ):

   global outDir, objectsInDatasets

   for nodeType in sorted(objectsInDatasets.keys()):
      
      outFile = '%s/%ss_in_datasets.tsv' % ( outDir, nodeType )

      with open(outFile, 'w') as OUT:
         
         OUT.write( '\t'.join( ['%s_id' % nodeType, 'containing_dataset_id' ] ) + '\n' )

         for objectID in sorted(objectsInDatasets[nodeType].keys()):
            
            for datasetID in sorted(objectsInDatasets[nodeType][objectID]):
               
               OUT.write( '\t'.join( [objectID, datasetID ] ) + '\n' )

            #
         #
      #
   #

# end sub writeObjectsInDatasets(  )

def writeSubjectTaxonomy(  ):

   global outDir, objectsToWrite

   outFile = '%s/subject_taxonomy.tsv' % outDir

   with open(outFile, 'w') as OUT:
      
      OUT.write( '\t'.join( ['subject_id', 'ncbi_taxon_id' ] ) + '\n' )

      for subjectID in sorted(objectsToWrite['subject']):
         
         OUT.write( '\t'.join( [subjectID, baseURL['ncbi_taxonomy_db'] + '9606' ] ) + '\n' )
      #
   #

# end sub writeSubjectTaxonomy(  )

def writeProducedBy(  ):

   global outDir, producedBy

   # producedBy[nodeType][objectID] = eventID

   outFile = '%s/produced_by.tsv' % outDir

   with open(outFile, 'w') as OUT:
      
      OUT.write( '\t'.join( ['id', 'type', 'produced_by' ] ) + '\n' )

      for nodeType in sorted(producedBy.keys()):
         
         for objectID in sorted(producedBy[nodeType].keys()):
            
            eventID = producedBy[nodeType][objectID]

            OUT.write( '\t'.join( [objectID, nodeType, eventID ] ) + '\n' )
         #
      #
   #

# end sub writeProducedBy(  )

def writeProcessedBy(  ):

   global outDir, processedBy

   # processedBy[nodeType][objectID] = { eventID_1, eventID_2, ... }

   outFile = '%s/processed_by.tsv' % outDir

   with open(outFile, 'w') as OUT:
      
      OUT.write( '\t'.join( ['id', 'type', 'processed_by' ] ) + '\n' )

      for nodeType in sorted(processedBy.keys()):
         
         for objectID in sorted(processedBy[nodeType].keys()):
            
            for eventID in sorted(processedBy[nodeType][objectID]):
               
               OUT.write( '\t'.join( [objectID, nodeType, eventID ] ) + '\n' )
            #
         #
      #
   #

# end sub writeProcessedBy(  )

def linkBioSamples(  ):
   
   global objectsToWrite, parents, prepToBioSample, producedBy, allowableNodeTypes, nodeIDToNativeType, processedBy, sampleToVisit, dummyProtocolID, visitToSubject, visitToDataEvent

   if 'bio_sample' not in producedBy:
      
      producedBy['bio_sample'] = {}

   # end if ( setup check for producedBy['bio_sample'] )

   if 'bio_sample' not in processedBy:
      
      processedBy['bio_sample'] = {}

   # end if ( setup check for processedBy['bio_sample'] )

   if 'subject' not in processedBy:
      
      processedBy['subject'] = {}

   # end if ( setup check for processedBy['subject'] )

   for sampleID in objectsToWrite['bio_sample']:
      
      if sampleID in prepToBioSample:
         
         parentSampleID = prepToBioSample[sampleID]

         # Create a data_event to log the BioSample's creation.

         eventID = getNewID('data_event.')

         objectsToWrite['data_event'][eventID] = {}

         objectsToWrite['data_event'][eventID]['protocol'] = dummyProtocolID

         objectsToWrite['data_event'][eventID]['rank'] = '0'

         objectsToWrite['data_event'][eventID]['event_ts'] = ''

         # Hook it up.

         producedBy['bio_sample'][sampleID] = eventID

         parentNodeType = allowableNodeTypes[nodeIDToNativeType[parentSampleID]]

         if parentNodeType not in processedBy:
            
            processedBy[parentNodeType] = {}
            processedBy[parentNodeType][parentSampleID] = { eventID }

         elif parentSampleID not in processedBy[parentNodeType]:
            
            processedBy[parentNodeType][parentSampleID] = { eventID }

         else:
            
            processedBy[parentNodeType][parentSampleID] |= { eventID }

         # end switch on setup for processedBy[parentNodeType][parentSampleID]

      elif sampleID in sampleToVisit:
         
         visitID = sampleToVisit[sampleID]

         subjectID = visitToSubject[visitID]

         eventID = visitToDataEvent[visitID]

         producedBy['bio_sample'][sampleID] = eventID

         processedBy['subject'][subjectID] = { eventID }

      else:
         
         die("Can't handle sample type for object ID '%s'; aborting." % sampleID)

      # end if ( switch on sample type )

   # end for ( each sampleID in objectsToWrite['bio_sample'] )

# end sub linkBioSamples(  )

def processAttrObjects(  ):
   
   global objectsToWrite, nativeTypeToNodeID, flatObjects, visitToDataEvent

   if 'auxiliary_data' not in objectsToWrite:
      
      objectsToWrite['auxiliary_data'] = {}

   if 'data_event' not in objectsToWrite['auxiliary_data']:
      
      objectsToWrite['auxiliary_data']['data_event'] = {}

   if 'subject' not in objectsToWrite['auxiliary_data']:
      
      objectsToWrite['auxiliary_data']['subject'] = {}

   if 'bio_sample' not in objectsToWrite['auxiliary_data']:
      
      objectsToWrite['auxiliary_data']['bio_sample'] = {}

   # end if ( all setup checks on objectsToWrite['auxiliary_data'][*] )

   for nodeType in ['sample_attr', 'subject_attr', 'visit_attr']:
      
      if nodeType not in nativeTypeToNodeID:
         
         die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

      # end if ( nodeType not in nativeTypeToNodeID )

      decorationTarget = 'bio_sample'

      if nodeType == 'subject_attr':
         
         decorationTarget = 'subject'

      elif nodeType == 'visit_attr':
         
         decorationTarget = 'data_event'

      for currentID in nativeTypeToNodeID[nodeType]:
         
         objectsToWrite['auxiliary_data'][decorationTarget][currentID] = {}

         linkedObjectID = flatObjects[currentID]['linkage__associated_with__000000000']

         if nodeType == 'visit_attr':
            
            # This should never fail. If it does, the program should break.

            linkedObjectID = visitToDataEvent[linkedObjectID]

         # end if ( type check for visit_attr -> map actual data_event ID to linked target )

         for fieldName in sorted(flatObjects[currentID].keys()):
            
            if re.search(r'^linkage', fieldName) is None:
               
               currentIndex = '0'

               currentValue = flatObjects[currentID][fieldName]

               matchResult = re.search(r'_+(\d+)$', fieldName)

               if not ( matchResult is None ):
                  
                  # Strip the encoded index from the end of the term and serialize as a (table-encoded) array.

                  currentIndex = str(int(matchResult.group(1)))

                  fieldName = re.sub(r'_+\d+$', r'', fieldName)

               # end if ( fieldName has a suffix-encoded index )

               if fieldName not in objectsToWrite['auxiliary_data'][decorationTarget][currentID]:
                  
                  objectsToWrite['auxiliary_data'][decorationTarget][currentID][fieldName] = { currentIndex: currentValue }

               else:
                  
                  objectsToWrite['auxiliary_data'][decorationTarget][currentID][fieldName][currentIndex] = currentValue

               # end if ( we've created an entry yet for currentID.fieldName )

            # end if ( we're not in a 'linkage__*' field )

         # end for ( fieldName in sorted(flatObjects[currentID].keys()) )

      # end for ( currentID in nativeTypeToNodeID[nodeType] )

   # end for ( nodeType in ['sample_attr', 'subject_attr', 'visit_attr' )

# end sub processAttrObjects(  )

def decorateTermsUsed(  ):
   
   global termsUsed, fullURL, cvFile

   for categoryID in termsUsed:
      
      if categoryID == 'anatomy' or categoryID == 'sample_type':
         
         cv = ''

         if categoryID == 'anatomy':
            
            cv = 'Uberon'

         elif categoryID == 'sample_type':
            
            cv = 'OBI'

         # end if ( categoryID type check )

         refFile = cvFile[cv]

         with open( refFile, 'r' ) as IN:
            
            recording = False

            currentTerm = ''

            for line in IN:
               
               line = line.rstrip('\r\n')
            
               matchResult = re.search(r'^id:\s+(\S.*)$', line)

               if not( matchResult is None ):
                  
                  currentTerm = matchResult.group(1)

                  currentTerm = re.sub( r':', r'_', currentTerm )

                  if currentTerm in termsUsed[categoryID]:
                     
                     recording = True

                     if 'synonyms' not in termsUsed[categoryID][currentTerm]:
                        
                        termsUsed[categoryID][currentTerm]['synonyms'] = ''

                     if not ( currentTerm in fullURL[categoryID] ):

                        die('No URL cached for used CV (%s) term "%s"; cannot proceed, aborting.' % ( cv, currentTerm ) )

                  else:
                     
                     currentTerm = ''

                     # (Recording is already switched off by default.)

               elif not( re.search(r'^\[Term\]', line) is None ):
                  
                  recording = False

               elif recording:
                  
                  if not ( re.search(r'^name:\s+(\S*.*)$', line) is None ):
                     
                     termsUsed[categoryID][currentTerm]['name'] = re.search(r'^name:\s+(\S*.*)$', line).group(1)

                  elif not ( re.search(r'^def:\s+\"(.*)\"[^\"]*$', line) is None ):
                     
                     termsUsed[categoryID][currentTerm]['description'] = re.search(r'^def:\s+\"(.*)\"[^\"]*$', line).group(1)

                  elif not ( re.search(r'^def:\s+', line) is None ):
                     
                     die('Unparsed def-line in %s OBO file: "%s"; aborting.' % ( cv, line ) )

                  elif not ( re.search(r'^synonym:\s+\"(.*)\"[^\"]*$', line) is None ):
                     
                     synonym = re.search(r'^synonym:\s+\"(.*)\"[^\"]*$', line).group(1)

                     if termsUsed[categoryID][currentTerm]['synonyms'] != '':
                        
                        termsUsed[categoryID][currentTerm]['synonyms'] = termsUsed[categoryID][currentTerm]['synonyms'] + '|' + synonym

                     else:
                        
                        termsUsed[categoryID][currentTerm]['synonyms'] = synonym

               # end if ( line-type selector switch )

            # end for ( input file line iterator )

         # end with ( open refFile as IN )

      elif categoryID == 'file_format' or categoryID == 'information_type':
         
         cv = 'EDAM'

         refFile = cvFile[cv]

         with open( refFile, 'r' ) as IN:
            
            header = IN.readline()

            for line in IN:
               
               line = line.rstrip('\r\n')

               ( termURL, name, synonyms, definition ) = re.split(r'\t', line)[0:4]

               currentTerm = re.sub(r'^.*\/([^\/]+)$', r'\1', termURL)

               if currentTerm in termsUsed[categoryID]:
                  
                  # There are some truly screwy things allowed inside
                  # tab-separated fields in this file. Clean them up.

                  name = name.strip().strip('"\'').strip()

                  synonyms = synonyms.strip().strip('"\'').strip()

                  definition = definition.strip().strip('"\'').strip()

                  termsUsed[categoryID][currentTerm]['name'] = name;
                  termsUsed[categoryID][currentTerm]['description'] = definition;
                  termsUsed[categoryID][currentTerm]['synonyms'] = synonyms;

               # end if ( currentTerm in termsUsed[categoryID] )

            # end for ( input file line iterator )

         # end with ( refFile opened as IN )

      # end if ( switch on categoryID )

   # end foreach ( categoryID in termsUsed )

# end sub decorateTermsUsed(  )

def serializeTermsUsed(  ):
   
   global termsUsed, outputColumns, termsUsed, fullURL, outDir

   for tableName in termsUsed:
      
      if tableName not in outputColumns:
         
         die("Can't find '%s' in outputColumns; aborting." % tableName)

      # end if ( we recognize this term category )

      targetFile = "%s/%s.tsv" % ( outDir, tableName )

      with open( targetFile, 'w' ) as OUT:
         
         OUT.write( '\t'.join( outputColumns[tableName] ) + '\n' )

         for currentID in sorted(termsUsed[tableName]):
            
            if tableName not in fullURL or currentID not in fullURL[tableName]:
               
               OUT.write(currentID)

            else:
               
               OUT.write(fullURL[tableName][currentID])

            for colName in outputColumns[tableName]:
               
               if colName != 'id':
                  
                  OUT.write( '\t' + termsUsed[tableName][currentID][colName] )

            # end foreach ( term to write )

            OUT.write('\n')

         # end foreach ( ID in termsUsed[tableName] )

      # end with ( write to targetFile as OUT )

   # end foreach ( tableName in termsUsed )

# end sub serializeTermsUsed(  )

def writeStubTables(  ):
   
   outFile = '%s/%s.tsv' % ( outDir, 'subject_group' )

   with open( outFile, 'w' ) as OUT:
      
      OUT.write( '\t'.join(['id', 'title', 'description']) + '\n' )

   outFile = '%s/%s.tsv' % ( outDir, 'subjects_in_subject_groups' )

   with open( outFile, 'w' ) as OUT:
      
      OUT.write( '\t'.join(['subject_id', 'subject_group_id']) + '\n' )

# end sub ( writeStubTables )

# Create three separate fake date-stamps for all iHMP-associated
# file objects, for interface testing. THIS FUNCTION SHOULD NOT
# BE CALLED IN A PRODUCTION VERSION OF THIS SCRIPT (check main
# execution block below for invocation).

def createFakeDates(  ):
   
   global objectsToWrite, objectsInDatasets

   setID = {
      
      'ibd': '1419f08f554e0c93f3b62fe90c004066',
      't2d': '194149ed5273e3f94fc60a9ba58f7c24',
      'momspi': '88af6472fb03642dd5eaf8cddcbf64a5'
   }

   creationDate = {
      
      'ibd': '2014-01-15',
      't2d': '2015-02-20',
      'momspi': '2016-07-08'
   }

   repoSubmissionDate = {
      
      'ibd': '2014-04-04',
      't2d': '2015-06-09',
      'momspi': '2016-11-21'
   }

   cfdeIngestDate = {
      
      'ibd': '2019-08-01',
      't2d': '2019-09-04',
      'momspi': '2019-11-05'
   }

   # objectsToWrite['file'][fileID] = { ... }

   # objectsInDatasets['file'][fileID] = { containing_set_1, containing_set_2, ... }

   # objectsToWrite['auxiliary_data']['file'][fileID][fieldName]['0'] = dateVal

   for fileID in objectsToWrite['file']:
      
      for setKeyword in ['ibd', 't2d', 'momspi']:
         
         if setID[setKeyword] in objectsInDatasets['file'][fileID]:
            
            if fileID not in objectsToWrite['auxiliary_data']['file']:
               
               objectsToWrite['auxiliary_data']['file'][fileID] = {}

            objectsToWrite['auxiliary_data']['file'][fileID]['creationDate'] = { '0': creationDate[setKeyword] }
            objectsToWrite['auxiliary_data']['file'][fileID]['repoSubmissionDate'] = { '0': repoSubmissionDate[setKeyword] }
            objectsToWrite['auxiliary_data']['file'][fileID]['cfdeIngestDate'] = { '0': cfdeIngestDate[setKeyword] }
         #
      #
   #

# end sub createFakeDates(  )

##########################################################################################
# PARAMETERS
##########################################################################################

##########################################################################################
# Location of the Table-Schema JSON file describing the output set.

tableSchemaLoc = '000_tableschema/datapackage.json'

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
# Directory containing TSVs mapping named GTEx fields to terms in
# third-party ontologies

mapDir = '002_HMP_maps_from_HMP_enums_and_CVs_to_C2M2_controlled_vocabs'

##########################################################################################
# Map-file locations, keyed by the name of the output-object property field
# meant to store references to the relevant ontology

mapFiles = {
   
   'file.file_format' : '%s/File_format_keyword_to_EDAM_for_File.file_format.tsv' % mapDir,
   'file.information_type' : '%s/File_information_type_keyword_to_EDAM_for_File.information_type.tsv' % mapDir,
   'bio_sample.anatomy' : '%s/BioSample_anatomy_keyword_to_Uberon_for_BioSample.anatomy.tsv' % mapDir,
   'bio_sample.sample_type' : '%s/BioSample_type_keyword_to_OBI_for_BioSample.sample_type.tsv' % mapDir
}

# Functions mapping value-constrained DCC-internal metadata field values
# to third-party CV term IDs for populating selected data fields in the
# output model.

enumMap = {}

##########################################################################################
# Base URLs (to be followed by term IDs) used to reconstruct full URLs
# referencing controlled-vocabulary terms in third-party ontologies

baseURL = {
   
   'EDAM' : 'http://edamontology.org/',
   'OBI' : 'http://purl.obolibrary.org/obo/',
   'Uberon' : 'http://purl.obolibrary.org/obo/',
   'ncbi_taxonomy_db' : 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id='
}

##########################################################################################
# Subdirectory containing full info for CVs, versioned to match the current data release.

cvRefDir = '003_CV_reference_data'

##########################################################################################
# Map of CV names to reference files.

cvFile = {
   
   'EDAM' : '%s/EDAM.version_1.21.tsv' % cvRefDir,
   'OBI' : '%s/OBI.version_2019-08-15.obo' % cvRefDir,
   'Uberon' : '%s/uberon.version_2019-06-27.obo' % cvRefDir
}

##########################################################################################
# Output directory.

outDir = '004_HMP__C2M2_preload__preBag_output_files'

##########################################################################################
# Auxiliary development stuff.

flatDir = 'zz98_flattened_HMP_object_TSVs'

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
   'wgs_raw_seq_set': 'file',
   
   '16s_dna_prep': 'bio_sample',
   'host_assay_prep': 'bio_sample',
   'host_seq_prep': 'bio_sample',
   'microb_assay_prep': 'bio_sample',
   'sample': 'bio_sample',
   'wgs_dna_prep': 'bio_sample',

   'subject': 'subject',

   # Skipping these two for the moment; they're not
   # connected properly to the rest of the data and
   # will require some extra treatment.
   #   
   #   'metagenomic_project_catalog_entry': 'dataset',
   #   'reference_genome_project_catalog_entry': 'dataset',
   'project': 'dataset',
   'study': 'dataset',

   'visit': 'noType',

   'sample_attr': 'noType',
   'subject_attr': 'noType',
   'visit_attr': 'noType'
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

# Native HMP object types that map to C2M2 'bio_sample' objects.

BioSampleNodeTypes = {
   
   '16s_dna_prep',
   'host_assay_prep',
   'host_seq_prep',
   'microb_assay_prep',
   'sample',
   'wgs_dna_prep'
}
   
# Native HMP object types that map to C2M2 'subject' objects.

SubjectNodeTypes = {
   
   'subject'
}

# Native HMP object types that map to C2M2 'dataset' objects.

DatasetNodeTypes = {

   # Skipping these two for the moment; they're not
   # connected properly to the rest of the data and
   # will require some extra treatment.
   #   
   #   'metagenomic_project_catalog_entry',
   #   'reference_genome_project_catalog_entry',

   'project',
   'study'
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
   
   'anatomy': [
      'id',
      'name',
      'description',
      'synonyms'
   ],
   'common_fund_program': [
      'id',
      'website',
      'name',
      'description'
   ],
   'data_event': [
      'id',
      'protocol',
      'rank',
      'event_ts'
   ],
   'dataset': [
      'id',
      'data_source',
      'title',
      'description'
   ],
   'file': [
      'id',
      'location',
      'information_type',
      'file_format',
      'length',
      'filename',
      'md5'
   ],
   'bio_sample': [
      'id',
      'sample_type',
      'anatomy',
      'protocol'
   ],
   'file_format': [
      'id',
      'name',
      'description',
      'synonyms'
   ],
   'information_type': [
      'id',
      'name',
      'description',
      'synonyms'
   ],
   'ncbi_taxonomy_db': [
      'id',
      'name',
      'description',
      'synonyms'
   ],
   'protocol': [
      'id',
      'name',
      'description'
   ],
   'sample_type': [
      'id',
      'name',
      'description',
      'synonyms'
   ],
   'subject': [
      'id',
      'granularity'
   ],
   'subject_granularity': [
      'id',
      'name',
      'description'
   ],
   'auxiliary_data': [
      'object_type',
      'id',
      'keyword',
      'index',
      'value'
   ]
}

##########################################################################################
# Serialization objects: initial load of HMP metadata

flatObjects = {}
nativeTypeToNodeID = {}
nodeIDToNativeType = {}
nativeTypeToNativeColNames = {}

sampleToVisit = {}
prepToBioSample = {}
visitToSubject = {}

visitToDataEvent = {}

termsUsed = {}

# Static presets for HMP (values known in advance):

# NCBI Taxonomy DB

humanTaxID = '9606'
humanURL = baseURL['ncbi_taxonomy_db'] + humanTaxID

termsUsed['ncbi_taxonomy_db'] = {}
termsUsed['ncbi_taxonomy_db'][humanURL] = {}
termsUsed['ncbi_taxonomy_db'][humanURL]['name'] = 'Homo sapiens'
termsUsed['ncbi_taxonomy_db'][humanURL]['description'] = 'Homo sapiens (modern human species)'
termsUsed['ncbi_taxonomy_db'][humanURL]['synonyms'] = ''

# List of Common Fund programs

hmpCFProgramID = 'COMMON_FUND_PROGRAM_ID.1'

termsUsed['common_fund_program'] = {}
termsUsed['common_fund_program'][hmpCFProgramID] = {}
termsUsed['common_fund_program'][hmpCFProgramID]['website'] = 'https://hmpdacc.org/'
termsUsed['common_fund_program'][hmpCFProgramID]['name'] = 'NIH Human Microbiome Project'
termsUsed['common_fund_program'][hmpCFProgramID]['description'] = 'Characterization of microbiome and human hosts using multiple sequencing and omics methods'

# Protocol specs (stubbed for now!)

dummyProtocolID = 'DUMMY_PROTOCOL_ID.00000000'

termsUsed['protocol'] = {}
termsUsed['protocol'][dummyProtocolID] = {}
termsUsed['protocol'][dummyProtocolID]['name'] = 'dummy_protocol'
termsUsed['protocol'][dummyProtocolID]['description'] = 'This is a stub pending further elaboration of the model for specifying experimental protocols.'

# Subject granularity (the only HMP entities described as 'subject' are individual humans) 

singleOrgGranularityID = 'SUBJECT_GRANULARITY_ID.00000000'

termsUsed['subject_granularity'] = {}
termsUsed['subject_granularity'][singleOrgGranularityID] = {}
termsUsed['subject_granularity'][singleOrgGranularityID]['name'] = 'single_organism'
termsUsed['subject_granularity'][singleOrgGranularityID]['description'] = 'A subject representing a single organism'

##########################################################################################
# Serialization objects: C2M2 output data structures

objectsToWrite = {}
objectsInDatasets = {}

parents = {}

containedIn = {}
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

# Load functions mapping internal DCC-specific enums to terms
# in pre-selected controlled vocabularies.

progressReport("Loading enum map data...")

loadEnumMaps()

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

# Gather all data needed to serialize 'bio_sample' objects.

progressReport("Building bio_sample table...")

populateBioSamples()

# Serialize all 'bio_sample' objects into a TSV.

progressReport("Writing bio_sample table...")

writeTable('bio_sample')

# Gather all data needed to serialize 'subject' objects.

progressReport("Building subject table...")

populateSubjects()

# Serialize all 'subject' objects into a TSV.

progressReport("Writing subject table...")

writeTable('subject')

# Process all HMP container structures which will be
# mapped to C2M2 'dataset' objects.

progressReport("Building dataset table...")

populateDatasets()

# Serialize all 'dataset' objects into a TSV.

progressReport("Writing dataset table...")

writeTable('dataset')

# Process all native-HMP 'visit' data into data_event objects.

progressReport("Processing visit data...")

processVisits()

# Hook up bio_sample objects to data_events in which they are used as inputs or outputs.

progressReport("Linking bio_samples to data_events...")

linkBioSamples()

# Write the 'data_event' table.

progressReport("Writing data_event table...")

writeTable('data_event')

# Process all containment links cached during previous passes.

progressReport("Processing dataset containment relationships...")

processDatasetContainment()

# Serialize all info on dataset containment.

progressReport("Writing all dataset containment tables...")

writeObjectsInDatasets()

# Write stub tables (headers) for subject_group and subjects_in_subject_groups, which we're not currently using.

progressReport("Writing (stub) tables for subject_group tracking (currently unused)...")

writeStubTables()

# Manage 'sample_attr', 'subject_attr' and 'visit_attr' HMP data-decorator types.

progressReport("Processing attr data decorations...")

processAttrObjects()

# Save data_event input/output processing linkage data.

progressReport("Writing processed-by and produced-by tables...")

writeProducedBy()
writeProcessedBy()

# Link subjects to their NCBI taxonomy designation (in this case, all are humans).

progressReport("Writing subject-taxonomy table...")

writeSubjectTaxonomy()

# Decorate CV terms used with extra metadata (where available and relevant).

progressReport("Decorating CV terms used with OBO data...")

decorateTermsUsed()

# Collect all CV terms used and save them in categorized tables.

progressReport("Creating tables recording all CV terms used...")

serializeTermsUsed()

# BELOW IS FOR TESTING ONLY: DISABLE FOR HONEST DATA IMPORT
# Create fake dates for file objects & store them in auxiliary_data.
# Create three fake dates for each file: creation, repository insertion (e.g. SRA), and CFDE ingest.
# We'll be decorating file objects from the three iHMP subprojects: IBDMDB, T2D and momspi.
progressReport("Inserting fake creation/archive/repo dates for later query testing...")

createFakeDates()
# ABOVE IS FOR TESTING ONLY: DISABLE FOR HONEST DATA IMPORT

# Dump all metadata that doesn't fit into C2M2 into a generic extension table.

progressReport("Writing unlinked auxiliary-data table...")

writeAuxTable()

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

"""

# AUXILIARY DEVELOPMENT STUFF

# Make the flat-table output directory if it doesn't yet exist.

if not os.path.isdir(flatDir) and os.path.exists(flatDir):
   
   die('%s exists but is not a directory; aborting.' % flatDir)

elif not os.path.isdir(flatDir):
   
   os.mkdir(flatDir)

# Write what we've wrought.

# Spit out all flattened data structures by HMP DB 'node_type'.

for objectType in sorted( allowableNodeTypes ):
   
   if objectType in nativeTypeToNodeID:
      
      tableFile = '%s/%s.tsv' % ( flatDir, objectType )

      if objectType not in nativeTypeToNativeColNames:
         
         die("Can't load column-name set for object type '%s'; aborting." % objectType)

      with open(tableFile, 'w') as OUT:
         
         OUT.write( '\t'.join( ['_id'] + sorted(nativeTypeToNativeColNames[objectType]) ) + '\n' )

         for objectID in sorted( nativeTypeToNodeID[objectType] ):
            
            OUT.write('%s' % objectID)

            for propertyName in sorted(nativeTypeToNativeColNames[objectType]):
               
               if propertyName in flatObjects[objectID]:
                  
                  OUT.write('\t%s' % flatObjects[objectID][propertyName])

               else:
                  
                  OUT.write('\t')

               # end if ( $propertyName exists in the data structure for object $objectID )

            # end for ( propertyName in sorted(nativeTypeToNativeColNames[objectType]) )
         
            OUT.write( '\n' )

         # end for ( objectID in sorted( nativeTypeToNodeID[objectType] )

      # end with ( tableName opened as 'OUT' )

   # end if ( we have any records for the current objectType )

# end for ( tableName in sorted( allowableNodeTypes ) )

"""


