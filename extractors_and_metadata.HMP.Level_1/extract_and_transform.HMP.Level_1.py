#!/usr/bin/env python3

##########################################################################################
#                                     AUTHOR INFO
##########################################################################################

# Arthur Brady (Univ. of MD Inst. for Genome Sciences) wrote this script to extract
# HMP experimental data and transform it to conform to the draft C2M2 Level 1 data
# specification prior to ingestion into a central CFDE database.

# Creation date: 2020-05-14
# Lastmod date unless I forgot to change it: 2020-05-17

# contact email: abrady@som.umaryland.edu

import os
import json
import re
import sys

##########################################################################################
##########################################################################################
##########################################################################################
#                   SUBROUTINES (in call order, including recursion)
##########################################################################################
##########################################################################################
##########################################################################################

####### progressReport ###################################################################
# 
# CALLED BY: main execution thread
# 
# Print a logging message to STDERR.
# 
#-----------------------------------------------------------------------------------------

def progressReport( message ):
   
   print('%s' % message, file=sys.stderr)

#-----------------------------------------------------------------------------------------
# end sub: progressReport( message )
##########################################################################################

####### loadBannedIDs ####################################################################
# 
# CALLED BY: main execution thread
# 
# Cache banned OSDF node IDs prior to processing HMP OSDF DB dump: ignore devel-test data
# and other similar noise.
# 
#-----------------------------------------------------------------------------------------

def loadBannedIDs(  ):
   
   global banList, bannedIDs

   with open(banList, 'r') as IN:
      
      for line in IN:
         
         bannedIDs.add(line.rstrip('\r\n'))

      # end for ( line iterator on banned-node file )

   # end with ( banned-node file opened as 'IN' )

#-----------------------------------------------------------------------------------------
# end sub: loadBannedIDs(  )
##########################################################################################

####### loadEnumMaps #####################################################################
# 
# CALLED BY: main execution thread
# 
# Load functions mapping enumerated sets of DCC-internal metadata field values
# to term IDs in controlled vocabularies. Filenames of the map-reference files being
# loaded, here, indicate:
# 
#   * which controlled vocabulary is being referenced (e.g. "OBI" or "Uberon"), and
#   * which C2M2 entity's field will contain the mapped CV terms in this C2M2 instance
#     (e.g. "biosample.anatomy" or "file.data_type")
# 
# (Example filename: "native_keyword_to_Uberon_for_biosample.anatomy.tsv", where 'native
# keyword' refers to local DCC-generated anatomical terms, documented in this mapping
# file, to be translated (mapped) to a corresponding CV term (listed in the file) for
# C2M2 serialization.)
# 
#-----------------------------------------------------------------------------------------

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

#-----------------------------------------------------------------------------------------
# end sub loadEnumMaps(  )
##########################################################################################

####### die ##############################################################################
# 
# CALLED BY: scanList, scanDict, [everything....]
# Halt program and report why.
# 
#-----------------------------------------------------------------------------------------

def die( errorMessage ):
   
   print('\n   FATAL: %s\n' % errorMessage, file=sys.stderr)

   sys.exit(-1)

#-----------------------------------------------------------------------------------------
# end sub: die( errorMessage )
##########################################################################################

####### scanList #########################################################################
# 
# CALLED BY: scanList(), scanDict(), flattenData()
# 
# Scan a list sub-object during processing of the raw-metadata Python object (HMPdata)
# that's been loaded directly from a native HMP JSON dump (flatten the auto-inflated
# data structures prior to rearrangement for C2M2 serialization).
# 
#-----------------------------------------------------------------------------------------

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

#-----------------------------------------------------------------------------------------
# end sub: scanList( objectID, objectType, listName, listObject )
##########################################################################################

####### scanDict #########################################################################
# 
# CALLED BY: scanList(), scanDict(), flattenData()
# 
# Scan a dict sub-object during processing of the raw-metadata Python object (HMPdata)
# that's been loaded directly from a native HMP JSON dump (flatten the auto-inflated
# data structures prior to rearrangement for C2M2 serialization).
# 
#-----------------------------------------------------------------------------------------

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

#-----------------------------------------------------------------------------------------
# end sub: scanDict( objectID, objectType, dictName, dictObject )
##########################################################################################

####### flattenData ######################################################################
# 
# CALLED BY: main execution thread
# 
# Flatten containment keywords for the (arbitrarily deeply nested) raw-metadata Python
# object (HMPdata) which has been loaded directly from a native HMP JSON dump.
# Facilitates needed restructuring of data prior to building a compliant C2M2
# serialization instance.
# 
#-----------------------------------------------------------------------------------------

def flattenData(  ):
   
   global HMPdata, allowableNodeTypes, bannedIDs, nativeTypeToNodeID, nodeIDToNativeType, nativeTypeToNativeColNames, flatObjects, scalarTypes

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

#-----------------------------------------------------------------------------------------
# end sub flattenData(  )
##########################################################################################

####### populateFiles ####################################################################
# 
# CALLED BY: main execution thread
# 
# Populate data structures representing C2M2 'file' entities with relevant data.
# 
#-----------------------------------------------------------------------------------------

def populateFiles(  ):
   
   global objectsToWrite, flatObjects, nativeTypeToNodeID, enumMap, FileNodeTypes, entityAssociations, parents, idNamespace, allowableNodeTypes

   if 'file' not in objectsToWrite:
      
      objectsToWrite['file'] = {}

   # end if ( we haven't yet created a 'file' substructure in objectsToWrite )

   for nodeType in FileNodeTypes:
      
      if nodeType not in nativeTypeToNodeID:
         
         die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

      # end if ( nodeType not in nativeTypeToNodeID )

      for currentID in nativeTypeToNodeID[nodeType]:
         
         objectsToWrite['file'][currentID] = {}

         #----------------------------------------------------------------------
         # These values are constant across all HMP metadata.

         objectsToWrite['file'][currentID]['id_namespace'] = idNamespace

         objectsToWrite['file'][currentID]['project_id_namespace'] = idNamespace

         #----------------------------------------------------------------------
         # We'll fill this in later after some needed transitive-association
         # tracing.

         objectsToWrite['file'][currentID]['project'] = ''

         #----------------------------------------------------------------------
         # We're not doing these fields yet.

         objectsToWrite['file'][currentID]['persistent_id'] = ''
         objectsToWrite['file'][currentID]['creation_time'] = ''

         #----------------------------------------------------------------------
         # Compute this file's filename from stored URL data.

         currentURL = ''

         if nodeType in { 'proteome', 'proteome_nonpride' }:
            
            currentURL = flatObjects[currentID]['raw_url__000000000']

         else:
            
            # { 'metabolome', 'serology', 'cytokine', 'lipidome',
            # 'clustered_seq_set', 'annotation', 'host_variant_call',
            # 'alignment', 'viral_seq_set', '*_seq_set',
            # 'abundance_matrix' }:
            
            currentURL = flatObjects[currentID]['urls__000000000']

         # end if ( nodeType switch for URL extraction )

         if re.search(r'<private>', currentURL) is not None:
            
            currentURL = ''

         baseName = re.sub(r'^.*\/', r'', currentURL)

         objectsToWrite['file'][currentID]['filename'] = baseName

         #----------------------------------------------------------------------
         # Locate and store any available SHA256 or MD5 checksum strings.

         # SHA256:

         if ( nodeType in { 'metabolome', 'cytokine' } ) or ( ( nodeType not in { 'clustered_seq_set', 'host_epigenetics_raw_seq_set', 'viral_seq_set', 'wgs_assembled_seq_set' } ) and ( re.search(r'_seq_set$', nodeType) is not None ) ):
            
            if 'checksums__sha256' in flatObjects[currentID]:
               
               objectsToWrite['file'][currentID]['sha256'] = flatObjects[currentID]['checksums__sha256']

            else:
               
               objectsToWrite['file'][currentID]['sha256'] = ''

         else:
            
            # SHA256 isn't stored anywhere for the nodeTypes not indicated above.

            objectsToWrite['file'][currentID]['sha256'] = ''

         # end if ( nodeType switch for SHA256 loading )

         # MD5:

         if ( nodeType in { 'proteome', 'metabolome', 'serology', 'cytokine', 'lipidome', 'clustered_seq_set', 'annotation', 'host_variant_call', 'alignment', 'viral_seq_set', 'abundance_matrix' } ) or ( re.search(r'_seq_set$', nodeType) is not None ):
            
            objectsToWrite['file'][currentID]['md5'] = flatObjects[currentID]['checksums__md5']

         else:
            
            # { 'proteome_nonpride' }

            # MD5 isn't stored anywhere.

            objectsToWrite['file'][currentID]['md5'] = ''

         # end if ( nodeType switch for MD5 loading )

         #----------------------------------------------------------------------
         # Locate and store any available 'size in bytes' data.

         if ( nodeType in { 'clustered_seq_set', 'annotation', 'host_variant_call', 'alignment', 'abundance_matrix' } ) or ( ( nodeType not in { 'viral_seq_set' } ) and ( re.search(r'_seq_set$', nodeType) is not None ) ):
            
            objectsToWrite['file'][currentID]['size_in_bytes'] = flatObjects[currentID]['size']

         else:
            
            # { 'proteome', 'metabolome', 'serology', 'cytokine', 'lipidome',
            # 'proteome_nonpride', 'viral_seq_set' }

            objectsToWrite['file'][currentID]['size_in_bytes'] = ''

         # end if ( nodeType switch for byte-size loading )

         #----------------------------------------------------------------------
         # Identify and store relevant EDAM CV terms for file_format and
         # data_type.

         fileFormatTerm = ''
         dataTypeTerm = ''

         ######################################################################
         ######################## nodeType: proteome ##########################
         ######################################################################

         if nodeType == 'proteome':
            
            # file.file_format

            if re.search(r'\.raw\.gz$', baseName) is not None:
               
               fileFormatTerm = enumMap['file.file_format']['thermoRaw']

            elif re.search(r'\.mzML\.gz', baseName) is not None:
               
               fileFormatTerm = enumMap['file.file_format']['mzML']

            else:
               
               die("Can't identify format for proteome file basename '%s'; aborting." % baseName)

            # end if ( we have an identifiable filename extension )

            # file.data_type

            dataTypeTerm = enumMap['file.data_type']['proteomicsData']
         
         ######################################################################
         ######################## nodeType: metabolome ########################
         ######################################################################

         elif nodeType == 'metabolome':
            
            # file.file_format

            if flatObjects[currentID]['format'] == 'raw':
               
               fileFormatTerm = enumMap['file.file_format']['thermoRaw']

            elif flatObjects[currentID]['format'] == 'mzXML':
               
               fileFormatTerm = enumMap['file.file_format']['mzXML']

            else:
               
               die("Can't identify format for metabolome file basename '%s'; aborting." % baseName)

            # end if ( we have an identifiable filename extension )

            # file.data_type

            dataTypeTerm = enumMap['file.data_type']['metabolomicsData']
         
         ######################################################################
         ######################## nodeType: serology ##########################
         ######################################################################

         elif nodeType == 'serology':
            
            # file.file_format

            if flatObjects[currentID]['format'] == 'tsv':
               
               fileFormatTerm = enumMap['file.file_format']['tsv']

            else:
               
               die("Can't identify format for serology file basename '%s'; aborting." % baseName)

            # end if ( we have an identifiable filename extension )

            # file.data_type

            dataTypeTerm = enumMap['file.data_type']['serologyData']
         
         ######################################################################
         ######################## nodeType: cytokine ##########################
         ######################################################################

         elif nodeType == 'cytokine':
            
            # file.file_format

            if flatObjects[currentID]['format'] == '':
               
               fileFormatTerm = ''
               
            elif flatObjects[currentID]['format'] == 'tsv':
               
               fileFormatTerm = enumMap['file.file_format']['tsv']

            else:
               
               die("Can't identify format for cytokine file basename '%s'; aborting." % baseName)

            # end if ( we have an identifiable filename extension )

            # file.data_type

            dataTypeTerm = enumMap['file.data_type']['cytokineData']
         
         ######################################################################
         ######################## nodeType: lipidome ##########################
         ######################################################################

         elif nodeType == 'lipidome':
            
            # (file.file_format is always blank)

            fileFormatTerm = ''

            # file.data_type

            dataTypeTerm = enumMap['file.data_type']['lipidomeData']
         
         ######################################################################
         ######################## nodeType: proteome_nonpride #################
         ######################################################################

         elif nodeType == 'proteome_nonpride':
            
            # file.file_format

            if re.search(r'\.mzML\.gz', baseName) is not None:
               
               fileFormatTerm = enumMap['file.file_format']['mzML']

            else:
               
               die("Can't identify format for proteome_nonpride file basename '%s'; aborting." % baseName)

            # end if ( we have an identifiable filename extension )

            # file.data_type

            dataTypeTerm = enumMap['file.data_type']['proteomicsData']
         
         ######################################################################
         ######################## nodeType: clustered_seq_set #################
         ######################################################################

         elif nodeType == 'clustered_seq_set':
            
            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:

               fileFormatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

            # file.data_type

            if flatObjects[currentID]['sequence_type'] == 'nucleotide':
               
               dataTypeTerm = enumMap['file.data_type']['nucleotideSequence']
         
            elif flatObjects[currentID]['sequence_type'] == 'peptide':
               
               dataTypeTerm = enumMap['file.data_type']['proteinSequence']

            else:
               
               die("Can't identify CV mapping for sequence_type '%s' of clustered_seq_set file basename '%s'; aborting." % ( flatObjects[currentID]['sequence_type'], baseName ) )

            # end if ( we have a usable 'sequence_type' value )
         
         ######################################################################
         ######################## nodeType: annotation ########################
         ######################################################################

         elif nodeType == 'annotation':
            
            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               fileFormatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

            # file.data_type

            dataTypeTerm = enumMap['file.data_type']['sequenceFeatures']

         ######################################################################
         ######################## nodeType: host_variant_call #################
         ######################################################################

         elif nodeType == 'host_variant_call':
            
            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               fileFormatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

            # file.data_type

            dataTypeTerm = enumMap['file.data_type']['sequenceVariations']

         ######################################################################
         ######################## nodeType: alignment ########################
         ######################################################################

         elif nodeType == 'alignment':
            
            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               fileFormatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

            # file.data_type

            dataTypeTerm = enumMap['file.data_type']['alignment']

         ######################################################################
         ######################## nodeType: viral_seq_set #####################
         ######################################################################

         elif nodeType == 'viral_seq_set':
            
            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               fileFormatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

            # file.data_type (current data only has BAM files in this node type)

            dataTypeTerm = enumMap['file.data_type']['alignment']

         ######################################################################
         ######################## nodeType: *_seq_set (not caught above) ######
         ######################################################################

         elif re.search(r'_seq_set$', nodeType) is not None:
            
            # file.file_format

            if flatObjects[currentID]['format'] == '':
               
               fileFormatTerm = ''
               
            elif flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               fileFormatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]

            else:
               
               die("Can't identify format for clustered_seq_set file basename '%s'; aborting." % baseName)

            # end if ( we have a usable 'format' value )

            # file.data_type (these are all sequence files)

            dataTypeTerm = enumMap['file.data_type']['sequence']

         ######################################################################
         ######################## nodeType: abundance_matrix ##################
         ######################################################################

         elif nodeType == 'abundance_matrix':
            
            # file.file_format

            if flatObjects[currentID]['format'] in enumMap['file.file_format']:
               
               fileFormatTerm = enumMap['file.file_format'][flatObjects[currentID]['format']]

            elif ( not flatObjects[currentID]['format'] is None ) and ( not flatObjects[currentID]['format'] == '' ):
               
               die("Did not load CV term information for (%s) %s.format '%s'; aborting." % ( nodeType, currentID, flatObjects[currentID]['format'] ))

            else:
               
               fileFormatTerm = ''

            # end if ( switch on sanity of currentID.'format' )

            # file.data_type

            if flatObjects[currentID]['matrix_type'] in enumMap['file.data_type']:
               
               dataTypeTerm = enumMap['file.data_type'][flatObjects[currentID]['matrix_type']]

            elif ( not flatObjects[currentID]['matrix_type'] is None ) and ( not flatObjects[currentID]['matrix_type'] == '' ):
               
               die("Did not load CV term information for (%s) %s.matrix_type '%s'; aborting." % ( nodeType, currentID, flatObjects[currentID]['matrix_type'] ))

            else:
               
               dataTypeTerm = ''

            # end if ( switch on sanity of currentID.'matrix_type' )

         #######################################################################
         #######################################################################
         #  end if ( nodeType switch to establish file_format and data_type )  #
         #######################################################################
         #######################################################################

         objectsToWrite['file'][currentID]['file_format'] = fileFormatTerm

         objectsToWrite['file'][currentID]['data_type'] = dataTypeTerm

         #######################################################################
         # Process OSDF linkage data to connect file entities to associated
         # biosample, subject, file and project entities, caching links in data
         # structures corresponding to C2M2 inter-entity association tables.
         # 
         # Project associations are transitively implied in the native HMP
         # linkage structure, so we will have to keep track of a global
         # association-hierarchy DAG (called "parents") for all loaded
         # entity types (wherein entities are possibly connected to their
         # sponsoring projects only indirectly, through association DAG paths
         # transiting through intermediate objects) to properly deduce project
         # associations for all entities.

         for fieldName in sorted(flatObjects[currentID].keys()):
            
            if ( re.search(r'^linkage', fieldName) is not None ):
               
               linkedID = flatObjects[currentID][fieldName]

               # Cache a containment DAG named "parents" for later project-containment reconstruction.

               if currentID not in parents:
                  
                  parents[currentID] = set()

               # end if ( setup check for parents[currentID] )

               parents[currentID] |= { linkedID }

               # Which C2M2 entity type is linked, here, to our file?

               linkedNodeType = allowableNodeTypes[nodeIDToNativeType[linkedID]]

               if linkedNodeType == 'biosample':
                  
                  if currentID not in entityAssociations['file_describes_biosample']:
                     
                     entityAssociations['file_describes_biosample'][currentID] = { linkedID }

                  else:
                     
                     entityAssociations['file_describes_biosample'][currentID] |= { linkedID }

               elif linkedNodeType == 'subject':
                  
                  if currentID not in entityAssociations['file_describes_subject']:
                     
                     entityAssociations['file_describes_subject'][currentID] = { linkedID }

                  else:
                     
                     entityAssociations['file_describes_subject'][currentID] |= { linkedID }

               # elif linkedNodeType == 'project':
                  
                  # This happens, but it's rare, and we're going to fully populate the 
                  # (required) 'project' field in our 'file' entities in a more
                  # systematic way, downstream.

               # end switch looking for `subject` or `biosample` entities to
               # link this `file` to via explicit C2M2 association

            # end if ( we're looking at a fieldName we should be processing for linkage relationships )

         # end for ( each fieldName in flatObjects for currentID )

      # end for ( currentID in nativeTypeToNodeID[nodeType] )

   # end for ( nodeType in FileNodeTypes )

#-----------------------------------------------------------------------------------------
# end sub populateFiles( nodeType )
##########################################################################################

####### populateBiosamples ###############################################################
# 
# CALLED BY: main execution thread
# 
# Populate new C2M2 'biosample' objects with relevant data.
# 
#-----------------------------------------------------------------------------------------

def populateBiosamples(  ):
   
   global objectsToWrite, flatObjects, nativeTypeToNodeID, enumMap, BiosampleNodeTypes, parents, idNamespace

   if 'biosample' not in objectsToWrite:
      
      objectsToWrite['biosample'] = {}

   # end if ( we haven't yet created a 'biosample' substructure in objectsToWrite )

   for nodeType in BiosampleNodeTypes:
      
      if nodeType not in nativeTypeToNodeID:
         
         die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

      # end if ( nodeType not in nativeTypeToNodeID )

      for currentID in nativeTypeToNodeID[nodeType]:
         
         objectsToWrite['biosample'][currentID] = {}

         #----------------------------------------------------------------------
         # These values are constant across all HMP metadata.

         objectsToWrite['biosample'][currentID]['id_namespace'] = idNamespace

         objectsToWrite['biosample'][currentID]['project_id_namespace'] = idNamespace

         #----------------------------------------------------------------------
         # We'll fill this in later after some needed transitive-association
         # tracing.

         objectsToWrite['biosample'][currentID]['project'] = ''

         #----------------------------------------------------------------------
         # We're not doing these fields yet.

         objectsToWrite['biosample'][currentID]['persistent_id'] = ''
         objectsToWrite['biosample'][currentID]['creation_time'] = ''

         ######################################################################
         ######################## nodeType: sample ############################
         ######################################################################

         if nodeType == 'sample':
            
            if flatObjects[currentID]['fma_body_site'] == '':
               
               objectsToWrite['biosample'][currentID]['anatomy'] = ''

            else:
               
               fmaCode = re.sub( r'^.*(FMA:\d+).*$', r'\1', flatObjects[currentID]['fma_body_site'] )

               if fmaCode not in enumMap['biosample.anatomy']:
                  
                  objectsToWrite['biosample'][currentID]['anatomy'] = ''
                  # Nope.
                  # die("Unrecognized FMA code in sample object '%s': '%s'; aborting." % ( currentID, fmaCode ))

               else:
                  
                  uberonTerm = enumMap['biosample.anatomy'][fmaCode]

                  objectsToWrite['biosample'][currentID]['anatomy'] = uberonTerm

               # end if ( we recognize the current FMA anatomy code )

            # end if ( we have a non-null value in 'fma_body_site' )

            #################################################################
            # Metadata mapped from a fixed set of values to third-party CVs:

            # biosample.assay_type

            assayTypeTerm = enumMap['biosample.assay_type']['material']

            objectsToWrite['biosample'][currentID]['assay_type'] = assayTypeTerm

         ######################################################################
         ######################## nodeType: *_prep ############################
         ######################################################################

         elif ( nodeType == '16s_dna_prep' or nodeType == 'wgs_dna_prep' or nodeType == 'host_seq_prep' or nodeType == 'microb_assay_prep' or nodeType == 'host_assay_prep' ):
            
            objectsToWrite['biosample'][currentID]['anatomy'] = ''

            assayTypeTerm = enumMap['biosample.assay_type']['library']

            objectsToWrite['biosample'][currentID]['assay_type'] = assayTypeTerm

         # end if ( nodeType switch )

         ############################################################################
         # Process OSDF linkage data to connect biosample entities to associated
         # biosample, subject, file and project entities, caching links in data
         # structures corresponding to C2M2 inter-entity association tables.
         # 
         # Project associations are transitively implied in the native HMP
         # linkage structure, so we will have to keep track of a global
         # association-hierarchy DAG (called "parents") for all loaded
         # entity types (wherein entities are possibly connected to their
         # sponsoring projects only indirectly, through association DAG paths
         # transiting through intermediate objects) to properly deduce project
         # associations for all entities.

         for fieldName in sorted(flatObjects[currentID].keys()):
            
            if re.search(r'^linkage', fieldName) is not None:
               
               # There's only ever one linkage field for any of the HMP nodeTypes
               # that map to C2M2.biosample: to a visit ID for 'sample' nodeType, and
               # to the ID of a generating 'sample' object for the '*_prep' nodeTypes.

               linkedID = flatObjects[currentID][fieldName]

               if currentID not in parents:
                  
                  parents[currentID] = set()

               # end if ( setup check for parents[currentID] )

               parents[currentID] |= { linkedID }

               if nodeType == 'sample':
                  
                  # We'll need this later to hook up biosamples and subjects.

                  sampleToVisit[currentID] = linkedID

               else:
                  
                  prepToBiosample[currentID] = linkedID

               # end if ( nodeType switch to determine how link caching will take place )

            # end if ( we're looking at a linkage fieldName )

         # end for ( each fieldName in flatObjects for currentID )

      # end for ( currentID in nativeTypeToNodeID[nodeType] )

   # end for ( nodeType in BiosampleNodeTypes )

#-----------------------------------------------------------------------------------------
# end sub populateBiosamples(  )
##########################################################################################

####### populateSubjects #################################################################
# 
# CALLED BY: main execution thread
# 
# Populate new C2M2 'subject' objects with relevant data.
# 
#-----------------------------------------------------------------------------------------

def populateSubjects(  ):
   
   global objectsToWrite, flatObjects, nativeTypeToNodeID, enumMap, SubjectNodeTypes, parents, singleOrgGranularityID

   if 'subject' not in objectsToWrite:
      
      objectsToWrite['subject'] = {}

   # end if ( we haven't yet created a 'subject' substructure in objectsToWrite )

   for nodeType in SubjectNodeTypes:
      
      if nodeType not in nativeTypeToNodeID:
         
         die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

      # end if ( nodeType not in nativeTypeToNodeID )

      for currentID in nativeTypeToNodeID[nodeType]:
         
         objectsToWrite['subject'][currentID] = {}

         #----------------------------------------------------------------------
         # These values are constant across all HMP metadata.

         objectsToWrite['subject'][currentID]['id_namespace'] = idNamespace
         
         objectsToWrite['subject'][currentID]['project_id_namespace'] = idNamespace

         #----------------------------------------------------------------------
         # We'll fill this in later after some needed transitive-association
         # tracing.

         objectsToWrite['subject'][currentID]['project'] = ''

         #----------------------------------------------------------------------
         # We're not doing these fields yet.

         objectsToWrite['subject'][currentID]['persistent_id'] = ''
         objectsToWrite['subject'][currentID]['creation_time'] = ''

         #----------------------------------------------------------------------
         # Constant. Only human subjects.

         objectsToWrite['subject'][currentID]['granularity'] = singleOrgGranularityID

         ############################################################################
         # 

         for fieldName in sorted(flatObjects[currentID].keys()):
            
            if re.search(r'^linkage', fieldName) is not None:
               
               # subject participates_in study

               linkedID = flatObjects[currentID][fieldName]

               if currentID not in parents:
                  
                  parents[currentID] = set()

               # end if ( setup check for parents[currentID] )

               parents[currentID] |= { linkedID }

            # end if ( we're looking at a linkage fieldName )

         # end for ( each fieldName in flatObjects for currentID )

      # end for ( currentID in nativeTypeToNodeID[nodeType] )

   # end for ( nodeType in SubjectNodeTypes )

#-----------------------------------------------------------------------------------------
# end sub populateSubjects(  )
##########################################################################################

####### populateProjects #################################################################
# 
# CALLED BY: main execution thread
# 
# Populate new C2M2 'project' objects with relevant data.
# 
#-----------------------------------------------------------------------------------------

def populateProjects(  ):
   
   global objectsToWrite, flatObjects, nativeTypeToNodeID, enumMap, ProjectNodeTypes, parents, idNamespace

   if 'project' not in objectsToWrite:
      
      objectsToWrite['project'] = {}

   # end if ( we haven't yet created a 'project' substructure in objectsToWrite )

   for nodeType in ProjectNodeTypes:
      
      if nodeType not in nativeTypeToNodeID:
         
         die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

      # end if ( nodeType not in nativeTypeToNodeID )

      for currentID in nativeTypeToNodeID[nodeType]:
         
         objectsToWrite['project'][currentID] = {}

         #----------------------------------------------------------------------
         # This value is a constant across all HMP metadata.

         objectsToWrite['project'][currentID]['id_namespace'] = idNamespace

         #----------------------------------------------------------------------
         # We're not doing these fields yet.

         objectsToWrite['project'][currentID]['persistent_id'] = ''

         objectsToWrite['project'][currentID]['abbreviation'] = ''

         #----------------------------------------------------------------------
         # We can load these straight from the native fields.

         objectsToWrite['project'][currentID]['name'] = flatObjects[currentID]['name']

         objectsToWrite['project'][currentID]['description'] = flatObjects[currentID]['description']

         ############################################################################
         # Process OSDF linkage data to connect project entities to associated
         # biosample, subject, file and project entities, caching links in data
         # structures corresponding to C2M2 inter-entity association tables.
         # 
         # Project associations are transitively implied in the native HMP
         # linkage structure, so we will have to keep track of a global
         # association-hierarchy DAG (called "parents") for all loaded
         # entity types (wherein entities are possibly connected to their
         # sponsoring projects only indirectly, through association DAG paths
         # transiting through intermediate objects) to properly deduce project
         # associations for all entities.

         for fieldName in sorted(flatObjects[currentID].keys()):
            
            if re.search(r'^linkage', fieldName) is not None:
               
               # The only linkages hanging off the 'study' nodeType are pointers to
               # supersets (other studies or projects). 'project' nodeTypes have
               # no linkages: they are top-level objects, and all links flow upward.

               linkedID = flatObjects[currentID][fieldName]

               if currentID not in parents:
                  
                  parents[currentID] = set()

               # end if ( setup check for parents[currentID] )

               parents[currentID] |= { linkedID }

            # end if ( we're looking at a linkage fieldName )

         # end for ( each fieldName in flatObjects for currentID )

      # end for ( currentID in nativeTypeToNodeID[nodeType] )

   # end for ( nodeType in ProjectNodeTypes )

#-----------------------------------------------------------------------------------------
# end sub populateProjects(  )
##########################################################################################

####### processVisits ####################################################################
# 
# CALLED BY: main execution thread
# 
# Process all native-HMP 'visit' data to close transitive project network around
# subjects, files and biosamples.
# 
#-----------------------------------------------------------------------------------------

def processVisits(  ):
   
   global flatObjects, objectsToWrite, nativeTypeToNodeID, parents, visitToSubject

   nodeType = 'visit'

   if nodeType not in nativeTypeToNodeID:
      
      die("Can't find node type '%s' in nativeTypeToNodeID map: aborting." % nodeType)

   # end if ( nodeType not in nativeTypeToNodeID )

   for flatID in nativeTypeToNodeID[nodeType]:
      
      visitToSubject[flatID] = flatObjects[flatID]['linkage__by__000000000']

      if flatID not in parents:
         
         parents[flatID] = set()

      # end if ( setup check for parents[flatID] )

      parents[flatID] |= { flatObjects[flatID]['linkage__by__000000000'] }

   # end for ( flatID in nativeTypeToNodeID[nodeType] )

#-----------------------------------------------------------------------------------------
# end sub processVisits(  )
##########################################################################################

####### linkBiosamples ###################################################################
# 
# CALLED BY: main execution thread
# 
# Hook up bio_sample entities to subjects they describe via
# 
#    entityAssociations['biosample_from_subject'].
# 
# Also add any relevant direct links to file_describes_subject using
# file_describes_biosample and biosample_from_subject.
# 
#-----------------------------------------------------------------------------------------

def linkBiosamples(  ):
   
   global prepToBiosample, sampleToVisit, visitToSubject, entityAssociations

   for sampleID in sampleToVisit:
      
         visitID = sampleToVisit[sampleID]

         subjectID = visitToSubject[visitID]

         # Assuming a unique subject per biosample here (and for HMP, that's fine):

         entityAssociations['biosample_from_subject'][sampleID] = { subjectID }

   # end for ( each sampleID in sampleToVisit )

   for prepID in prepToBiosample:
      
      sampleID = prepToBiosample[prepID]

      visitID = sampleToVisit[sampleID]

      subjectID = visitToSubject[visitID]

      entityAssociations['biosample_from_subject'][prepID] = { subjectID }

   # end for ( each prepID in prepToBiosample )

   for fileID in entityAssociations['file_describes_biosample']:
      
      for sampleID in entityAssociations['file_describes_biosample'][fileID]:
         
         if sampleID in entityAssociations['biosample_from_subject']:
            
            if fileID not in entityAssociations['file_describes_subject']:
               
               entityAssociations['file_describes_subject'][fileID] = set()

            for subjectID in entityAssociations['biosample_from_subject'][sampleID]:
               
               entityAssociations['file_describes_subject'][fileID] |= { subjectID }

#-----------------------------------------------------------------------------------------
# end sub linkBiosamples(  )
##########################################################################################

####### computeProjectDepth ##############################################################
# 
# CALLED BY: processProjectContainment()
# 
# Identify the number of hops from each node in the project DAG to the closest root.
# Used to identify the most specific project division available for "primary project"
# foreign-key references.
# 
#-----------------------------------------------------------------------------------------

def computeProjectDepth(  ):
   
   global parents, projectDepth, allowableNodeTypes, objectsToWrite

   # Identify the root nodes.

   projectCount = len(objectsToWrite['project'])

   labelCount = 0

   for projectID in objectsToWrite['project']:
      
      if projectID not in parents:
         
         projectDepth[projectID] = 0

         labelCount += 1

      # end if ( this is a root node in the project DAG )

   # end for ( projectID in objectsToWrite['project'] )

   while labelCount < projectCount:
      
      labelCount = 0

      for projectID in objectsToWrite['project']:
         
         if projectID in projectDepth:
            
            # This project already has a depth label.

            labelCount += 1

         else:
            
            # This project doesn't yet have a depth label. Look for
            # a labeled parent.
            
            minSeen = -1

            for parentID in parents[projectID]:
               
               if parentID in projectDepth:
                  
                  if minSeen == -1 or projectDepth[parentID] < minSeen:
                     
                     minSeen = projectDepth[parentID]

            if minSeen != -1:
               
               # We found a parent with an assigned depth. Increment and propagate.

               projectDepth[projectID] = minSeen + 1

               labelCount += 1

      # end for ( each projectID )

   # end while ( not all projectIDs have depth labels )

#-----------------------------------------------------------------------------------------
# end sub computeProjectDepth(  )
##########################################################################################

####### findContainingSets ###############################################################
# 
# CALLED BY: processProjectContainment()
# 
# Recursively scan containment DAG to establish
# top-level project containment for a given object.
# 
#-----------------------------------------------------------------------------------------

def findContainingSets( objectID, containingSets ):
   
   global parents, nodeIDToNativeType, ProjectNodeTypes, containedIn, allowableNodeTypes

   if objectID in containedIn:
      
      containingSets |= containedIn[objectID]

   else:
      
      if allowableNodeTypes[nodeIDToNativeType[objectID]] == 'project':
         
         containingSets |= { objectID }

      # end if ( this is a project type )
      
      if objectID in parents:
         
         for parent in parents[objectID]:
            
            containingSets |= findContainingSets(parent, containingSets)

         # end for ( each parent of objectID )

      # end if ( objectID has any parents )

   # end if ( containedIn already has a record for objectID )

   return containingSets

#-----------------------------------------------------------------------------------------
# end sub findContainingSets( objectID, containingSets )
##########################################################################################

####### findDeepestParentProjectID #######################################################
# 
# CALLED BY: processProjectContainment()
# 
# Given a set of (transitively) containing projects for a particular
# object, pick one containing project ID that's closest to the flow-sink end of
# the HMP project DAG.
# 
#-----------------------------------------------------------------------------------------

def findDeepestParentProjectID( containingSets ):
   
   global projectDepth

   minSeen = -1

   returnProjectID = ''

   for projectID in containingSets:
      
      if minSeen < projectDepth[projectID]:
         
         minSeen = projectDepth[projectID]
         returnProjectID = projectID

   return returnProjectID

#-----------------------------------------------------------------------------------------
# end sub findDeepestParentProjectID( containingSets )
##########################################################################################

####### processProjectContainment ########################################################
# 
# CALLED BY: main execution thread
# 
# Process all parent[] links to make explicit (i.e., flatten) the (originally implicit,
# transitive) project containment DAG describing the HMP metadata space.
# 
#-----------------------------------------------------------------------------------------

def processProjectContainment(  ):
   
   global parents, projectDepth, containedIn, objectsToWrite, allowableNodeTypes, entityAssociations

   # Identify the number of hops from each node in the project DAG to the closest root.
   # Used to identify the most specific project division available for "primary project"
   # foreign-key references.

   computeProjectDepth()

   for objectID in parents.keys():
      
      containingSets = set()

      containingSets |= findContainingSets(objectID, containingSets)

      # This structure will end up storing objectID. Remove it.

      containingSets -= { objectID }

      # Cache results to speed later lookups.

      containedIn[objectID] = containingSets.copy()

      targetType = allowableNodeTypes[nodeIDToNativeType[objectID]]

      if targetType == 'project':
         
         # Cache the relationship to the parent project (if there is one) in a
         # project_in_project record.

         if objectID in parents:
            
            entityAssociations['project_in_project'][objectID] = findDeepestParentProjectID(containingSets)

      elif targetType in { 'file', 'biosample', 'subject' }:
         
         # Save the most specific parent project ID as an entity field.

         objectsToWrite[targetType][objectID]['project'] = findDeepestParentProjectID(containingSets)

      # end if ( valid targetType switch )

   # end for ( each ingested object )

#-----------------------------------------------------------------------------------------
# end sub processProjectContainment(  )
##########################################################################################

####### writeTable #######################################################################
# 
# CALLED BY: main execution thread
# 
# Write the specified entity table as a TSV.
# 
#-----------------------------------------------------------------------------------------

def writeTable( objectName ):
   
   global draftDir, objectsToWrite, outputColumns

   if objectName not in objectsToWrite:
      
      die("No data loaded for output table '%s'; aborting." % objectName)

   # end if ( we have no data loaded into $objectsToWrite for output type $objectName )

   if objectName not in outputColumns:
      
      die("Can't find an 'outputColumns' list for C2M2 output type '%s'; aborting." % objectName)

   # end if ( objectName doesn't have a corresponding list of output column names )

   outFile = '%s/%s.tsv' % ( draftDir, objectName )

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

#-----------------------------------------------------------------------------------------
# end sub writeTable( objectName )
##########################################################################################

####### writeProjectInProject #######################################################################
# 
# CALLED BY: main execution thread
# 
# Write the HMP project containment DAG.
# 
#-----------------------------------------------------------------------------------------

def writeProjectInProject(  ):
   
   global draftDir, entityAssociations, outputColumns, idNamespace

   objectName = 'project_in_project'

   outFile = '%s/%s.tsv' % ( draftDir, objectName )

   with open(outFile, 'w') as OUT:
      
      OUT.write( '\t'.join(outputColumns[objectName]) + '\n' )

      for childID in sorted( entityAssociations[objectName] ):
         
         parentID = entityAssociations[objectName][childID]

         first = True

         for colName in outputColumns[objectName]:
            
            if not first:
               
               OUT.write('\t')

            first = False

            if re.search(r'id_namespace$', colName) is not None:
               
               OUT.write( idNamespace )
               
            elif colName == 'id':
               
               # Special case. Will need to change this if 'id' fields are eliminated or renamed.

               OUT.write( currentID )

            elif colName == 'parent_project_id':
               
               OUT.write( '%s' % parentID )

            elif colName == 'child_project_id':
               
               OUT.write( '%s' % childID )

            else:
               
               die('writeProjectInProject encountered unexpected column name ("' + colName + '"); aborting.')

            # end if ( switch on colName )

         # end for ( loop through column names for this object type in order )

         OUT.write( '\n' )

      # end for ( each objectID of this object type )

   # end with ( outFile opened as 'OUT' )

#-----------------------------------------------------------------------------------------
# end sub writeProjectInProject(  )
##########################################################################################

####### writePairwiseAssociationTables ###################################################
# 
# CALLED BY: main execution thread
# 
# Serialize the three pairwise inter-entity association tables.
# 
#-----------------------------------------------------------------------------------------

def writePairwiseAssociationTables(  ):
   
   global draftDir, entityAssociations, outputColumns, idNamespace

   for tableName in { 'file_describes_biosample', 'file_describes_subject', 'biosample_from_subject' }:
      
      outFile = '%s/%s.tsv' % ( draftDir, tableName )

      with open(outFile, 'w') as OUT:
         
         OUT.write( '\t'.join(outputColumns[tableName]) + '\n' )

         for agentID in sorted( entityAssociations[tableName] ):
            
            for patientID in entityAssociations[tableName][agentID]:
               
               OUT.write( '\t'.join( [ idNamespace, agentID, idNamespace, patientID ] ) + '\n')

#-----------------------------------------------------------------------------------------
# end sub writePairwiseAssociationTables(  )
##########################################################################################

####### writeSubjectRoleTaxonomy #############################################################
# 
# CALLED BY: main execution thread
# 
# Link subjects, via predefined roles, to their NCBI taxonomy designation(s) -- in the
# case of HMP, all are human.
# 
#-----------------------------------------------------------------------------------------

def writeSubjectRoleTaxonomy(  ):

   global draftDir, objectsToWrite, humanTaxID, singleOrgRoleID, idNamespace

   tableName = 'subject_role_taxonomy'

   outFile = '%s/%s.tsv' % ( draftDir, tableName )

   with open(outFile, 'w') as OUT:
      
      OUT.write( '\t'.join(outputColumns[tableName]) + '\n' )

      for subjectID in sorted(objectsToWrite['subject']):
         
         OUT.write( '\t'.join( [ idNamespace, subjectID, singleOrgRoleID, humanTaxID ] ) + '\n' )
      #
   #

#-----------------------------------------------------------------------------------------
# end sub writeSubjectRoleTaxonomy(  )
##########################################################################################

####### createFakeDates ##################################################################
# 
# CURRENTLY DEPRECATED: REQUIRES REFERENTIAL INTEGRITY UPDATE
# 
# Create three separate fake date-stamps for all iHMP-associated
# file objects, for interface testing. THIS FUNCTION SHOULD NOT
# BE CALLED IN A PRODUCTION VERSION OF THIS SCRIPT (check main
# execution block below for invocation).
# 
#-----------------------------------------------------------------------------------------

def createFakeDates(  ):
   
   global objectsToWrite, objectsInProjects

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

   # objectsInProjects['file'][fileID] = { containing_set_1, containing_set_2, ... }

   # objectsToWrite['auxiliary_data']['file'][fileID][fieldName]['0'] = dateVal

   for fileID in objectsToWrite['file']:
      
      for setKeyword in ['ibd', 't2d', 'momspi']:
         
         if setID[setKeyword] in objectsInProjects['file'][fileID]:
            
            if fileID not in objectsToWrite['auxiliary_data']['file']:
               
               objectsToWrite['auxiliary_data']['file'][fileID] = {}

            objectsToWrite['auxiliary_data']['file'][fileID]['creationDate'] = { '0': creationDate[setKeyword] }
            objectsToWrite['auxiliary_data']['file'][fileID]['repoSubmissionDate'] = { '0': repoSubmissionDate[setKeyword] }
            objectsToWrite['auxiliary_data']['file'][fileID]['cfdeIngestDate'] = { '0': cfdeIngestDate[setKeyword] }
         #
      #
   #

#-----------------------------------------------------------------------------------------
# end sub createFakeDates(  )
##########################################################################################

##########################################################################################
##########################################################################################
##########################################################################################
#                                        PARAMETERS
##########################################################################################
##########################################################################################
##########################################################################################

# id_namespace string assigned to HMP by CFDE for identifier uniqueness protection

idNamespace = 'cfde_id_namespace:2'

##########################################################################################
# Globally unique (at least, within the current idNamespace) disambiguator index for
# on-the-fly ID generation.

uniqueNumericIndex = 0

##########################################################################################
# Directory containing raw JSON dump of full HMP couchDB database.

dbDir = '000_raw_HMP_couchDB_JSON_backup_dump__2019_10_20'

# List of banned nodeIDs for HMP DB: ignore test nodes and other such noise.

banList = '%s/banned_IDs.txt' % dbDir

bannedIDs = set()

# Raw JSON dump of full HMP couchDB database.

inFile = '%s/osdf-ihmp.igs.umaryland.edu_couchdb_2019-10-20.json' % dbDir

##########################################################################################
# Location of the Frictionless Data Data Package JSON Schema file describing Level 1
# C2M2 TSV instance data.

schemaLoc = '001_data_package_JSON_schema_Level_1/C2M2_Level_1.datapackage.json'

##########################################################################################
# CFDE-controlled vocabularies whose use is required for compliance with C2M2
# Level 1 ETL instance specification. To be provided to each DCC along with usage
# notes.

internalCvDir = '002_internal_CV_TSVs'

##########################################################################################
# Subdirectory containing full info for external CVs, versioned to match the current
# data release.

cvRefDir = '003_external_CVs_versioned_reference_files'

# Map of CV names to reference files.

cvFile = {
   
   'EDAM' : '%s/EDAM.version_1.21.tsv' % cvRefDir,
   'OBI' : '%s/OBI.version_2019-08-15.obo' % cvRefDir,
   'Uberon' : '%s/uberon.version_2019-06-27.obo' % cvRefDir
}

##########################################################################################
# Directory containing TSVs mapping named fields to terms in
# third-party ontologies

mapDir = '004_maps_from_native_HMP_terms_to_corresponding_C2M2_CVs'

# Map-file locations, keyed by the name of the output-object property field
# meant to store references to the relevant ontology

mapFiles = {
   
   'file.file_format' : '%s/native_keyword_to_EDAM_for_file.file_format.tsv' % mapDir,
   'file.data_type' : '%s/native_keyword_to_EDAM_for_file.data_type.tsv' % mapDir,
   'biosample.assay_type' : '%s/native_keyword_to_OBI_for_biosample.assay_type.tsv' % mapDir,
   'biosample.anatomy' : '%s/native_keyword_to_Uberon_for_biosample.anatomy.tsv' % mapDir
}

# Functions mapping value-constrained DCC-internal metadata field values
# to third-party CV term IDs for populating selected data fields in the
# output model.

enumMap = {}

##########################################################################################
# Directory in which (constant-value) HMP-written stub TSVs will be stored prior to
# combination with programmatically generated ETL data and CFDE-provided CV and JSON
# files for bdbagging.

stubDir = '005_HMP-specific_stub_TSVs'

##########################################################################################
# Directory in which HMP-written ETL data TSVs will be produced prior to combination with
# (constant) stub tables CFDE-provided CV and JSON files for bdbagging.

draftDir = '006_HMP-specific_ETL_TSVs'

##########################################################################################
# Directory in which HMP-written TSVs will be produced to track all controlled-vocabulary
# terms used throughout this Level 1 C2M2 instance (as specified in the Level 1
# specification).

termTableDir = '007_HMP-specific_CV_term_usage_TSVs'

##########################################################################################
# Final output directory.

outDir = 'HMP_C2M2_Level_1_preBag_ETL_instance_TSV_files'

# Temporary directory name used for bdbaggery.

bagDir = 'HMP_C2M2_Level_1_bdbag'

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
   
   '16s_dna_prep': 'biosample',
   'host_assay_prep': 'biosample',
   'host_seq_prep': 'biosample',
   'microb_assay_prep': 'biosample',
   'sample': 'biosample',
   'wgs_dna_prep': 'biosample',

   'subject': 'subject',

   # Skipping these two for the moment; they're not
   # connected properly to the rest of the data and
   # will require some extra treatment if they're to
   # be used at all.
   #   
   #   'metagenomic_project_catalog_entry': 'project',
   #   'reference_genome_project_catalog_entry': 'project',
   'project': 'project',
   'study': 'project',

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

# Native HMP object types that map to C2M2 'biosample' objects.

BiosampleNodeTypes = {
   
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

# Native HMP object types that map to C2M2 'project' objects.

ProjectNodeTypes = {

   # Skipping these two for the moment; they're not
   # connected properly to the rest of the data and
   # will require some extra treatment if they're to
   # be used at all.
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
   
   'file': [
      'id_namespace',
      'id',
      'project_id_namespace',
      'project',
      'persistent_id',
      'creation_time',
      'size_in_bytes',
      'sha256',
      'md5',
      'filename',
      'file_format',
      'data_type'
   ],
   'biosample': [
      'id_namespace',
      'id',
      'project_id_namespace',
      'project',
      'persistent_id',
      'creation_time',
      'assay_type',
      'anatomy'
   ],
   'subject': [
      'id_namespace',
      'id',
      'project_id_namespace',
      'project',
      'persistent_id',
      'creation_time',
      'granularity'
   ],
   'project': [
      'id_namespace',
      'id',
      'persistent_id',
      'abbreviation',
      'name',
      'description'
   ],
   'project_in_project':  [
      'parent_project_id_namespace',
      'parent_project_id',
      'child_project_id_namespace',
      'child_project_id'
   ],
   'collection': [
      'id_namespace',
      'id',
      'persistent_id',
      'abbreviation',
      'name',
      'description'
   ],
   'collection_in_collection': [
      'superset_collection_id_namespace',
      'superset_collection_id',
      'subset_collection_id_namespace',
      'subset_collection_id'
   ],
   'collection_defined_by_project': [
      'collection_id_namespace',
      'collection_id',
      'project_id_namespace',
      'project_id'
   ],
   'file_in_collection': [
      'file_id_namespace',
      'file_id',
      'collection_id_namespace',
      'collection_id'
   ],
   'biosample_in_collection': [
      'biosample_id_namespace',
      'biosample_id',
      'collection_id_namespace',
      'collection_id'
   ],
   'subject_in_collection': [
      'subject_id_namespace',
      'subject_id',
      'collection_id_namespace',
      'collection_id'
   ],
   'file_describes_biosample': [
      'file_id_namespace',
      'file_id',
      'biosample_id_namespace',
      'biosample_id'
   ],
   'file_describes_subject': [
      'file_id_namespace',
      'file_id',
      'subject_id_namespace',
      'subject_id'
   ],
   'biosample_from_subject': [
      'biosample_id_namespace',
      'biosample_id',
      'subject_id_namespace',
      'subject_id'
   ],
   'subject_role_taxonomy': [
      'subject_id_namespace',
      'subject_id',
      'role_id',
      'taxonomy_id'
   ]
}

##########################################################################################
# Serialization objects: initial load of HMP metadata

flatObjects = {}
nativeTypeToNodeID = {}
nodeIDToNativeType = {}
nativeTypeToNativeColNames = {}

##########################################################################################
# Intermediate (transform-layer) inter-entity association tracking structures

# Visit structures sit between biosamples and the subjects from which they're derived;
# we'll need this map to resolve and report direct biosample<->subject associations.

sampleToVisit = {}

# This tracks connections between "preparation" biosamples and the upstream biosamples
# from which they derived.

prepToBiosample = {}

# This links visits to subjects so that biosample<->visit<->subject associations can be
# flattened for Level 1 C2M2 compliance.

visitToSubject = {}

##########################################################################################
# Static presets for HMP (values known in advance):

# NCBI Taxonomy DB ID for human: HMP has only human subjects

humanTaxID = 'NCBI:txid9606'

# CFDE CV ID for subject granularity category 'single organism'

singleOrgGranularityID = 'cfde_subject_granularity:0'

# CFDE CV ID for subject role category 'single organism'

singleOrgRoleID = 'cfde_subject_role:0'

##########################################################################################
# Serialization objects: C2M2 output data structures

objectsToWrite = {}

entityAssociations = {
   
   'file_describes_biosample' : {},
   'file_describes_subject' : {},
   'biosample_from_subject' : {},
   'project_in_project' : {},
   'subject_role_taxonomy' : {}
}

# Inter-entity association DAG. Populated on the fly while loading entity data; scanned
# after completion to compute all required primary_project<->biosample|file|subject
# associations.
parents = {}

# Number of hops from each node to the closest root in the project DAG. Used to identify
# the most specific project division available for "primary project" foreign-key
# references.
projectDepth = {}

# Cache for cutting off containment-scan recursion.
containedIn = {}

##########################################################################################
##########################################################################################
##########################################################################################
#                                       EXECUTION
##########################################################################################
##########################################################################################
##########################################################################################






# Make all the subdirs from 000 through 007













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
# in pre-selected reference versions of controlled vocabularies.

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

# Gather all data needed to serialize 'biosample' objects.

progressReport("Building biosample table...")

populateBiosamples()

# Gather all data needed to serialize 'subject' objects.

progressReport("Building subject table...")

populateSubjects()

# Process all HMP container structures which will be
# mapped to C2M2 'project' objects.

progressReport("Building project table...")

populateProjects()

# Process all native-HMP 'visit' data to close transitive project network around
# subjects, files and biosamples.

progressReport("Processing visit-to-subject links and closing transitivity on biosample associations...")

processVisits()

linkBiosamples()

# Process all containment links cached during previous passes.

progressReport("Processing project containment relationships and populating required foreign keys...")

processProjectContainment()

"""

##########################################################################################

# BELOW IS FOR TESTING ONLY: DISABLE FOR HONEST DATA IMPORT
# Create fake dates for file objects & store them in auxiliary_data.
# Create three fake dates for each file: creation, repository insertion (e.g. SRA), and CFDE ingest.
# We'll be decorating file objects from the three iHMP subprojects: IBDMDB, T2D and momspi.
progressReport("Inserting fake creation/archive/repo dates for later query testing...")

createFakeDates()
# ABOVE IS FOR TESTING ONLY: DISABLE FOR HONEST DATA IMPORT

##########################################################################################

"""

# Serialize all 'file' objects into a TSV.

progressReport("Writing file table...")

writeTable('file')

# Serialize all 'biosample' objects into a TSV.

progressReport("Writing biosample table...")

writeTable('biosample')

# Serialize all 'subject' objects into a TSV.

progressReport("Writing subject table...")

writeTable('subject')

# Serialize all 'project' objects into a TSV.

progressReport("Writing project table...")

writeTable('project')

# Serialize all info on project containment.

progressReport("Writing project containment DAG...")

writeProjectInProject()

# Serialize the three pairwise inter-entity association tables.

progressReport("Writing file_describes_biosample, file_describes_subject and biosample_from_subject association tables...")

writePairwiseAssociationTables()

# Link subjects, via predefined roles, to their NCBI taxonomy
# designation(s) -- in the case of HMP, all are human.

progressReport("Writing subject-role-taxonomy table...")

writeSubjectRoleTaxonomy()

# Call the standalone script to detect and decorate all external CV terms used
# in this serialized C2M2 instance, and save the results to the required
# term-tracker tables.

os.system('python3 ./build_term_tables.py')

################################### OUTPUT PACKAGING #####################################

# Include the Table-Schema JSON document in the output for reference, plus all the
# various TSV subcollections.

progressReport("Copying component filesets to bdbag data store...")

# JSON Schema.

os.system('cp ' + schemaLoc + ' ' + outDir)

# CFDE-internal CV dictionary TSVs.

os.system('cp ' + internalCvDir + '/* ' + outDir)

# (Constant-value) stub tables.

os.system('cp ' + stubDir + '/* ' + outDir)

# Programmatically-generated ETL data tables.

os.system('cp ' + draftDir + '/* ' + outDir)

# CV term-usage tracking TSVs for downstream display decoration.

os.system('cp ' + termTableDir + '/* ' + outDir)

# Make a BDBag for final delivery and rename it to remove local indexing info.

progressReport("Making bdbag...")

os.system('mv ' + outDir + ' ' + bagDir);

os.system('bdbag --quiet --archiver tgz ' + bagDir);

# Revert the intermediate output directory from BDBag format to avoid
# chaos and despair when running this script multiple times without
# clearing outputs.

os.system('bdbag --quiet --revert ' + bagDir);

os.system('mv ' + bagDir + ' ' + outDir);

progressReport("done!")

################################# END OF MAIN EXECUTION ##################################


