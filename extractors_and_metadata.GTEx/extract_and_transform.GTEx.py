#!/usr/bin/env python3

##########################################################################################
# AUTHOR INFO
##########################################################################################

# Arthur Brady (Univ. of MD Inst. for Genome Sciences) wrote this script to extract
# GTEx (v7) experimental data and transform it to conform to the draft C2M2 data
# specification prior to ingestion into a central CFDE database.

# Creation date: 2019-08-28
# Lastmod date unless I forgot to change it: 2019-10-09

# contact email: abrady@som.umaryland.edu

import sys
import os
import re

##########################################################################################
# ENABLE DICT ITEM AUTOVIVIFICATION
##########################################################################################

class AutoVivifyingDict(dict):
   
   """Replicate Perl's nested-hash autovivification feature."""

   def __getitem__(self, item):
      
      try:
         
         return dict.__getitem__(self, item)

      except KeyError:
         
         value = self[item] = type(self)()
         return value

##########################################################################################
# SUBROUTINES
##########################################################################################

def die( errorMessage ):
   
   print('\n   FATAL: %s\n' % errorMessage, file=sys.stderr)

   sys.exit(-1)

def getNewID( prefix ):
   
   global uniqueNumericIndex

   if prefix == '':
      
      die('getNewID() called with no ID prefix; aborting.')

   newID = '%s%08d' % (prefix, uniqueNumericIndex)

   uniqueNumericIndex = uniqueNumericIndex + 1

   return newID

# Load functions mapping value-constrained internal GTEx metadata field values
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

# end sub loadEnumMaps()

def consumeSubjectData( inFile ):
   
   global subjects, auxData, SubjectTaxonomy, SubjectsInSubjectGroups

   with open( inFile, 'r' ) as IN:
      
      header = IN.readline()

      colNames = re.split(r'\t', header)

      for line in IN:
         
         subjectID, sexCode, ageRange, hardyScaleCode = re.split(r'\t', line.rstrip('\r\n'))

         # This subject is a single organism (human) -- this is constant for this dataset.

         subjects[subjectID] = {
            
            'granularity' : 'SUBJECT_GRANULARITY_ID.0'
         }

         # Cache all DCC-specific extension data about this Subject. Go ahead
         # and create records for empty values to avoid seriously screwing up
         # downstream existence expectations about associations between data
         # structures.

         auxData['Subject'][subjectID] = {
            
            'GTExSubjectSex' : sexCode,
            'GTExSubjectAgeRange' : ageRange,
            'GTExSubjectHardyScaleCode' : hardyScaleCode
         }

         # Link up subjects with the NCBI taxonomy code for "human."

         SubjectTaxonomy[subjectID] = {
            
            baseURL['NCBI_Taxonomy_DB'] + '9606' : 1
         }

         # The next 2 blocks are provisionally deprecated until support for
         # autoloading using NCBI taxonomy terms is built. Right now,
         # there's just one species, so this isn't enabled.
         # 
         # if not 'NCBI_Taxonomy_DB' in termsUsed:
         #    
         #    termsUsed['NCBI_Taxonomy_DB'] = {
         #       
         #       '9606': 1
         #    }
         # 
         # else:
         #    
         #    termsUsed['NCBI_Taxonomy_DB']['9606'] = 1
         #
         # if not 'NCBI_Taxonomy_DB' in fullURL:
         #    
         #    fullURL['NCBI_Taxonomy_DB'] = {
         #       
         #       '9606' : baseURL['NCBI_Taxonomy_DB'] + '9606'
         # 
         # else:
         #    
         #    fullURL['NCBI_Taxonomy_DB']['9606'] = baseURL['NCBI_Taxonomy_DB'] + '9606'
         #

         # TEMPORARY: Assignment to top-level subject group is hard-coded here.
         # Refinements will require group decisions on how to parse the GTEx
         # subject population.

         SubjectsInSubjectGroups[subjectID] = {
            
            topSubjectGroupID : 1
         }

      # end for ( line iterator on subject-metadata file )

   # end with ( subject-metadata file opened as 'IN' )

# end sub consumeSubjectData()

def consumeSampleData( inFile ):
   
   global subjects, enumMap, termsUsed, fullURL, samples, baseURL, auxData

   with open( inFile, 'r' ) as IN:
      
      header = IN.readline()

      colNames = re.split(r'\t', header)

      for line in IN:
         
         ( SAMPID, SMATSSCR, SMCENTER, SMPTHNTS, SMRIN,
         SMTS, SMTSD, SMUBRID,
         SMTSISCH, SMTSPAX,
         SMNABTCH, SMNABTCHT, SMNABTCHD,
         SMGEBTCH, SMGEBTCHD, SMGEBTCHT,
         SMAFRZE

#       Ignoring these for the current iteration. They're all DCC-specific
#       sample metadata according to the current draft model.
#
#        SMGTC, SME2MPRT, SMCHMPRS, SMNTRART, SMNUMGPS, SMMAPRT,
#        SMEXNCRT, SM550NRM, SMGNSDTC, SMUNMPRT, SM350NRM, SMRDLGTH,
#        SMMNCPB, SME1MMRT, SMSFLGTH, SMESTLBS, SMMPPD, SMNTERRT,
#        SMRRNANM, SMRDTTL, SMVQCFL, SMMNCV, SMTRSCPT, SMMPPDPR,
#        SMCGLGTH, SMGAPPCT, SMUNPDRD, SMNTRNRT, SMMPUNRT, SMEXPEFF,
#        SMMPPDUN, SME2MMRT, SME2ANTI, SMALTALG, SME2SNSE, SMMFLGTH,
#        SME1ANTI, SMSPLTRD, SMBSMMRT, SME1SNSE, SME1PCTS, SMRRNART,
#        SME1MPRT, SMNUM5CD, SMDPMPRT, SME2PCTS

         ) = re.split(r'\t', line.rstrip('\r\n'))[0:17]

         # Default type for the following is 'string' unless otherwise indicated
         # in a comment.

         sampleID = SAMPID
         autolysisScore = SMATSSCR     # int stored as decimal
         collectionSiteCode = SMCENTER # enum: short strings
         pathologyNotes = SMPTHNTS
         rnaIntegrityNumber = SMRIN    # decimal

         tissueType = SMTS             # enum: general tissue-type keywords
         specificTissueType = SMTSD    # enum: specific tissue-type keywords
         uberonID = SMUBRID            # Note: This encodes SMTSD, not SMTS.

         ischemicTime = SMTSISCH       # int (minutes) stored as decimal
         timeSpentInPaxGeneFixative = SMTSPAX      # int (min.) as decimal

         nucleicAcidIsolationBatchID = SMNABTCH 
         nucleicAcidIsolationBatchProcess = SMNABTCHT
         nucleicAcidIsolationBatchDate = SMNABTCHD  # date: MM/DD/YYYY

         genotypeOrExpressionBatchID = SMGEBTCH
         genotypeOrExpressionBatchDate = SMGEBTCHD  # date: MM/DD/YYYY
         genotypeOrExpressionBatchProcess = SMGEBTCHT

         analysisType = SMAFRZE # enum: { RNASEQ, WGS, WES, OMNI, EXCLUDE }

#      # See if there exist any reasons to ignore the current sample record.
#      # 
#      # At present, this happens if & only if the analysis-type code from
#      # 'SMAFRZE' is 'EXCLUDE', a code used by GTEx to flag samples as
#      # recommended for exclusion from analysis due to genotype anomalies
#      # and other QC factors.
#      # 
#      # Sanity checks are performed at more basic logical levels: if the
#      # incoming data fails any of the verifications regarding expected
#      # content, the whole script will fail -- silently excluding some
#      # samples for nonconformity would be a disastrous idea. Either the
#      # data should be conformant as-is, or we should change our expectations
#      # about its structure.
#
#      sampleOK = True
#
#      # Exclude this sample record?
#
#      if analysisType == 'EXCLUDE':
#         
#         sampleOK = False

         # Incoming data conformant to expectations?

         subjectID = 'SOMETHING_WENT_WRONG'

         # Subject IDs should be encoded in sample IDs: the latter are segmented
         # by dashes, and the subject ID should be the first two dash-separated
         # fields of the sample ID.

         matchResult = re.search(r'^([^-]+)-([^-]+)-', sampleID)

         if matchResult is None:
            
            die('Can\'t parse subject ID from sample ID "%s"; aborting.' % sampleID)

         else:
            
            subjectID = '%s-%s' % ( matchResult.group(1), matchResult.group(2) )

         if subjectID not in subjects:
            
            die('Parsed subject ID for sample "%s" to "%s", but there is no record of that subject ID in the input fileset.' % ( sampleID, subjectID ))

         # Despite GTEx's description of the 'SMUBRID' field as "Uberon ID", this
         # column occasionally contains EFO IDs instead (stored as integers
         # prefixed with 'EFO_' instead of the unprefixed integers used to
         # reference actual Uberon IDs), apparently for anatomical
         # structures not represented in Uberon.
         # 
         # No attempt is being made at present to resolve these schema-breaking
         # IDs to best-match Uberon IDs; instead, any non-Uberon IDs will simply
         # be replaced with the Uberon ID for 'anatomical part'.

         matchResult = re.search(r'^\d+$', uberonID)

         if matchResult is None:
            
            uberonID = '0001062'

         # Resolve mapped enum values.

         if 'BioSample.sample_type' not in enumMap or analysisType not in enumMap['BioSample.sample_type']:
            
            die('Can\'t map analysis type "%s" to OBI term; aborting.\n\n   Please update mapFiles[\'BioSample.sample_type\'] as needed.' % analysisType )

         analysisType = enumMap['BioSample.sample_type'][analysisType]

         savedAnalysisType = re.sub(r'OBI_', r'OBI:', analysisType)

         termsUsed['SampleType'][savedAnalysisType] = 1

         fullURL['SampleType'][savedAnalysisType] = baseURL['OBI'] + analysisType

         # Build and cache the sample data structure. Bottom-level hash keys,
         # here, should exactly match output-model property names for the
         # BioSample object.

         samples[sampleID]['subject'] = subjectID

         samples[sampleID]['sample_type'] = baseURL['OBI'] + analysisType

         samples[sampleID]['anatomy'] = baseURL['Uberon'] + uberonID

         termsUsed['Anatomy']['UBERON:' + uberonID] = 1

         fullURL['Anatomy']['UBERON:' + uberonID] = baseURL['Uberon'] + uberonID

         # TEMPORARY: "BioSample.protocol" is currently hard-coded to a constant
         # ID value because we currently have no pre-built model set for
         # protocols; the field will reference a stub record, meant to be
         # replaced later on with references to specific protocol models.

         samples[sampleID]['protocol'] = 'PROTOCOL_ID.0'

         # TEMPORARY: (See previous comment.)

         samples[sampleID]['rank'] = 0

         # This BioSample represents a batch of isolated molecules, not an
         # unprocessed tissue sample. Encode the 'creation date' for this
         # BioSample using the appropriate batch-isolation date (DNA or RNA).

         creationDate = nucleicAcidIsolationBatchDate

         if analysisType == 'OBI_0000880':
            
            # RNA extract. Switch the BioSample creation date to the
            # corresponding isolation event.

            creationDate = genotypeOrExpressionBatchDate

         matchResult = re.search(r'not\s+reported', creationDate, re.IGNORECASE)

         if not ( matchResult is None ):
            
            creationDate = ''

         else:
            
            matchResult = re.search(r'^\d+\/\d+\/\d+$', creationDate)

            if matchResult is None:
               
               die('Malformed date value "%s" for sample "%s"; aborting.' % ( creationDate, sampleID ) )

            else:
               
               ( month, day, year ) = re.split(r'\/', creationDate)

               creationDate = '%d-%02d-%02d' % ( int(year), int(month), int(day) )

         samples[sampleID]['event_ts'] = creationDate

         # Cache all DCC-specific extension data about this BioSample. Go ahead
         # and create records for empty values to avoid seriously screwing up
         # downstream expectations about associations between data structures.

         auxData['BioSample'][sampleID]['GTExBioSampleAutolysisScore'] = autolysisScore
         auxData['BioSample'][sampleID]['GTExBioSampleCollectionSiteCode'] = collectionSiteCode
         auxData['BioSample'][sampleID]['GTExBioSamplePathologyNotes'] = pathologyNotes
         auxData['BioSample'][sampleID]['GTExBioSampleRnaIntegrityNumber'] = rnaIntegrityNumber
         auxData['BioSample'][sampleID]['GTExBioSampleIschemicTime'] = ischemicTime
         auxData['BioSample'][sampleID]['GTExBioSamplePaxGeneFixativeTime'] = timeSpentInPaxGeneFixative

      # end ( line iterator on input sample-data file )

   # end with ( sample-metadata file opened as 'IN' )

# end sub consumeSampleData()

def consumeLocationData( dataType ):
   
   global locationFiles, files, termsUsed, fullURL, dataEvents, analyzedBy, assayedBy, producedBy, FilesInDatasets

   inFile = locationFiles[dataType]

   with open( inFile, 'r' ) as IN:
      
      header = IN.readline()

      for line in IN:
         
         # RNA-Seq.reference_alignments: sample_id cram_file_gcp cram_index_gcp cram_file_aws cram_index_aws cram_file_md5 cram_file_size cram_index_md5
         # WGS.reference_alignments:     sample_id cram_file_gcp cram_index_gcp cram_file_aws cram_index_aws cram_file_md5 cram_file_size cram_index_md5

         ( sampleID, cramFileGCP, cramIndexGCP, cramFileAWS, cramIndexAWS, cramFileMD5, cramFileSize, cramIndexMD5 ) = re.split(r'\t', line.rstrip('\r\n'))

         cramBaseName = re.sub(r'^.*\/([^\/]+)$', r'\1', cramFileGCP)

         # Build an object to track this File along with a DataEvent object to reference its creation.

         # BEGIN TEMPORARY BLOCK: Right now I don't have raw-sequence file location
         # data. The data I do have is for read alignments to the human
         # reference genome; I'm going to have to create dummy File objects
         # (and associated DataEvents) to represent sequence files in order to
         # connect associated Sample objects to these alignment File objects.

         if sampleID not in samples:
            
            die( 'Loaded file referencing unknown sample ID "%s"; aborting.' % sampleID );

         # Create a File object for the raw sequence data.

         seqFileID = getNewID('FILE_ID.')

         files[seqFileID]['url'] = 'gs://UNKNOWN_LOCATION.RAW_SEQUENCES.' + seqFileID
         files[seqFileID]['information_type'] = baseURL['EDAM'] + enumMap['File.information_type']['Generic_sequence']
         files[seqFileID]['file_format'] = baseURL['EDAM'] + enumMap['File.file_format']['FASTQ']
         files[seqFileID]['length'] = ''
         files[seqFileID]['filename'] = 'RAW_SEQUENCES.' + seqFileID
         files[seqFileID]['md5'] = ''

         termsUsed['InformationType'][baseURL['EDAM'] + enumMap['File.information_type']['Generic_sequence']] = 1
         fullURL['InformationType'][baseURL['EDAM'] + enumMap['File.information_type']['Generic_sequence']] = baseURL['EDAM'] + enumMap['File.information_type']['Generic_sequence']
         termsUsed['FileFormat'][baseURL['EDAM'] + enumMap['File.file_format']['FASTQ']] = 1
         fullURL['FileFormat'][baseURL['EDAM'] + enumMap['File.file_format']['FASTQ']] = baseURL['EDAM'] + enumMap['File.file_format']['FASTQ']

         # Create a File record for the alignment results.

         alnFileID = getNewID('FILE_ID.')

         files[alnFileID]['url'] = cramFileGCP
         files[alnFileID]['information_type'] = baseURL['EDAM'] + enumMap['File.information_type'][dataType]
         files[alnFileID]['file_format'] = baseURL['EDAM'] + enumMap['File.file_format']['CRAM']
         files[alnFileID]['length'] = cramFileSize
         files[alnFileID]['filename'] = cramBaseName
         files[alnFileID]['md5'] = cramFileMD5

         termsUsed['InformationType'][baseURL['EDAM'] + enumMap['File.information_type'][dataType]] = 1
         fullURL['InformationType'][baseURL['EDAM'] + enumMap['File.information_type'][dataType]] = baseURL['EDAM'] + enumMap['File.information_type'][dataType]
         termsUsed['FileFormat'][baseURL['EDAM'] + enumMap['File.file_format']['CRAM']] = 1
         fullURL['FileFormat'][baseURL['EDAM'] + enumMap['File.file_format']['CRAM']] = baseURL['EDAM'] + enumMap['File.file_format']['CRAM']

         # Sequencing event.

         dataEventID = getNewID('DATA_EVENT.')

         dataEvents[dataEventID]['protocol'] = 'PROTOCOL_ID.0'
                                             # Generic sequencing event.
         dataEvents[dataEventID]['method'] = baseURL['OBI'] + 'OBI_0600047'
         termsUsed['Method']['OBI:0600047'] = 1
         fullURL['Method']['OBI:0600047'] = baseURL['OBI'] + 'OBI_0600047'

         dataEvents[dataEventID]['rank'] = 0
                                               # Copy for now from parent sample object.
         dataEvents[dataEventID]['event_ts'] = samples[sampleID]['event_ts']

         assayedBy[sampleID][dataEventID] = 1
         producedBy[seqFileID][dataEventID] = 1

         # Alignment event.

         dataEventID = getNewID('DATA_EVENT.')

         dataEvents[dataEventID]['protocol'] = 'PROTOCOL_ID.0'
                                             # Generic alignment event.
         dataEvents[dataEventID]['method'] = baseURL['OBI'] + 'OBI_0002567'
         termsUsed['Method']['OBI:0002567'] = 1
         fullURL['Method']['OBI:0002567'] = baseURL['OBI'] + 'OBI_0002567'

         dataEvents[dataEventID]['rank'] = 1
                                               # Copy for now from parent sample object.
         dataEvents[dataEventID]['event_ts'] = samples[sampleID]['event_ts']

         analyzedBy[seqFileID][dataEventID] = 1
         producedBy[alnFileID][dataEventID] = 1

         # END TEMPORARY BLOCK

         # TEMPORARY: Assignments to predefined Dataset groups are hard-coded
         # here. Refinements will require group decisions on how to parse the
         # GTEx file (sub)collections.

         FilesInDatasets[seqFileID][datasetIDs['sequence_files']] = 1

         if dataType == 'RNA-Seq.reference_alignments':
            
            FilesInDatasets[alnFileID][datasetIDs['rnaseq_alignment_files']] = 1

         elif dataType == 'WGS.reference_alignments':
            
            FilesInDatasets[alnFileID][datasetIDs['wgs_alignment_files']] = 1

      # end ( line iterator on file-location input file )

   # end with ( file-location metadata file opened as 'IN' )

# end sub consumeLocationData()

def writeDummyTables( stubHash ):
   
   global outDir

   for tableName in sorted( stubHash.keys() ):
      
      tableFile = outDir + '/' + tableName + '.tsv'

      with open(tableFile, 'w') as OUT:
         
         OUT.write( '\t'.join( ('id', 'url', 'name', 'description') ) + '\n' )

         OUT.write( '\t'.join( (stubHash[tableName]['id'], stubHash[tableName]['url'], stubHash[tableName]['name'], stubHash[tableName]['description']) ) + '\n' )

      # end with ( tableName opened as 'OUT' )

   # end for tableName in sorted( stubHash.keys())

# end sub writeDummyTables()

def writeDataTables():
   
   global outDir, auxData, samples, subjects, files, dataEvents, datasets, subjectGroups
   global assayedBy, observedBy, analyzedBy, producedBy
   global SubjectTaxonomy, SubjectsInSubjectGroups, FilesInDatasets, DatasetsInDatasets
   
   # Core data tables.

   writeTable( 'BioSample', samples, ['subject', 'sample_type', 'anatomy', 'protocol', 'rank', 'event_ts'] )
   writeTable( 'Subject', subjects, ['granularity'] )
   writeTable( 'File', files, ['url', 'information_type', 'file_format', 'length', 'filename', 'md5'] )
   writeTable( 'DataEvent', dataEvents, ['method', 'platform', 'protocol', 'rank', 'event_ts'] )
   writeTable( 'Dataset', datasets, ['data_source', 'url', 'title', 'description'] )
   writeTable( 'SubjectGroup', subjectGroups, ['title', 'description'] )

   # Association tables.

   writeAssociationTable( 'ProducedBy', producedBy, ['FileID', 'DataEventID'] )
   writeAssociationTable( 'AssayedBy', assayedBy, ['BioSampleID', 'DataEventID'] )
   writeAssociationTable( 'ObservedBy', observedBy, ['SubjectID', 'DataEventID'] )
   writeAssociationTable( 'AnalyzedBy', analyzedBy, ['FileID', 'DataEventID'] )
   writeAssociationTable( 'SubjectTaxonomy', SubjectTaxonomy, ['SubjectID', 'NCBITaxonID'] )
   writeAssociationTable( 'SubjectsInSubjectGroups', SubjectsInSubjectGroups, ['SubjectID', 'SubjectGroupID'] )
   writeAssociationTable( 'FilesInDatasets', FilesInDatasets, ['FileID', 'DatasetID'] )
   writeAssociationTable( 'DatasetsInDatasets', DatasetsInDatasets, ['ContainedDatasetID', 'ContainingDatasetID'] )

   # Auxiliary data for Subjects.

   outFile = outDir + '/AuxiliaryData.tsv'

   with open( outFile, 'w' ) as OUT:
      
      OUT.write( 'ObjectType\tObjectID\tDataDescription\tValue\n' )

      for objectType in sorted( auxData.keys() ):
         
         for objectID in sorted( auxData[objectType].keys() ):
            
            for dataDesc in sorted( auxData[objectType][objectID].keys() ):
               
               value = str(auxData[objectType][objectID][dataDesc])

               OUT.write( '\t'.join( [objectType, objectID, dataDesc, value] ) + '\n' )

# end sub writeDummyTables()

def writeTable( fileBase, dataHash, fieldNames ):
   
   global outDir

   outFile = outDir + '/' + fileBase + '.tsv'

   with open(outFile, 'w') as OUT:
      
      OUT.write( 'id\t' + '\t'.join( fieldNames ) + '\n' )

      for objectID in sorted( dataHash.keys() ):
         
         OUT.write(objectID)

         for fieldName in fieldNames:
            
            OUT.write('\t')

            if fieldName in dataHash[objectID]:
               
               OUT.write( str(dataHash[objectID][fieldName]) )

         OUT.write('\n')

# end sub writeTable()

def writeAssociationTable( fileBase, dataHash, fieldNames ):
   
   global outDir

   # writeAssociationTable('SubjectTaxonomy', SubjectTaxonomy, 'SUBJECT_ID', 'TAXON_ID')

   outFile = outDir + '/' + fileBase + '.tsv'

   with open(outFile, 'w') as OUT:
      
      OUT.write( '\t'.join( fieldNames ) + "\n" )

      # TEMPORARY? Assuming exactly two columns (ID -> VALUE or ID -> ID), here.

      for objectID in sorted( dataHash.keys() ):
         
         for val in sorted( dataHash[objectID].keys() ):
            
            OUT.write( '\t'.join( [objectID, val] ) + '\n' )

# end sub writeAssociationTable()

def writeOntologyReferenceTables(  ):
   
   global termsUsed, ontoMap, cvFile, fullURL

#  ontoMap = {
#     
#     'SampleType' : 'OBI',
#     'Anatomy' : 'Uberon',
#     'InformationType' : 'EDAM',
#     'FileFormat' : 'EDAM',
#     'Method' : 'OBI'
#  }

   for vocabTable in termsUsed.keys():
      
      cv = ontoMap[vocabTable]

      refFile = cvFile[cv]

      with open( refFile, 'r' ) as IN:
         
         ontoData = AutoVivifyingDict()

         if ( cv == 'OBI' ) or ( cv == 'Uberon' ):
            
            # We have OBO files for these.

            recording = False

            currentTerm = ''

            for line in IN:
               
               line = line.rstrip('\r\n')
            
#         matchResult = re.search(r'^\d+$', uberonID)
#         if matchResult is None:
#         savedAnalysisType = re.sub(r'OBI_', r'OBI:', analysisType)
#            subjectID = '%s-%s' % ( matchResult.group(1), matchResult.group(2) )
#         (stuff, stuff ) = re.split(r'\t', line.rstrip('\r\n'))[0:17]
               matchResult = re.search(r'^id:\s+(\S.*)$', line)

               if not( matchResult is None ):
                  
                  currentTerm = matchResult.group(1)

                  if currentTerm in termsUsed[vocabTable]:
                     
                     recording = True

                     if not ( currentTerm in fullURL[vocabTable] ):

                        die('No URL cached for used CV (%s) term "%s"; cannot proceed, aborting.' % ( cv, currentTerm ) )

                  else:
                     
                     currentTerm = ''

                     # (Recording is already switched off by default.)

               elif not( re.search(r'^\[Term\]', line) is None ):
                  
                  recording = False

               elif recording:
                  
                  if not ( re.search(r'^name:\s+(\S*.*)$', line) is None ):
                     
                     ontoData[fullURL[vocabTable][currentTerm]]['name'] = re.search(r'^name:\s+(\S*.*)$', line).group(1)

                  elif not ( re.search(r'^def:\s+\"(.*)\"[^\"]*$', line) is None ):
                     
                     ontoData[fullURL[vocabTable][currentTerm]]['description'] = re.search(r'^def:\s+\"(.*)\"[^\"]*$', line).group(1)

                  elif not ( re.search(r'^def:\s+', line) is None ):
                     
                     die('Unparsed def-line in %s OBO file: "%s"; aborting.' % ( cv, line ) )

                  elif not ( re.search(r'^synonym:\s+\"(.*)\"[^\"]*$', line) is None ):
                     
                     synonym = re.search(r'^synonym:\s+\"(.*)\"[^\"]*$', line).group(1)

                     if str(ontoData[fullURL[vocabTable][currentTerm]]['synonyms']) != '{}':
                        
                        ontoData[fullURL[vocabTable][currentTerm]]['synonyms'] = str(ontoData[fullURL[vocabTable][currentTerm]]['synonyms']) + '|' + synonym

                     else:
                        
                        ontoData[fullURL[vocabTable][currentTerm]]['synonyms'] = synonym

               # end if ( line-type selector switch )

            # end for ( input file line iterator )

         elif cv == 'EDAM':
            
            # We have a CV-specific TSV for this one.

            header = IN.readline()

            for line in IN:
               
               line = line.rstrip('\r\n')

               ( objectID, name, synonyms, definition ) = re.split(r'\t', line)[0:4]

               if str(termsUsed[vocabTable][objectID]) != '{}':
                  
                  # There are some truly screwy things allowed inside
                  # tab-separated fields in this file. Clean them up.

                  name = name.strip().strip('"\'').strip()

                  synonyms = synonyms.strip().strip('"\'').strip()

                  definition = definition.strip().strip('"\'').strip()

                  ontoData[objectID]['name'] = name;
                  ontoData[objectID]['synonyms'] = synonyms;
                  ontoData[objectID]['description'] = definition;

               # end if ( objectID in termsUsed[vocabTable] )

            # end for ( input file line iterator )
         
         # end if ( cv type switch )

      # end with ( refFile as IN )

      outFile = outDir + '/' + vocabTable + '.tsv'

      with open( outFile, 'w' ) as OUT:
         
         OUT.write('\t'.join( ['id', 'url', 'name', 'description', 'synonyms'] ) + '\n')

         for objectID in sorted( ontoData.keys() ):
            
            OUT.write('\t'.join([
                                    objectID,
                                    objectID,
                                    str(ontoData[objectID]['name']) if str(ontoData[objectID]['name']) != '{}' else '',
                                    str(ontoData[objectID]['description']) if str(ontoData[objectID]['description']) != '{}' else '',
                                    str(ontoData[objectID]['synonyms']) if str(ontoData[objectID]['synonyms']) != '{}' else ''

                                 ]) + "\n")

      # end with ( outFile as OUT )

   # end for ( each vocabTable in termsUsed.keys() )

   # TEMPORARY: Hack pending enabling of dynamic lookup for NCBI taxonomy IDs.

   outFile = outDir + '/NCBI_Taxonomy_DB.tsv'

   with open( outFile, 'w' ) as OUT:
      
      OUT.write( '\t'.join(['id', 'url', 'name', 'description', 'synonyms']) + '\n' )

      OUT.write( '\t'.join([
                              'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606',
                              'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606',
                              'Homo sapiens',
                              'Homo sapiens (modern human species)'

                          ]) + '\t\n' )

   # end with ( outFile as OUT )

# end sub writeOntologyReferenceTables()

##########################################################################################
# PARAMETERS
##########################################################################################

# Location of Table-Schema JSON document describing output. To be included in output
# collection for reference on ingest.

tableSchemaLoc = '000_tableschema/GTEx_C2M2_instance.json'

##########################################################################################
# Directory containing unmodified GTEx-published metadata files

inDir = '001_GTEx.v7.raw_input_files'

# Metadata describing samples and subjects

sampleFile = '%s/GTEx_v7_Annotations_SampleAttributesDS.txt' % inDir
subjectFile = '%s/GTEx_v7_Annotations_SubjectPhenotypesDS.txt' % inDir

# URLs locating CRAM file data assets (plus basic file metadata)

locationFiles = {
   
   'RNA-Seq.reference_alignments' : '%s/rnaseq_cram_files_v7_datacommons_011516.txt' % inDir,
   'WGS.reference_alignments' : '%s/wgs_cram_files_v7_hg38_datacommons_011516.txt' % inDir
}

locationFiles = AutoVivifyingDict(locationFiles)

##########################################################################################
# Base URLs (to be followed by term IDs) used to reconstruct full URLs
# referencing controlled-vocabulary terms in third-party ontologies

baseURL = {
   
   'EDAM' : 'http://edamontology.org/',
   'OBI' : 'http://purl.obolibrary.org/obo/',
   'Uberon' : 'http://purl.obolibrary.org/obo/UBERON_',
   'NCBI_Taxonomy_DB' : 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id='
}

baseURL = AutoVivifyingDict(baseURL)

##########################################################################################
# Track which external CV terms are used in the output; after main data tables are built,
# use this data to build CV lookup tables for terms used (primary key (ID): URL;
# minimal data fields: name, definition, synonyms; all but ID, name nullable).

termsUsed = AutoVivifyingDict()

fullURL = AutoVivifyingDict()

##########################################################################################
# Subdirectory containing full info for CVs, versioned to match the current data release.

cvRefDir = '003_CV_reference_data'

##########################################################################################
# Map of CV names to reference files. NCBI taxonomy should be added here; right now
# we're just making a stub reference for that CV due to the presence of just one
# referenced species (human).

cvFile = {
   
   'EDAM' : '%s/EDAM.version_1.21.tsv' % cvRefDir,
   'OBI' : '%s/OBI.version_2019-08-15.obo' % cvRefDir,
   'Uberon' : '%s/uberon.version_2019-06-27.obo' % cvRefDir
}

cvFile = AutoVivifyingDict(cvFile)

##########################################################################################
# Map of internal table names to external ontology reference IDs.

ontoMap = {
   
   'SampleType' : 'OBI',
   'Anatomy' : 'Uberon',
   'InformationType' : 'EDAM',
   'FileFormat' : 'EDAM',
   'Method' : 'OBI'
}

ontoMap = AutoVivifyingDict(ontoMap)

##########################################################################################
# TEMPORARY: Loadable objects representing CV table stubs that are
#            specific to CFDE. Once these objects have been built into a target DB
#            instance, code producing these tables will be replaced with code linking
#            relevant fields to existing CV table records.

tempTables = {
   
   'CommonFundProgram'  :  {
                              'id' : 'COMMON_FUND_PROGRAM_ID.0',
                              'url' : 'https://commonfund.nih.gov/gtex',
                              'name' : 'GTEx',
                              'description' : 'The Genotype-Tissue Expression (GTEx) project'
                           },
   'Platform'           :  {
                              'id' : 'PLATFORM_ID.0',
                              'url' : '',
                              'name' : 'Dummy_platform',
                              'description' : 'Description of the dummy platform'
                           },
   'Protocol'           :  {
                              'id' : 'PROTOCOL_ID.0',
                              'url' : '',
                              'name' : 'Dummy_protocol',
                              'description' : 'Description of the dummy protocol'
                           },
   'SubjectGranularity' :  {
                              'id' : 'SUBJECT_GRANULARITY_ID.0',
                              'url' : '',
                              'name' : 'single_organism',
                              'description' : 'A subject representing a single organism'
                           }
}

tempTables = AutoVivifyingDict(tempTables)

uniqueNumericIndex = 0

##########################################################################################
# Directory containing TSVs mapping named GTEx fields to terms in
# third-party ontologies

mapDir = '002_GTEx.v7.maps_from_GTEx_enums_to_controlled_vocabs'

# Map-file locations, keyed by the name of the output-object property field
# meant to store references to the relevant ontology

mapFiles = {
   
   'BioSample.sample_type' : '%s/GTEx_v7_Annotations_SampleAttributesDS.txt.SMAFRZE_to_OBI_for_BioSample.sample_type.tsv' % mapDir,
   'File.information_type' : '%s/File_information_type_keyword_to_EDAM_for_File.information_type.tsv' % mapDir,
   'File.file_format' : '%s/File_format_keyword_to_EDAM_for_File.file_format.tsv' % mapDir
}

mapFiles = AutoVivifyingDict(mapFiles)

##########################################################################################
# Output directory

outDir = '004_GTEx.v7.C2M2_preload.preBag_output_files'

##########################################################################################
# Global variables

# Functions mapping value-constrained internal GTEx metadata field values
# to third-party CV term IDs for populating selected data fields in the
# output model.

enumMap = AutoVivifyingDict()

# Data structure to populate the BioSample output table.

samples = AutoVivifyingDict()

# Data structure to populate the Subject output table.

subjects = AutoVivifyingDict()

# Data structure to populate the File output table.

files = AutoVivifyingDict()

# Data structure to populate the DataEvent output table.

dataEvents = AutoVivifyingDict()

# Data structure containing data to be stored in DCC-specific extension tables.

auxData = AutoVivifyingDict()

# Association tables.

assayedBy = AutoVivifyingDict()
observedBy = AutoVivifyingDict()
analyzedBy = AutoVivifyingDict()
producedBy = AutoVivifyingDict()
SubjectTaxonomy = AutoVivifyingDict()
SubjectsInSubjectGroups = AutoVivifyingDict()
FilesInDatasets = AutoVivifyingDict()
DatasetsInDatasets = AutoVivifyingDict()

# Data structure to track Datasets (includes init for top-level set).

datasets = AutoVivifyingDict()

topDatasetID = getNewID('DATASET.')

datasets[topDatasetID] = {
   
   'data_source' : 'COMMON_FUND_PROGRAM_ID.0',
   'title' : 'GTEx v7 data',
   'description' : 'All data from the v7 release of the GTEx project'
}

datasets = AutoVivifyingDict(datasets)

# Recover Dataset IDs by keyword.

datasetIDs = {
   
   'top' : topDatasetID,
   'sequence_files' : getNewID('DATASET.'),
   'alignment_files' : getNewID('DATASET.'),
   'rnaseq_alignment_files' : getNewID('DATASET.'),
   'wgs_alignment_files' : getNewID('DATASET.')
}

datasetIDs = AutoVivifyingDict(datasetIDs)

datasets[datasetIDs['sequence_files']] = {
   
   'data_source' : 'COMMON_FUND_PROGRAM_ID.0',
   'title' : 'GTEx v7 raw sequence files',
   'description' : 'GTEx v7 raw sequence files'
}

datasets[datasetIDs['alignment_files']] = {
   
   'data_source' : 'COMMON_FUND_PROGRAM_ID.0',
   'title' : 'GTEx v7 alignment files',
   'description' : 'GTEx v7 sequence alignment result files'
}

datasets[datasetIDs['rnaseq_alignment_files']] = {
   
   'data_source' : 'COMMON_FUND_PROGRAM_ID.0',
   'title' : 'GTEx v7 RNA-Seq alignment files',
   'description' : 'GTEx v7 RNA-Seq sequence alignment result files'
}

datasets[datasetIDs['wgs_alignment_files']] = {
   
   'data_source' : 'COMMON_FUND_PROGRAM_ID.0',
   'title' : 'GTEx v7 WGS alignment files',
   'description' : 'GTEx v7 WGS sequence alignment result files'
}

# Encode Dataset hierarchy.

for setKeyword in ( 'sequence_files', 'alignment_files' ):
   
   DatasetsInDatasets[datasetIDs[setKeyword]] = { datasetIDs['top'] : 1 }

for setKeyword in ( 'rnaseq_alignment_files', 'wgs_alignment_files' ):
   
   DatasetsInDatasets[datasetIDs[setKeyword]] = { datasetIDs['alignment_files'] : 1 }

# Data structure to track SubjectGroups (includes init for top-level group).

subjectGroups = AutoVivifyingDict()

topSubjectGroupID = getNewID('SUBJECT_GROUP.')

subjectGroups[topSubjectGroupID] = {
   
   'title' : 'GTEx v7 subjects',
   'description' : 'All subjects from the v7 release of the GTEx project'
}

##########################################################################################
# EXECUTION
##########################################################################################

if not os.path.isdir(outDir) and os.path.exists(outDir):
   
   die('%s exists but is not a directory; aborting.' % outDir)

elif not os.path.isdir(outDir):
   
   os.mkdir(outDir)

# Load mapping data from native GTEx values to third-party ontologies.

loadEnumMaps()

# Load metadata describing Subjects.

consumeSubjectData(subjectFile)

# Load metadata describing Samples.

consumeSampleData(sampleFile)

# Load metadata describing file locations for RNA-Seq and WGS alignment CRAMs.

consumeLocationData('RNA-Seq.reference_alignments')
consumeLocationData('WGS.reference_alignments')

# TEMPORARY: Write stub table objects for CommonFundProgram, Platform, Protocol
#            and SubjectGranularity.

writeDummyTables(tempTables)

# Collate and construct the final output collection of Table-Schema-constrained
# TSVs.

writeDataTables()

# Construct reference tables describing all terms from external CVs/ontologies
# used in this dataset.

writeOntologyReferenceTables()

# Include the Table-Schema JSON document in the output for reference.

os.system('cp ' + tableSchemaLoc + ' ' + outDir)

# Make a BDBag for final delivery and rename it to remove local indexing info.

bagDir = re.sub(r'preBag_output_files', r'bdbag', re.sub(r'^\d+_', r'', outDir))

os.system('mv ' + outDir + ' ' + bagDir);

os.system('bdbag --quiet --archiver tgz ' + bagDir);

# Revert the intermediate output directory from BDBag format to avoid
# chaos and despair when running this script multiple times without
# clearing outputs.

os.system('bdbag --quiet --revert ' + bagDir);

os.system('mv ' + bagDir + ' ' + outDir);


