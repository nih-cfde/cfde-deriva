#!/usr/bin/env python3

##########################################################################################
#                                     AUTHOR INFO
##########################################################################################

# Arthur Brady (Univ. of MD Inst. for Genome Sciences) wrote this script to extract
# HMP experimental data and transform it to conform to the draft C2M2 Level 1 data
# specification prior to ingestion into a central CFDE database.

# Creation date: 2020-05-17
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

def identifyTermsUsed(  ):
   
   global termsUsed, draftDir, fileToColumnToCategory

   for basename in fileToColumnToCategory:
      
      inFile = draftDir + '/' + basename

      with open( inFile, 'r' ) as IN:
         
         header = IN.readline()

         for line in IN:
            
            fields = re.split(r'\t', line.rstrip('\r\n'))

            for colIndex in fileToColumnToCategory[basename]:
               
               currentCategory = fileToColumnToCategory[basename][colIndex]

               if fields[colIndex] != '':
                  
                  termsUsed[currentCategory][fields[colIndex]] = {}

# end sub: identifyTermsUsed(  )

def decorateTermsUsed(  ):
   
   global termsUsed, cvFile

   for categoryID in termsUsed:
      
      if categoryID == 'anatomy' or categoryID == 'assay_type':
         
         cv = ''

         if categoryID == 'anatomy':
            
            cv = 'Uberon'

         elif categoryID == 'assay_type':
            
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

                  if currentTerm in termsUsed[categoryID]:
                     
                     recording = True

                     if 'synonyms' not in termsUsed[categoryID][currentTerm]:
                        
                        termsUsed[categoryID][currentTerm]['synonyms'] = ''

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

      elif categoryID == 'file_format' or categoryID == 'data_type':
         
         cv = 'EDAM'

         refFile = cvFile[cv]

         with open( refFile, 'r' ) as IN:
            
            header = IN.readline()

            for line in IN:
               
               line = line.rstrip('\r\n')

               ( termURL, name, synonyms, definition ) = re.split(r'\t', line)[0:4]

               currentTerm = re.sub(r'^.*\/([^\/]+)$', r'\1', termURL)

               currentTerm = re.sub(r'data_', r'data:', currentTerm)
               currentTerm = re.sub(r'format_', r'format:', currentTerm)

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

def writeTermsUsed(  ):
   
   global termTableDir, termsUsed

   for categoryID in termsUsed:
      
      outFile = '%s/%s.tsv' % ( termTableDir, categoryID )

      with open(outFile, 'w') as OUT:
         
         OUT.write( '\t'.join( [ 'id', 'name', 'description', 'synonyms' ] ) + '\n' )

         for termID in termsUsed[categoryID]:
            
#           OUT.write( '\t'.join( [ termID, termsUsed[categoryID][termID]['name'], termsUsed[categoryID][termID]['description'], termsUsed[categoryID][termID]['synonyms'] ] ) + '\n' )
            
            # The synonyms we loaded from the OBO files don't conform to the spec constraints. Punting to blank values for now.

            OUT.write( '\t'.join( [ termID, termsUsed[categoryID][termID]['name'], termsUsed[categoryID][termID]['description'],                  ''                       ] ) + '\n' )

# end sub writeTermsUsed(  )

##########################################################################################
##########################################################################################
##########################################################################################
#                                        PARAMETERS
##########################################################################################
##########################################################################################
##########################################################################################

##########################################################################################
# Subdirectory containing full info for CVs, versioned to match the current data release.

cvRefDir = '003_external_CVs_versioned_reference_files'

##########################################################################################
# Map of CV names to reference files.

cvFile = {
   
   'EDAM' : '%s/EDAM.version_1.21.tsv' % cvRefDir,
   'OBI' : '%s/OBI.version_2019-08-15.obo' % cvRefDir,
   'Uberon' : '%s/uberon.version_2019-06-27.obo' % cvRefDir
}

##########################################################################################
# Directory in which HMP-written ETL data TSVs will be produced prior to combination with
# (constant) stub tables CFDE-provided CV and JSON files for bdbagging.

draftDir = '006_HMP-specific_ETL_TSVs'

##########################################################################################
# Directory in which HMP-written TSVs will be produced to track all controlled-vocabulary
# terms used throughout this Level 1 C2M2 instance (as specified in the Level 1
# specification).

termTableDir = '007_HMP-specific_CV_term_usage_TSVs'

# Term-tracker data structure.

termsUsed = {
   
   'file_format': {},
   'data_type': {},
   'assay_type': {},
   'anatomy': {}
}

# Map indicating which columns in which files contain terms from which CV categories.

fileToColumnToCategory = {
   
   'file.tsv': {
      9: 'file_format',
      10: 'data_type'
   },
   'biosample.tsv': {
      5: 'assay_type',
      6: 'anatomy'
   }
}

##########################################################################################
##########################################################################################
##########################################################################################
#                                       EXECUTION
##########################################################################################
##########################################################################################
##########################################################################################

# Find all the CV terms used in the ETL draft instance in "draftDir".

identifyTermsUsed()

# Load data from CV reference files to fill out needed columns in Level 1 C2M2
# term-tracker tables.

decorateTermsUsed()

# Write the term-tracker tables.

writeTermsUsed()


