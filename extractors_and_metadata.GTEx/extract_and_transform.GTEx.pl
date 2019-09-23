#!/usr/bin/env perl

use strict;

$| = 1;

##########################################################################################
# AUTHOR INFO
##########################################################################################

# Arthur Brady (Univ. of MD Inst. for Genome Sciences) wrote this script to extract
# GTEx (v7) experimental data and transform it to conform to the draft C2M2 data
# specification prior to ingestion into a central CFDE database.

# Creation date: 2019-08-28
# Lastmod date unless I forgot to change it: 2019-09-05

# contact email: abrady@som.umaryland.edu

##########################################################################################
# PARAMETERS
##########################################################################################

# Location of Table-Schema JSON document describing output. To be included in output
# collection for reference on ingest.

my $tableSchemaLoc = '000_tableschema/GTEx_C2M2_instance.json';

##########################################################################################
# Directory containing unmodified GTEx-published metadata files

my $inDir = '001_GTEx.v7.raw_input_files';

# Metadata describing samples and subjects

my $sampleFile = "$inDir/GTEx_v7_Annotations_SampleAttributesDS.txt";
my $subjectFile = "$inDir/GTEx_v7_Annotations_SubjectPhenotypesDS.txt";

# URLs locating CRAM file data assets (plus basic file metadata)

my $locationFiles = {
   
   'RNA-Seq.reference_alignments' => "$inDir/rnaseq_cram_files_v7_datacommons_011516.txt",
   'WGS.reference_alignments' => "$inDir/wgs_cram_files_v7_hg38_datacommons_011516.txt"
};

##########################################################################################
# Base URLs (to be followed by term IDs) used to reconstruct full URLs
# referencing controlled-vocabulary terms in third-party ontologies

my $baseURL = {
   
   'EDAM' => 'http://edamontology.org/',
   'OBI' => 'http://purl.obolibrary.org/obo/',
   'Uberon' => 'http://purl.obolibrary.org/obo/UBERON_',
   'NCBI_Taxonomy_DB' => 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id='
};

##########################################################################################
# Track which external CV terms are used in the output; after main data tables are built,
# use this data to build CV lookup tables for terms used (primary key (ID): URL;
# minimal data fields: name, definition, synonyms; all but ID, name nullable).

my $termsUsed = {};

my $fullURL = {};

##########################################################################################
# Subdirectory containing full info for CVs, versioned to match the current data release.

my $cvRefDir = '003_CV_reference_data';

##########################################################################################
# Map of CV names to reference files. NCBI taxonomy should be added here; right now
# we're just making a stub reference for that CV due to the presence of just one
# referenced species (human).

my $cvFile = {
   
   'EDAM' => "$cvRefDir/EDAM.version_1.21.tsv",
   'OBI' => "$cvRefDir/OBI.version_2019-08-15.obo",
   'Uberon' => "$cvRefDir/uberon.version_2019-06-27.obo"
};

##########################################################################################
# Map of internal table names to external ontology reference IDs.

my $ontoMap = {
   
   'SampleType' => 'OBI',
   'Anatomy' => 'Uberon',
   'InformationType' => 'EDAM',
   'FileFormat' => 'EDAM',
   'Method' => 'OBI'
};

##########################################################################################
# TEMPORARY: Loadable objects representing single-row CV table stubs that are
#            specific to CFDE (Organization, Protocol, SubjectGranularity). Once
#            these objects have been built into a target DB instance, code
#            producing these tables will be replaced with code linking relevant
#            fields to existing CV table records.

my $tempTables = {
   
   'Organization' =>       {
                              'id' => 'ORGANIZATION_ID.0',
                              'url' => '',
                              'name' => 'GTEx',
                              'description' => 'The Genotype-Tissue Expression (GTEx) project'
                           },
   'Platform' =>           {
                              'id' => 'PLATFORM_ID.0',
                              'url' => '',
                              'name' => 'Dummy_platform',
                              'description' => 'Description of the dummy platform'
                           },
   'Protocol' =>           {
                              'id' => 'PROTOCOL_ID.0',
                              'url' => '',
                              'name' => 'Dummy_protocol',
                                 'description' => 'Description of the dummy protocol'
                           },
   'SubjectGranularity' => {
                              'id' => 'SUBJECT_GRANULARITY_ID.0',
                              'url' => '',
                              'name' => 'single_organism',
                              'description' => 'A subject representing a single organism'
                           }
};

my $uniqueNumericIndex = 0;

##########################################################################################
# Directory containing TSVs mapping named GTEx fields to terms in
# third-party ontologies

my $mapDir = '002_GTEx.v7.maps_from_GTEx_enums_to_controlled_vocabs';

# Map-file locations, keyed by the name of the output-object property field
# meant to store references to the relevant ontology

my $mapFiles = {
   
   'BioSample.sample_type' => "$mapDir/GTEx_v7_Annotations_SampleAttributesDS.txt.SMAFRZE_to_OBI_for_BioSample.sample_type.tsv",
   'File.information_type' => "$mapDir/File_information_type_keyword_to_EDAM_for_File.information_type.tsv",
   'File.file_format' => "$mapDir/File_format_keyword_to_EDAM_for_File.file_format.tsv"
};

##########################################################################################
# Output directory

my $outDir = '004_GTEx.v7.C2M2_preload.preBag_output_files';

##########################################################################################
# Global variables

# Functions mapping value-constrained internal GTEx metadata field values
# to third-party CV term IDs for populating selected data fields in the
# output model.

my $enumMap = {};

# Data structure to populate the BioSample output table.

my $samples = {};

# Data structure to populate the Subject output table.

my $subjects = {};

# Data structure to populate the File output table.

my $files = {};

# Data structure to populate the DataEvent output table.

my $dataEvents = {};

# Data structure containing data to be stored in DCC-specific extension tables.

my $auxData = {};

# Association tables.

my $producedBy = {};
my $assayedBy = {};
my $observedBy = {};
my $analyzedBy = {};
my $generatedBy = {};
my $sponsoredBy = {};
my $SubjectTaxonomy = {};
my $SubjectsInSubjectGroups = {};
my $FilesInDatasets = {};
my $DatasetsInDatasets = {};

# Data structure to track Datasets (includes init for top-level set).

my $datasets = {};

my $topDatasetID = &getNewID('DATASET.');

$datasets->{$topDatasetID}->{'title'} = 'GTEx v7 data';
$datasets->{$topDatasetID}->{'description'} = 'All data from the v7 release of the GTEx project';

# Recover Dataset IDs by keyword.

my $datasetIDs = {
   
   'top' => $topDatasetID,
   'sequence_files' => &getNewID('DATASET.'),
   'alignment_files' => &getNewID('DATASET.'),
   'rnaseq_alignment_files' => &getNewID('DATASET.'),
   'wgs_alignment_files' => &getNewID('DATASET.')
};

$datasets->{$datasetIDs->{'sequence_files'}}->{'title'} = 'GTEx v7 raw sequence files';
$datasets->{$datasetIDs->{'sequence_files'}}->{'description'} = 'GTEx v7 raw sequence files';

$datasets->{$datasetIDs->{'alignment_files'}}->{'title'} = 'GTEx v7 alignment files';
$datasets->{$datasetIDs->{'alignment_files'}}->{'description'} = 'GTEx v7 sequence alignment result files';

$datasets->{$datasetIDs->{'rnaseq_alignment_files'}}->{'title'} = 'GTEx v7 RNA-Seq alignment files';
$datasets->{$datasetIDs->{'rnaseq_alignment_files'}}->{'description'} = 'GTEx v7 RNA-Seq sequence alignment result files';

$datasets->{$datasetIDs->{'wgs_alignment_files'}}->{'title'} = 'GTEx v7 WGS alignment files';
$datasets->{$datasetIDs->{'wgs_alignment_files'}}->{'description'} = 'GTEx v7 WGS sequence alignment result files';

# Encode Dataset hierarchy.

$DatasetsInDatasets->{$datasetIDs->{'sequence_files'}}->{$datasetIDs->{'top'}} = 1;
$DatasetsInDatasets->{$datasetIDs->{'alignment_files'}}->{$datasetIDs->{'top'}} = 1;
$DatasetsInDatasets->{$datasetIDs->{'rnaseq_alignment_files'}}->{$datasetIDs->{'alignment_files'}} = 1;
$DatasetsInDatasets->{$datasetIDs->{'wgs_alignment_files'}}->{$datasetIDs->{'alignment_files'}} = 1;

# TEMPORARY: All creator attributions for Datasets are hard-coded
# to the (local) ID for the 'GTEx' organization.

foreach my $setKeyword ( 'sequence_files', 'alignment_files', 'rnaseq_alignment_files', 'wgs_alignment_files' ) {
   
   $generatedBy->{$datasetIDs->{$setKeyword}}->{'ORGANIZATION_ID.0'} = 1;
}

# Data structure to track SubjectGroups (includes init for top-level group).

my $subjectGroups = {};

my $topSubjectGroupID = &getNewID('SUBJECT_GROUP.');

$subjectGroups->{$topSubjectGroupID}->{'title'} = 'GTEx v7 subjects';
$subjectGroups->{$topSubjectGroupID}->{'description'} = 'All subjects from the v7 release of the GTEx project';

##########################################################################################
# EXECUTION
##########################################################################################

system("mkdir -p $outDir") if ( not -d $outDir );

# Load mapping data from native GTEx values to third-party ontologies.

&loadEnumMaps();

# Load metadata describing Subjects.

&consumeSubjectData($subjectFile);

# Load metadata describing Samples.

&consumeSampleData($sampleFile);

# Load metadata describing file locations for RNA-Seq and WGS alignment CRAMs.

&consumeLocationData('RNA-Seq.reference_alignments');
&consumeLocationData('WGS.reference_alignments');

# TEMPORARY: Write stub table objects for Organization, Protocol and
#            SubjectGranularity.

&writeDummyTables($tempTables);

# Collate and construct the final output collection of Table-Schema-constrained
# TSVs.

&writeDataTables();

# Construct reference tables describing all terms from external CVs/ontologies
# used in this dataset.

&writeOntologyReferenceTables();

# Include the Table-Schema JSON document in the output for reference.

system("cp $tableSchemaLoc $outDir");

# Make a BDBag for final delivery and rename it to remove local indexing info.

my $bagDir = $outDir;

$bagDir =~ s/^\d+_//;

$bagDir =~ s/preBag_output_files/bdbag/;

system("mv $outDir $bagDir");

system("bdbag --quiet --archiver tgz $bagDir");

# Revert the intermediate output directory from BDBag format to avoid
# chaos and despair when running this script multiple times without
# clearing outputs.

system("bdbag --quiet --revert $bagDir");

system("mv $bagDir $outDir");

##########################################################################################
# SUBROUTINES
##########################################################################################

# Load functions mapping value-constrained internal GTEx metadata field values
# to third-party CV term IDs. Map filenames encode source file & field, target
# ontology name, and the name of the target data structure in the output model.

sub loadEnumMaps {
   
   foreach my $targetField ( sort keys %$mapFiles ) {
      
      my $mapFile = $mapFiles->{$targetField};

      open IN, "<$mapFile" or die("Can't open $mapFile for reading.\n");

      my $header = <IN>;

      while ( chomp( my $line = <IN> ) ) {
         
         my ( $dccFieldVal, $dccFieldDesc, $cvTermID, $cvTermName ) = split(/\t/, $line);

         $enumMap->{$targetField}->{$dccFieldVal} = $cvTermID;
      }

      close IN;
   }
}

sub consumeSubjectData {
   
   my $inFile = shift;

   open IN, "<$inFile" or die("Can't open $inFile for reading.\n");

   chomp( my $headerLine = <IN> );

   my @colNames = split(/\t/, $headerLine);

   while ( chomp( my $line = <IN> ) ) {
      
      my ( $subjectID, $sexCode, $ageRange, $hardyScaleCode ) = split(/\t/, $line);

      # This subject is a single organism (human) -- this is constant for this dataset.

      $subjects->{$subjectID}->{'granularity'} = 'SUBJECT_GRANULARITY_ID.0';

      # Cache all DCC-specific extension data about this Subject. Go ahead
      # and create records for empty values to avoid seriously screwing up
      # downstream existence expectations about associations between data
      # structures.

      $auxData->{'Subject'}->{$subjectID}->{'GTExSubjectSex'} = $sexCode;
      $auxData->{'Subject'}->{$subjectID}->{'GTExSubjectAgeRange'} = $ageRange;
      $auxData->{'Subject'}->{$subjectID}->{'GTExSubjectHardyScaleCode'} = $hardyScaleCode;

      # Link up subjects with the NCBI taxonomy code for "human."

      $SubjectTaxonomy->{$subjectID}->{$baseURL->{'NCBI_Taxonomy_DB'} . '9606'} = 1;

      # The next 2 lines aren't happening until support for autoloading used
      # NCBI taxonomy terms is built. Right now, there's just one species,
      # so this isn't enabled.
      # 
      # $termsUsed->{'NCBI_Taxonomy_DB'}->{'9606'} = 1;
      # $fullURL->{'NCBI_Taxonomy_DB'}->{'9606'} = $baseURL->{'NCBI_Taxonomy_DB'} . '9606';

      # TEMPORARY: Assignment to top-level subject group is hard-coded here.
      # Refinements will require group decisions on how to parse the GTEx
      # subject population.

      $SubjectsInSubjectGroups->{$subjectID}->{$topSubjectGroupID} = 1;
   }

   close IN;
}

sub consumeSampleData {
   
   my $inFile = shift;

   open IN, "<$inFile" or die("Can't open $inFile for reading.\n");

   chomp( my $headerLine = <IN> );

   my @colNames = split(/\t/, $headerLine);

   while ( chomp( my $line = <IN> ) ) {
      
      my (
         
         $SAMPID, $SMATSSCR, $SMCENTER, $SMPTHNTS, $SMRIN,
         $SMTS, $SMTSD, $SMUBRID,
         $SMTSISCH, $SMTSPAX,
         $SMNABTCH, $SMNABTCHT, $SMNABTCHD,
         $SMGEBTCH, $SMGEBTCHD, $SMGEBTCHT,
         $SMAFRZE,

#       Ignoring these for the current iteration. They're all DCC-specific
#       sample metadata according to the current draft model.
#
#        $SMGTC, $SME2MPRT, $SMCHMPRS, $SMNTRART, $SMNUMGPS, $SMMAPRT,
#        $SMEXNCRT, $SM550NRM, $SMGNSDTC, $SMUNMPRT, $SM350NRM, $SMRDLGTH,
#        $SMMNCPB, $SME1MMRT, $SMSFLGTH, $SMESTLBS, $SMMPPD, $SMNTERRT,
#        $SMRRNANM, $SMRDTTL, $SMVQCFL, $SMMNCV, $SMTRSCPT, $SMMPPDPR,
#        $SMCGLGTH, $SMGAPPCT, $SMUNPDRD, $SMNTRNRT, $SMMPUNRT, $SMEXPEFF,
#        $SMMPPDUN, $SME2MMRT, $SME2ANTI, $SMALTALG, $SME2SNSE, $SMMFLGTH,
#        $SME1ANTI, $SMSPLTRD, $SMBSMMRT, $SME1SNSE, $SME1PCTS, $SMRRNART,
#        $SME1MPRT, $SMNUM5CD, $SMDPMPRT, $SME2PCTS

         @theRest
 
      ) = split(/\t/, $line);

      # Default type for the following is 'string' unless otherwise indicated
      # in a comment.

      my $sampleID = $SAMPID;
      my $autolysisScore = $SMATSSCR;     # int stored as decimal
      my $collectionSiteCode = $SMCENTER; # enum: short strings
      my $pathologyNotes = $SMPTHNTS;
      my $rnaIntegrityNumber = $SMRIN;    # decimal

      my $tissueType = $SMTS;             # enum: general tissue-type keywords
      my $specificTissueType = $SMTSD;    # enum: specific tissue-type keywords
      my $uberonID = $SMUBRID;            # Note: This encodes SMTSD, not SMTS.

      my $ischemicTime = $SMTSISCH;       # int (minutes) stored as decimal
      my $timeSpentInPaxGeneFixative = $SMTSPAX;      # int (min.) as decimal

      my $nucleicAcidIsolationBatchID = $SMNABTCH; 
      my $nucleicAcidIsolationBatchProcess = $SMNABTCHT;
      my $nucleicAcidIsolationBatchDate = $SMNABTCHD;  # date: MM/DD/YYYY

      my $genotypeOrExpressionBatchID = $SMGEBTCH;
      my $genotypeOrExpressionBatchDate = $SMGEBTCHD;  # date: MM/DD/YYYY
      my $genotypeOrExpressionBatchProcess = $SMGEBTCHT;

      my $analysisType = $SMAFRZE; # enum: { RNASEQ, WGS, WES, OMNI, EXCLUDE }

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
#      my $sampleOK = 1;
#
#      # Exclude this sample record?
#
#      if ( $analysisType eq 'EXCLUDE' ) {
#         
#         $sampleOK = 0;
#      }

      # Incoming data conformant to expectations?

      my $subjectID = 'SOMETHING_WENT_WRONG';

      # Subject IDs should be encoded in sample IDs: the latter are segmented
      # by dashes, and the subject ID should be the first two dash-separated
      # fields of the sample ID.

      if ( $sampleID =~ /^([^-]+)-([^-]+)-/ ) {
         
         $subjectID = "$1-$2";

         if ( not exists( $subjects->{$subjectID} ) ) {
            
            die("FATAL: Parsed subject ID for sample \"$sampleID\" to "
              . "\"$subjectID\", but there is no record of that subject ID "
              . "in the input fileset.\n");
         }

      } else {
         
         die("FATAL: Can't parse subject ID from sample ID \"$sampleID\"; "
           . "aborting.\n");
      }

      # Despite GTEx's description of the 'SMUBRID' field as "Uberon ID", this
      # column occasionally contains EFO IDs instead (stored as integers
      # prefixed with 'EFO_' instead of the unprefixed integers used to
      # reference actual Uberon IDs), apparently for anatomical
      # structures not represented in Uberon.
      # 
      # No attempt is being made at present to resolve these schema-breaking
      # IDs to best-match Uberon IDs; instead, any non-Uberon IDs will simply
      # be replaced with the Uberon ID for 'anatomical part'.

      if ( $uberonID !~ /^\d+$/ ) {
         
         $uberonID = '0001062';
      }

#      if ( $sampleOK ) {
         
         # Resolve mapped enum values.

         if ( not exists( $enumMap->{'BioSample.sample_type'}->{$analysisType} ) ) {
            
            die("FATAL: Can't map analysis type \"$analysisType\" to OBI term; "
              . "aborting.\n\nPlease update "
              . "$mapFiles->{'BioSample.sample_type'} as needed.\n");
         }

         $analysisType = $enumMap->{'BioSample.sample_type'}->{$analysisType};

         my $savedAnalysisType = $analysisType;

         $savedAnalysisType =~ s/OBI_/OBI:/;

         $termsUsed->{'SampleType'}->{$savedAnalysisType} = 1;
         $fullURL->{'SampleType'}->{$savedAnalysisType} = $baseURL->{'OBI'} . $analysisType;

         # Build and cache the sample data structure. Bottom-level hash keys,
         # here, should exactly match output-model property names for the
         # BioSample object.

         $samples->{$sampleID}->{'subject'} = $subjectID;

         $samples->{$sampleID}->{'sample_type'} = $baseURL->{'OBI'} . $analysisType;

         $samples->{$sampleID}->{'anatomy'} = $baseURL->{'Uberon'} . $uberonID;

         $termsUsed->{'Anatomy'}->{"UBERON:$uberonID"} = 1;
         $fullURL->{'Anatomy'}->{"UBERON:$uberonID"} = $baseURL->{'Uberon'} . $uberonID;

         # TEMPORARY: "BioSample.protocol" is currently hard-coded to a constant
         # ID value because we currently have no pre-built model set for
         # protocols; the field will reference a stub record, meant to be
         # replaced later on with references to specific protocol models.

         $samples->{$sampleID}->{'protocol'} = 'PROTOCOL_ID.0';

         # TEMPORARY: (See previous comment.)

         $samples->{$sampleID}->{'rank'} = 0;

         # This BioSample represents a batch of isolated molecules, not an
         # unprocessed tissue sample. Encode the 'creation date' for this
         # BioSample using the appropriate batch-isolation date (DNA or RNA).

         my $creationDate = $nucleicAcidIsolationBatchDate;

         if ( $analysisType eq 'OBI_0000880' ) {
            
            # RNA extract. Switch the BioSample creation date to the
            # corresponding isolation event.

            $creationDate = $genotypeOrExpressionBatchDate;
         }

         if ( $creationDate eq '' or $creationDate =~ /not\s+reported/i ) {
            
            $creationDate = '';

         } elsif ( $creationDate !~ /^\d+\/\d+\/\d+$/ ) {
            
            die("FATAL: Malformed date value \"$creationDate\" for sample "
              . "\"$sampleID\"; aborting.\n");

         } else {
            
            my ( $month, $day, $year ) = split(/\//, $creationDate);

            $creationDate = sprintf("%d\-%02d\-%02d", $year, $month, $day);
         }

         $samples->{$sampleID}->{'event_ts'} = $creationDate;

         # Cache all DCC-specific extension data about this BioSample. Go ahead
         # and create records for empty values to avoid seriously screwing up
         # downstream expectations about associations between data structures.

         $auxData->{'BioSample'}->{$sampleID}->{'GTExBioSampleAutolysisScore'} = $autolysisScore;
         $auxData->{'BioSample'}->{$sampleID}->{'GTExBioSampleCollectionSiteCode'} = $collectionSiteCode;
         $auxData->{'BioSample'}->{$sampleID}->{'GTExBioSamplePathologyNotes'} = $pathologyNotes;
         $auxData->{'BioSample'}->{$sampleID}->{'GTExBioSampleRnaIntegrityNumber'} = $rnaIntegrityNumber;
         $auxData->{'BioSample'}->{$sampleID}->{'GTExBioSampleIschemicTime'} = $ischemicTime;
         $auxData->{'BioSample'}->{$sampleID}->{'GTExBioSamplePaxGeneFixativeTime'} = $timeSpentInPaxGeneFixative;

#      } # end if ( $sampleOK )

   } # end ( line iterator on input sample-data file )

   close IN;
}

sub consumeLocationData {
   
   my $dataType = shift;

   my $inFile = $locationFiles->{$dataType};

   open IN, "<$inFile" or die("Can't open $inFile for reading.\n");

   # RNA-Seq.reference_alignments: sample_id cram_file_gcp cram_index_gcp cram_file_aws cram_index_aws cram_file_md5 cram_file_size cram_index_md5
   # WGS.reference_alignments:     sample_id cram_file_gcp cram_index_gcp cram_file_aws cram_index_aws cram_file_md5 cram_file_size cram_index_md5

   my $header = <IN>;

   while ( chomp( my $line = <IN> ) ) {
      
      my ( $sampleID, $cramFileGCP, $cramIndexGCP, $cramFileAWS, $cramIndexAWS, $cramFileMD5, $cramFileSize, $cramIndexMD5 ) = split(/\t/, $line);

      my $cramBaseName = $cramFileGCP;

      $cramBaseName =~ s/^.*\/([^\/]+)$/$1/;

      # Build an object to track this File along with a DataEvent object to reference its creation.

      # BEGIN TEMPORARY BLOCK: Right now I don't have raw-sequence file location
      # data. The data I do have is for read alignments to the human
      # reference genome; I'm going to have to create dummy File objects
      # (and associated DataEvents) to represent sequence files in order to
      # connect associated Sample objects to these alignment File objects.

      if ( not exists( $samples->{$sampleID} ) ) {
         
         die("FATAL: Loaded file referencing unknown sample ID \"$sampleID\"; aborting.\n");
      }

      # Create a File object for the raw sequence data.

      my $seqFileID = &getNewID('FILE_ID.');

      $files->{$seqFileID}->{'url'} = "gs://UNKNOWN_LOCATION.RAW_SEQUENCES.$seqFileID";
      $files->{$seqFileID}->{'information_type'} = $baseURL->{'EDAM'} . $enumMap->{'File.information_type'}->{'Generic_sequence'};
      $files->{$seqFileID}->{'file_format'} = $baseURL->{'EDAM'} . $enumMap->{'File.file_format'}->{'FASTQ'};
      $files->{$seqFileID}->{'length'} = 1;
      $files->{$seqFileID}->{'filename'} = "RAW_SEQUENCES.$seqFileID";
      $files->{$seqFileID}->{'md5'} = '.';

      $termsUsed->{'InformationType'}->{$baseURL->{'EDAM'} . $enumMap->{'File.information_type'}->{'Generic_sequence'}} = 1;
      $fullURL->{'InformationType'}->{$baseURL->{'EDAM'} . $enumMap->{'File.information_type'}->{'Generic_sequence'}} = $baseURL->{'EDAM'} . $enumMap->{'File.information_type'}->{'Generic_sequence'};
      $termsUsed->{'FileFormat'}->{$baseURL->{'EDAM'} . $enumMap->{'File.file_format'}->{'FASTQ'}} = 1;
      $fullURL->{'FileFormat'}->{$baseURL->{'EDAM'} . $enumMap->{'File.file_format'}->{'FASTQ'}} = $baseURL->{'EDAM'} . $enumMap->{'File.file_format'}->{'FASTQ'};

      # Create a File record for the alignment results.

      my $alnFileID = &getNewID('FILE_ID.');

      $files->{$alnFileID}->{'url'} = $cramFileGCP;
      $files->{$alnFileID}->{'information_type'} = $baseURL->{'EDAM'} . $enumMap->{'File.information_type'}->{$dataType};
      $files->{$alnFileID}->{'file_format'} = $baseURL->{'EDAM'} . $enumMap->{'File.file_format'}->{'CRAM'};
      $files->{$alnFileID}->{'length'} = $cramFileSize;
      $files->{$alnFileID}->{'filename'} = $cramBaseName;
      $files->{$alnFileID}->{'md5'} = $cramFileMD5;

      $termsUsed->{'InformationType'}->{$baseURL->{'EDAM'} . $enumMap->{'File.information_type'}->{$dataType}} = 1;
      $fullURL->{'InformationType'}->{$baseURL->{'EDAM'} . $enumMap->{'File.information_type'}->{$dataType}} = $baseURL->{'EDAM'} . $enumMap->{'File.information_type'}->{$dataType};
      $termsUsed->{'FileFormat'}->{$baseURL->{'EDAM'} . $enumMap->{'File.file_format'}->{'CRAM'}} = 1;
      $fullURL->{'FileFormat'}->{$baseURL->{'EDAM'} . $enumMap->{'File.file_format'}->{'CRAM'}} = $baseURL->{'EDAM'} . $enumMap->{'File.file_format'}->{'CRAM'};

      # Sequencing event.

      my $dataEventID = &getNewID('DATA_EVENT.');

      $dataEvents->{$dataEventID}->{'protocol'} = 'PROTOCOL_ID.0';
                                                # Generic sequencing event.
      $dataEvents->{$dataEventID}->{'method'} = $baseURL->{'OBI'} . 'OBI_0600047';
      $termsUsed->{'Method'}->{'OBI:0600047'} = 1;
      $fullURL->{'Method'}->{'OBI:0600047'} = $baseURL->{'OBI'} . 'OBI_0600047';

      $dataEvents->{$dataEventID}->{'rank'} = 0;
                                                  # Copy for now from parent sample object.
      $dataEvents->{$dataEventID}->{'event_ts'} = $samples->{$sampleID}->{'event_ts'};

      $assayedBy->{$sampleID}->{$dataEventID} = 1;
      $producedBy->{$seqFileID}->{$dataEventID} = 1;

      # Alignment event.

      $dataEventID = &getNewID('DATA_EVENT.');

      $dataEvents->{$dataEventID}->{'protocol'} = 'PROTOCOL_ID.0';
                                                # Generic alignment event.
      $dataEvents->{$dataEventID}->{'method'} = $baseURL->{'OBI'} . 'OBI_0002567';
      $termsUsed->{'Method'}->{'OBI:0002567'} = 1;
      $fullURL->{'Method'}->{'OBI:0002567'} = $baseURL->{'OBI'} . 'OBI_0002567';

      $dataEvents->{$dataEventID}->{'rank'} = 1;
                                                  # Copy for now from parent sample object.
      $dataEvents->{$dataEventID}->{'event_ts'} = $samples->{$sampleID}->{'event_ts'};

      $analyzedBy->{$seqFileID}->{$dataEventID} = 1;
      $producedBy->{$alnFileID}->{$dataEventID} = 1;

      # END TEMPORARY BLOCK

      # TEMPORARY: Assignments to predefined Dataset groups are hard-coded
      # here. Refinements will require group decisions on how to parse the
      # GTEx file (sub)collections.

      $FilesInDatasets->{$seqFileID}->{$datasetIDs->{'sequence_files'}} = 1;

      if ( $dataType eq 'RNA-Seq.reference_alignments' ) {
         
         $FilesInDatasets->{$alnFileID}->{$datasetIDs->{'rnaseq_alignment_files'}} = 1;

      } elsif ( $dataType eq 'WGS.reference_alignments' ) {
         
         $FilesInDatasets->{$alnFileID}->{$datasetIDs->{'wgs_alignment_files'}} = 1;
      }

   } # end ( line iterator on file-location input file )

   close IN;
}

sub writeDummyTables {
   
   my $stubHash = shift;

   foreach my $tableName ( sort keys %$stubHash ) {
      
      my $tableFile = "$outDir/$tableName.tsv";

      open OUT, ">$tableFile" or die("Can't open $tableFile for writing.\n");

      print OUT join("\t", ( 'id', 'url', 'name', 'description' )) . "\n";

      print OUT join("\t", ( $stubHash->{$tableName}->{'id'}, $stubHash->{$tableName}->{'url'}, $stubHash->{$tableName}->{'name'}, $stubHash->{$tableName}->{'description'} )) . "\n";

      close OUT;
   }
}

sub writeDataTables {
   
   # Core data tables.

   &writeTable('BioSample', $samples, 'subject', 'sample_type', 'anatomy', 'protocol', 'rank', 'event_ts');
   &writeTable('Subject', $subjects, 'granularity');
   &writeTable('File', $files, 'url', 'information_type', 'file_format', 'length', 'filename', 'md5');
   &writeTable('DataEvent', $dataEvents, 'method', 'protocol', 'rank', 'event_ts');
   &writeTable('Dataset', $datasets, 'title', 'description');
   &writeTable('SubjectGroup', $subjectGroups, 'title', 'description');

   # Association tables.

   &writeAssociationTable('SubjectTaxonomy', $SubjectTaxonomy, 'SubjectID', 'NCBITaxonID');
   &writeAssociationTable('ProducedBy', $producedBy, 'FileID', 'DataEventID');
   &writeAssociationTable('AssayedBy', $assayedBy, 'BioSampleID', 'DataEventID');
   &writeAssociationTable('ObservedBy', $observedBy, 'SubjectID', 'DataEventID');
   &writeAssociationTable('AnalyzedBy', $analyzedBy, 'FileID', 'DataEventID');
   &writeAssociationTable('GeneratedBy', $generatedBy, 'DatasetID', 'OrganizationID');
   &writeAssociationTable('SponsoredBy', $sponsoredBy, 'DatasetID', 'OrganizationID');
   &writeAssociationTable('SubjectsInSubjectGroups', $SubjectsInSubjectGroups, 'SubjectID', 'SubjectGroupID');
   &writeAssociationTable('FilesInDatasets', $FilesInDatasets, 'FileID', 'DatasetID');
   &writeAssociationTable('DatasetsInDatasets', $DatasetsInDatasets, 'ContainedDatasetID', 'ContainingDatasetID');

   # Auxiliary data for Subjects.

   my $outFile = "$outDir/AuxiliaryData.tsv";

   open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

   print OUT "ObjectType\tObjectID\tDataDescription\tValue\n";

   foreach my $objectType ( sort { $a cmp $b } keys %$auxData ) {
      
      foreach my $objectID ( sort { $a cmp $b } keys %{$auxData->{$objectType}} ) {
         
         foreach my $dataDesc ( sort { $a cmp $b } keys %{$auxData->{$objectType}->{$objectID}} ) {
            
            my $value = $auxData->{$objectType}->{$objectID}->{$dataDesc};
               
            print OUT join("\t", $objectType, $objectID, $dataDesc, $value) . "\n";
         }
      }
   }

   close OUT;
}

sub writeTable {
   
   my $fileBase = shift;

   my $dataHash = shift;

   my @fieldNames = ();

   while ( my $fieldName = shift ) {
      
      push @fieldNames, $fieldName;
   }

   my $outFile = "$outDir/$fileBase.tsv";

   open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

   print OUT join("\t", ( 'id', @fieldNames ) ) . "\n";

   foreach my $id ( sort { $a cmp $b } keys %$dataHash ) {
      
      print OUT "$id";

      foreach my $fieldName ( @fieldNames ) {
         
         print OUT "\t";

         if ( exists( $dataHash->{$id}->{$fieldName} ) ) {
            
            print OUT $dataHash->{$id}->{$fieldName};
         }
      }

      print OUT "\n";
   }

   close OUT;
}

sub writeAssociationTable {
   
   # &writeAssociationTable('SubjectTaxonomy', $SubjectTaxonomy, 'SUBJECT_ID', 'TAXON_ID');

   my $fileBase = shift;

   my $dataHash = shift;

   my @colNames = ();

   while ( my $colName = shift ) {
      
      push @colNames, $colName;
   }

   my $outFile = "$outDir/$fileBase.tsv";

   open OUT, ">$outFile" or die("Can't open outFile for writing.\n");

   print OUT join("\t", @colNames) . "\n";

   # TEMPORARY? Assuming exactly two columns (ID -> VALUE or ID -> ID), here.

   foreach my $id ( sort { $a cmp $b } keys %$dataHash ) {
      
      foreach my $val ( sort { $a cmp $b } keys %{$dataHash->{$id}} ) {
         
         print OUT "$id\t$val\n";
      }
   }

   close OUT;
}

sub getNewID {
   
   my $prefix = shift;

   die("FATAL: getNewID() called with no ID prefix; aborting.\n") if ( $prefix eq '' );

   my $newID = sprintf("$prefix%08d", $uniqueNumericIndex);

   $uniqueNumericIndex++;

   return $newID;
}

sub writeOntologyReferenceTables {
   
#   my $ontoMap = {
#      
#      'SampleType' => 'OBI',
#      'Anatomy' => 'Uberon',
#      'InformationType' => 'EDAM',
#      'FileFormat' => 'EDAM',
#      'Method' => 'OBI'
#   };

   foreach my $vocabTable ( keys %$termsUsed ) {
      
      my $cv = $ontoMap->{$vocabTable};

      my $refFile = $cvFile->{$cv};

      open IN, "<$refFile" or die("Can't open $refFile for reading.\n");

      my $ontoData = {};

      if ( $cv eq 'OBI' or $cv eq 'Uberon' ) {
         
         # We have OBO files for these.

         my $recording = 0;

         my $currentTerm = '';

         while ( chomp( my $line = <IN> ) ) {
            
            if ( $line =~ /^id:\s+(\S.*)$/ ) {
               
               $currentTerm = $1;

               if ( exists( $termsUsed->{$vocabTable}->{$currentTerm} ) ) {
                  
                  $recording = 1;

                  if ( not exists( $fullURL->{$vocabTable}->{$currentTerm} ) ) {
                     
                     die("FATAL: No URL cached for used CV ($cv) term "
                       . "\"$currentTerm\"; cannot proceed, aborting.\n");
                  }

               } else {
                  
                  $currentTerm = '';

                  # (Recording is already switched off by default.)
               }

            } elsif ( $line =~ /^\[Term\]/ ) {
               
               $recording = 0;

            } elsif ( $recording ) {
               
               if ( $line =~ /^name:\s+(\S*.*)$/ ) {
                  
                  $ontoData->{$fullURL->{$vocabTable}->{$currentTerm}}->{'name'} = $1;

               } elsif ( $line =~ /^def:\s+\"(.*)\"[^\"]*$/ ) {
                  
                  $ontoData->{$fullURL->{$vocabTable}->{$currentTerm}}->{'description'} = $1;

               } elsif ( $line =~ /^def:\s+/ ) {
                  
                  die("FATAL: Unparsed def-line in $cv OBO file: "
                  . "\"$line\"; aborting.\n");

               } elsif ( $line =~ /^synonym:\s+\"(.*)\"[^\"]*$/ ) {
                  
                  my $synonym = $1;

                  if ( exists( $ontoData->{$fullURL->{$vocabTable}->{$currentTerm}}->{'synonyms'} ) ) {
                     
                     $ontoData->{$fullURL->{$vocabTable}->{$currentTerm}}->{'synonyms'} .= "\|$synonym";

                  } else {
                     
                     $ontoData->{$fullURL->{$vocabTable}->{$currentTerm}}->{'synonyms'} = $synonym;
                  }
               }

            } # end if ( line-type selector switch )

         } # end while ( input file line iterator )

      } elsif ( $cv eq 'EDAM' ) {
         
         # We have a CV-specific TSV for this one.

         my $header = <IN>;

         while ( chomp( my $line = <IN> ) ) {
            
            my ( $id, $name, $synonyms, $def, @theRest ) = split(/\t/, $line);

            if ( exists( $termsUsed->{$vocabTable}->{$id} ) ) {
               
               # There are some truly screwy things allowed inside
               # tab-separated fields in this file. Clean them up.

               $name = &trimSpace($name);
               $name = &trimQuotes($name);
               $name = &trimSpace($name);

               $synonyms = &trimSpace($synonyms);
               $synonyms = &trimQuotes($synonyms);
               $synonyms = &trimSpace($synonyms);

               $def = &trimSpace($def);
               $def = &trimQuotes($def);
               $def = &trimSpace($def);

               $ontoData->{$id}->{'name'} = $name;
               $ontoData->{$id}->{'synonyms'} = $synonyms;
               $ontoData->{$id}->{'description'} = $def;
            }
         }
      }

      close IN;

      my $outFile = "$outDir/$vocabTable.tsv";

      open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

      print OUT "id\turl\tname\tdescription\tsynonyms\n";

      foreach my $id ( sort { $a cmp $b } keys %$ontoData ) {
         
         print OUT join("\t",
                              $id,
                              $id,
                              $ontoData->{$id}->{'name'},
                              $ontoData->{$id}->{'description'},
                              $ontoData->{$id}->{'synonyms'} ) . "\n";
      }

      close OUT;

   } # end foreach ( $vocabTable )

   # TEMPORARY: Hack pending enabling of dynamic lookup for NCBI taxonomy IDs.

   my $outFile = "$outDir/NCBI_Taxonomy_DB.tsv";

   open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

   print OUT "id\turl\tname\tdescription\tsynonyms\n";

   print OUT "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606\thttps://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606\tHomo sapiens\tHomo sapiens (modern human species)\t\n";

   close OUT;
}

sub trimSpace {
   
   my $string = shift;

   my $result = $string;

   $result =~ s/^\s+//;

   $result =~ s/\s+$//;

   return $result;
}

sub trimQuotes {
   
   my $string = shift;

   my $result = $string;

   $result =~ s/^\"+//;

   $result =~ s/\"+$//;

   return $result;
}
