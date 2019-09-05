# Prototype extract/transform script from GTEx v7 metadata to draft C2M2 core metadata standard

## Contents

This directory contains the prototype GTEx extractor script along with
- raw input data from GTEx
- a Table-Schema JSON file describing the output
- some auxiliary files mapping GTEx terminology to terms in selected controlled vocabularies
- a gzipped tarball containing example output
- an ER diagram (based on but different from Karl's working draft) precisely describing output structure
  - ...except for DCC-specific auxiliary data, which isn't drawn: this is encoded as a flat table ("AuxiliaryData.tsv") of 4-tuples: "ObjectType", "ObjectID", "DataDescription" and "Value", so that each record generically links some bit of metadata to some existing object [like a BioSample or a Subject] within the C2M2 model).

## Dependencies

- A working version of Perl5
- 'bdbag' needs to be accessible via $PATH

## Design notes

