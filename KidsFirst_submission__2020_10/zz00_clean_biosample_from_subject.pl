#!/usr/bin/perl

use strict;

$| = 1;

my $wd = '001_static_stubs.some_newly_created_with_abhijna';

my $biosample = "$wd/biosample.tsv";

my $inFile = "$wd/biosample_from_subject.too_many_biosamples.tsv";

my $outFile = "$wd/biosample_from_subject.tsv";

open IN, "<$biosample" or die("Can't open $biosample for reading.\n");

my $header = <IN>;

my $seen = {};

while ( chomp( my $line = <IN> ) ) {
   
   my ( $ns, $id, @theRest ) = split(/\t/, $line);

   $seen->{$id} = 1;
}

close IN;

open IN, "<$inFile" or die("Can't open $inFile for reading.\n");

open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

my $header = <IN>;

print OUT $header;

while ( my $line = <IN> ) {
   
   my ( $ns, $id, @theRest ) = split(/\t/, $line);

   if ( $seen->{$id} ) {
      
      print OUT $line;
   }
}

close OUT;

close IN;


