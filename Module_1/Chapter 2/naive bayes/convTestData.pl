#!/usr/bin/perl

use strict;
use warnings;
use Switch ;

my $testFile = "./DigitalBreathTestData2013-MALE2.csv" ;
my $outFile  = "./DigitalBreathTestData2013-MALE2a.csv" ;
my $fileLine ; 
my @dataCols ;
my ($col1, $col2, $col3, $col4, $col5, $col6) ; 

$col1=0, $col2=0, $col3=0, $col4=0, $col5=0, $col6=0 ; 

open IFILE,"<",  $testFile or die "File  $testFile not found";
open OFILE,">",  $outFile  or die "File  $outFile  not found";

print "\nStart Processing : $testFile\n" ;

while (<IFILE>)
{
  $fileLine = $_ ;
  @dataCols = split(',', $fileLine);

  # process data to enumerated types

  switch( $dataCols[0] ) 
  {
    case "Male"       { $col1 = 0 }
    case "Female"     { $col1 = 1 }
    else              { $col1 = 2 }
  }
  
  switch( $dataCols[1] ) 
  {
    case "Moving Traffic Violation"      { $col2 = 0 }
    case "Other"                         { $col2 = 1 }
    case "Road Traffic Collision"        { $col2 = 2 }
    case "Suspicion of Alcohol"          { $col2 = 3 }
    else                                 { $col2 = 99 }
  }

  switch( $dataCols[2] ) 
  {
    case "Weekday"                       { $col3 = 0 }
    case "Weekend"                       { $col3 = 1 }
    else                                 { $col3 = 99 }
  }

  switch( $dataCols[3] ) 
  {
    case "12am-4am"                      { $col4 = 0 }
    case "4am-8am"                       { $col4 = 1 }
    case "8am-12pm"                      { $col4 = 2 }
    case "12pm-4pm"                      { $col4 = 3 }
    case "4pm-8pm"                       { $col4 = 4 }
    case "8pm-12pm"                      { $col4 = 5 }
    else                                 { $col4 = 99 }
  }

  $col5 =  $dataCols[4] ;

  $dataCols[5] =~ s/\n//g ; 
  $dataCols[5] =~ s/\r//g ; 

  switch( $dataCols[5] ) 
  {
    case "16-19"                         { $col6 = 0 }
    case "20-24"                         { $col6 = 1 }
    case "25-29"                         { $col6 = 2 }
    case "30-39"                         { $col6 = 3 }
    case "40-49"                         { $col6 = 4 }
    case "50-59"                         { $col6 = 5 }
    case "60-69"                         { $col6 = 6 }
    case "70-98"                         { $col6 = 7 }
    case "Other"                         { $col6 = 8 }
    else                                 { $col6 = 99 }
  }

  
  # print data to out file 

  print OFILE "$col1,$col2 $col3 $col4 $col5 $col6\n" ;

} # file processing

print "Processing Finished \n\n" ;

close(IFILE);
close(OFILE);

