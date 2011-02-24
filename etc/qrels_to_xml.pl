#!/usr/bin/perl

use strict;
use warnings;

print "<parameters>\n";

my $line;
while( defined( $line = <STDIN> ) ) {
  my ( $qid, $zero, $docname, $judgment ) = split( /\s+/, $line );
  print "<judgment qid=\"" . $qid . "\" doc=\"" . $docname . "\" grade=\"" . $judgment . "\"/>\n";
}

print "</parameters>\n";
