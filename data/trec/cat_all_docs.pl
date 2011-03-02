#!/usr/bin/perl

$file = shift or die "usage: $0 [list-of-files]\n";

open(FILE, "< $file");
while ( <FILE> ) {
    print `cat $_`;
}
close(FILE);
