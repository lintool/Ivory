#!/usr/bin/perl

$file = shift or die "$0 [mapping]\n";
open(FILE, "< $file");
while ( <FILE> ) {
    chomp($_);
    @arr = split(/\s+/, $_);
    $M{$arr[0]} = $arr[1];
    #print "$arr[0]#$arr[1]\n";
}
close(FILE);

while ( <> ) {
    chomp($_);
    @arr = split(/\s+/, $_);
    $docno = $M{$arr[2]};
    print "$arr[0]\t$arr[1]\t$docno\t$arr[3]\n";
}
