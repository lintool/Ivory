#!/usr/bin/perl

while ( <STDIN> ) {
    @arr = split(/\s+/, $_);
    print unless $arr[3] > 1000;
}
