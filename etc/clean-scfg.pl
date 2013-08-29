$numfiles = scalar @ARGV;

for(my $i=0; $i<$numfiles; $i++){
	open(OUT,">x");
	open(IN,$ARGV[$i]);
	while(<IN>){
		s/\[X\] \|\|\|\s//g;
		print OUT $_;
	}
	close(IN);
	close(OUT);
	system "mv x ".$ARGV[$i];
}
