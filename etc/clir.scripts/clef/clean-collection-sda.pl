my %map = ();
my $ignore = 0;
while(<>){
        if(($_ =~ /^<(.+)>.*<\/(.+)>$/ || $_ =~ /^<(.+)>/) && $_ !~ /^<\/.+>$/){
		if($1 ne "DOCNO" && $1 ne "TX"	&& $1 ne "DOC"){
		#	print "ignoring $1\n";
			$ignore = 1;
		}
	}
	if($ignore == 0){
		s/<TX>//g;
		s/<\/TX>//g;
		print;
	}
	if($_ =~ /<\/(.+)>/){
		#if($ignore != 1){
		#	die "problem";
		#}
		$ignore = 0;	
	}
}
