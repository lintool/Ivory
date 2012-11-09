my $ignore = 0;
# these are tags we care about
%tags = ("DOCNO" => 1, "TEXT" => 1,"DOC" => 1, "TITLE" => 1);
while(<>){
	if(($_ =~ /^<(.+)>.*<\/(.+)>$/ || $_ =~ /^<(.+)>/) && $_ !~ /^<\/.+>$/){
		if(not defined $tags{$1}){
		#	print "ignoring tag $1\n";
			$ignore = 1;
		}
	}
	if($ignore == 0){
		s/<TEXT>//g;   # slightly different tags?
		s/<\/TEXT>//g;
		s/<TITLE>//g;
		s/<\/TITLE>//g;
		s/&gt;//g;
		s/&lt;//g;
		print;
	}
	if($_ =~ /<\/(.+)>/){
		$ignore = 0;	
	}
}
