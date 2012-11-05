my $ignore = 0;
# these are tags we care about
%tags = ("DOCNO" => 1,"TEXT" => 1,"DOC" => 1, "TITLE" => 1, "P" => 1, "HEADLINE" => 1, "BODY" => 1);
while(<>){ 
	s/&HT;//g;
	if(($_ =~ /^<(.+)>.*<\/(.+)>$/ || $_ =~ /^<(.+)>/) && $_ !~ /^<\/.+>$/){
		if(not defined $tags{$1}){
		#	print "ignoring tag $1\n";
			$ignore = 1;
		}
	}

	if($ignore == 0){
		#	s/<TX>/<TEXT>/g;   # slightly different tags?
		#	s/<\/TX>/<\/TEXT>/g;
		print;
	}
	
	if($_ =~ /<\/(.+)>/){
                $ignore = 0;
	}
}
