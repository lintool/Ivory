my $ignore = 0;
# we care about content in between these tags 
%tags2read = ("DOCNO" => 1,"TEXT" => 1,"DOC" => 1, "TITLE" => 1, "P" => 1, "HEADLINE" => 1, "BODY" => 1);
# we would like to keep only the following tags
%tags2keep = ("DOCNO" => 1,"DOC" => 1);

while(<>){ 
        s/&HT;//g;
	if(($_ =~ /^<(.+)>.*<\/(.+)>$/ || $_ =~ /^<(.+)>/) && $_ !~ /^<\/.+>$/){
		if(not defined $tags2read{$1}){
		#	print "ignoring tag $1\n";
			$ignore = 1;
		}
	}

	if($ignore == 0){
		print;
	}
	
	if($_ =~ /<\/(.+)>/){
                $ignore = 0;
	}
}
