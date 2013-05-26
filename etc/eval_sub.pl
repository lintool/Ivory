sub eval1{
	open(COSINE,$_[0]);
	
	$goldcnt=0;
	%hash = ();
	while(<COSINE>){

		if($_ =~ /\((\d+), (\d+)\)\t(.+)\n/){
			$hash{"$1,$2"}=1;
			
			$goldcnt++;
		}
	}
	close(COSINE);
	
	if($goldcnt==0){
		die $_[0]." no gold read.";
	}

	
#	   while ( my ($key, $value) = each(%hash) ) {
#	        print "$key => $value\n";
#	   }
	    
	open(PWSIM,$_[1]);
	
	$pwsimcnt=0, $pwsimcorrect=0, $pwsimwrong=0, $prevdocno=-1, $correct=0, $wrong=0;
	while(<PWSIM>){
#		print $_;
		if($_ =~ /\((\d+), (\d+)\)\t.+/){
			if($1 ne $prevdocno){
			        if($prevdocno!=-1){	
				#	print "$1\t".$correct."\t".$wrong."\t".($correct/($correct+$wrong))."\n";
				}
				$correct=0;
				$wrong=0;
				$prevdocno=$1;
				
			}
				
			$pwsimcnt++;
			$s = "$1,$2";
#			print "$s\n";
#			print "$hash{$s}\n";
			if($hash{$s}>0){
				#	print "HIT $2,$3\n";
				$pwsimcorrect++;$correct++;
			}else{
				# print "MISS $1\t$2\t$3\n";
				$pwsimwrong++;$wrong++;
			}
		}
	}
	close(PWSIM);
	
	if($pwsimcnt==0){
		return;#nrint $_[1]." no pwsim read.";
	}
	$recall = $pwsimcorrect/$goldcnt;
	$precision = $pwsimcorrect/$pwsimcnt;

	print STDERR "$_[1]\n#Golden pairs: $goldcnt\n#Pwsim pairs: $pwsimcnt (#Correct=$pwsimcorrect, #Wrong=$pwsimwrong)\nPrecision: $precision\nRecall: $recall\n";
	return ($precision, $recall);
}

sub eval2{
	open(COSINE,$_[0]);
	
	$goldcnt=0;
	%hash = ();
	while(<COSINE>){

		if($_ =~ /(\d+)\t(\d+)/){
			$hash{$2}=$1;
			
			$goldcnt++;
	#		print "$2$1\n";
		}
	}
	close(COSINE);
	
	if($goldcnt==0){
		die $_[0]." no gold read.";
	}
	
#	   while ( my ($key, $value) = each(%hash) ) {
#	        print "$key => $value\n";
#	   }
	    
	open(PWSIM,$_[1]);
	
	$pwsimcnt=0, $pwsimcorrect=0, $pwsimwrong=0, $prevdocno=-1, $correct=0, $wrong=0;
	while(<PWSIM>){
#		print $_;
                if($_ =~ /\((\d+), (\d+)\)\t.+/){
			if($1 ne $prevdocno){
				if($prevdocno!=-1){	
				#	print "$1\t".$correct."\t".$wrong."\t".($correct/($correct+$wrong))."\n";
				}
				$correct=0;
				$wrong=0;
				$prevdocno=$1;
				
			}
				
			$pwsimcnt++;
			$s = "$1,$2";
#			print "$s\n";
#			print "$hash{$s}\n";
			if(defined $hash{$2}){
				if($hash{$2}==$1){
					$pwsimcorrect++;$correct++;
	#				print "HIT\t$1\t$2\n";
				}else{
	#				print "MISS\t$1\t$2\n";
					$pwsimwrong++;$wrong++;
				}
			}else{
#				print "NO INTERWIKI LINK\t$1\t$2\n";
			}
		}
	}
	close(PWSIM);
	
	if($pwsimcnt==0){
		die $_[1]." no pwsim read.";
	}
	$recall = $pwsimcorrect/$goldcnt;
	$precision = $pwsimcorrect/($pwsimcorrect+$pwsimwrong);

	print STDERR "$_[1]\n#Golden pairs: $goldcnt\n#Pwsim pairs: $pwsimcnt (#Correct=$pwsimcorrect, #Wrong=$pwsimwrong, No gold truth for rest)\nPrecision: $precision\nRecall: $recall\nF-score: ".(2*$precision*$recall/($precision+$recall))."\n";
	return ($precision, $recall);
}
sub eval3{
	open(A,$_[0]);

        %hash = ();
        while(<A>){

                if($_ =~ /(\d+)\t\((.+), (\d+)\)/){
                        $hash{"$1,$3"}=1;
                }
        }
        close(COSINE);

        open(PWSIM,$_[1]);
        while(<PWSIM>){
                if($_ =~ /(\d+)\t\((.+), (\d+)\)/){
                        $s = "$3,$1";
                        if($hash{$s}>0){
                                #print "HIT $2,$3\n";
                                print $_;
                        }else{
                                #print "MISS $1\t$2\t$3\n";
                        }
                }
        }
        close(PWSIM);

}
1;
