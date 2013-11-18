### Given two files, each containing one AP score per line, in the same order, this script will compute statistical significance based on randomized test from (Smucker, 2007)
### usage: stat_sig.pl file1 file2 number-of-repeats
### output is a number that denotes the p-value

open(R1,$ARGV[0]);
open(R2,$ARGV[1]);

@res1 = <R1>;
@res2 = <R2>;
$numtopics = scalar @res1;

$sysA = 0;
$sysB = 0;
for(my $top=0;$top<$numtopics;$top++){
        $sysA = $sysA + $res1[$top];
	$sysB = $sysB + $res2[$top];
}

$diff = abs($sysA-$sysB);

print "Actual diff = $diff\n";

for($i=0;$i<$ARGV[2];$i++){
	$sysA = 0;
	$sysB = 0;
	for(my $top=0;$top<$numtopics;$top++){
		$num = rand();
		if($num > 0.5){
		#	print "H";
			$sysA = $sysA + $res1[$top];	
	                $sysB = $sysB + $res2[$top];
		}else{
		#	print "T";
                        $sysA = $sysA + $res2[$top];
                        $sysB = $sysB + $res1[$top];
		}
	}
	if ((($sysA-$sysB) <= -$diff) || (($sysA-$sysB) >= $diff)){
		$cnt++;
	}

#	print ($sysA-$sysB);
#	print "\n";
}
print $cnt/$ARGV[2];
