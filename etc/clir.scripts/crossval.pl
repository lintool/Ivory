$start=$ARGV[0];	# first topic no
$last=$ARGV[1];		# last topic no
$fold=$ARGV[2];		# no of folds
$logfiles=$ARGV[3];		# log directory with output of grid search

system "rm tmp$fold.ap";

$num=$last-$start+1;
$inc=($num/$fold);
for($topic=$start;$topic<$last;$topic=$topic+$inc){
	$topic2=$topic+$inc;
	print "$topic,$topic2,$inc\n";
	system "perl filter-analyze.pl 10 $logfiles $topic $topic2 | sort -n -k2 -r | awk '{print \$1}' | head -1 > filt$fold.tmp";

	open(F,"filt$fold.tmp");
	while(<F>){
		chomp;
		$setting=$_;
	}
	close(F);

	print "$setting\n";

	system "grep '<AP>' $logfiles | grep $setting | awk -F\":::\" '{if(\$3>=$topic && \$3<$topic2){print \$3,\$6;}}' >> tmp$fold.ap";
}

system "sort -k1 -n tmp$fold.ap | awk '{print \$2}' > crossval$fold.ap";
