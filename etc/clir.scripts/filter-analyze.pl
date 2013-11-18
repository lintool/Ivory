## print out MAP values for various settings, filtered by a range of topic values

$n=$ARGV[0];
$logfiles=$ARGV[1];
$t1=$ARGV[2];
$t2=$ARGV[3];
$num=$t2-$t1;
#$file= ($#ARGV + 1)==6 ? $ARGV[4] : "";

for($l1=0;$l1<=100;$l1=$l1+10){	
	for($l2=0;$l1+$l2<=100;$l2=$l2+10){
		$scfg=(100-$l1-$l2)/100.0;
#		if($file eq ""){
			system "grep '$n-$l1-$l2-' $logfiles | grep '<AP>' | awk -F\":::\" '{if(\$3>=$t1 && \$3<$t2){}else{sum=sum+\$6;set=\$2;}}END{print set\" \"sum/$num}'";
#		}else{
#			system "grep '$n-$l1-$l2-' $logfiles | grep '<AP>' | awk -F\":::\" '{if(\$3>=$t1 && \$3<$t2){}else{sum=sum+\$6;set=\$2;}}END{print set\" \"sum/$num}' >> $name.scfg=$scfg.dat";
#		}
	}
}
