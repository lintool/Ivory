require 'eval_sub.pl';

### example runs
# ($precision, $recall)=&eval("ground-cosine20-top25-sample","pwsim-dist436-top25-sample");
# ($precision, $recall)=&eval("en-wiki-de-links","ground-cosine20-top1-sample");
# ($precision, $recall)=&eval("en-wiki-de-links","ground-cosine20-top2-sample");
# ($precision, $recall)=&eval("en-wiki-de-links","ground-cosine20-top25-sample");
# ($precision, $recall)=&eval("ground-cosine30-sample","pwsim-dist400-sample");
# ($precision, $recall)=&eval("ground-cosine30-sample","pwsim-dist800-sample");
# ($precision, $recall)=&eval("ground-cosine30-sample","pwsim-dist1200-sample");

my $numargs = scalar @ARGV;
if($numargs==2){	
	($prec,$recall) = &eval1($ARGV[0],$ARGV[1]);
}elsif($numargs==3){	#need to change code in eval2 if language links are de-en
	($prec,$recall) = &eval2($ARGV[0],$ARGV[1]);
}else{
	die "usage: eval.pl [ground] [pwsim] (optional: [is-language-links-format])"
}
print "Precision = $prec\nRecall = $recall\n";
