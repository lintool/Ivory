%observed=();
while(<>){
if($_ =~ /<DOC>/){
  $ignore = 0;
}else{
 if($_ =~ /<DOCNO>(.+)<\/DOCNO>/){
    $docno = $1;
    $docno =~ s/^\s+//g;
    $docno =~ s/\s+$//g;
    if($observed{$docno}==1){
    	$ignore = 1;
    }else{
	print "<DOC>\n$_";
	$observed{$docno}=1;
    }
 }else{
    if($ignore == 0){
      print;
    }
 }

}

}
