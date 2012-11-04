while(<>){
if($_ =~ /<DOC>/){
  $ignore = 0;
}else{
 if($_ =~ /<DOCNO>(.+)<\/DOCNO>/){
    $docno = $1;
    $docno =~ s/^\s+//g;
    $docno =~ s/\s+$//g;
    if($docno =~ /XIN_CMN_(\d{4}).*/){
         if($1 < 2002){
            $ignore = 1;
         }else{
	    print "<DOC>\n$_";
         }
    }
 }else{
    if($ignore == 0){
      print;
    }
 }

}

}
