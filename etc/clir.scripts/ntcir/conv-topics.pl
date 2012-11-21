while(<>)
{
if($_ =~ /<(.+) LANG="EN"></){
	$s=$1;
	s/\s*<$s LANG="EN"><!\[CDATA\[/<$s>/g;
	s/\s*\]\]><\/$s>/<\/$s>/gi;
	s/QUESTION/title/g;
	s/NARRATIVE/desc/g;
	print;
}
}
