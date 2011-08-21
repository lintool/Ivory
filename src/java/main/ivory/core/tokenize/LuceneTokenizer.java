/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.core.tokenize;

import java.io.Reader;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class LuceneTokenizer implements Tokenizer {

	private static boolean replaceInvalidAcronym = false;

	private static int maxTokenLength = 255;

	@SuppressWarnings("unused")
	private static final String[] LUCENE_STOP_WORDS = { "a", "an", "and", "are", "as", "at", "be",
			"but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or",
			"such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was",
			"will", "with" };

	private static final String[] TERRIER_STOP_WORDS = { "x", "y", "your", "yours", "yourself",
			"yourselves", "you", "yond", "yonder", "yon", "ye", "yet", "z", "zillion", "j", "u",
			"umpteen", "usually", "us", "username", "uponed", "upons", "uponing", "upon", "ups",
			"upping", "upped", "up", "unto", "until", "unless", "unlike", "unliker", "unlikest",
			"under", "underneath", "use", "used", "usedest", "r", "rath", "rather", "rathest",
			"rathe", "re", "relate", "related", "relatively", "regarding", "really", "res",
			"respecting", "respectively", "q", "quite", "que", "qua", "n", "neither", "neaths",
			"neath", "nethe", "nethermost", "necessary", "necessariest", "necessarier", "never",
			"nevertheless", "nigh", "nighest", "nigher", "nine", "noone", "nobody", "nobodies",
			"nowhere", "nowheres", "no", "noes", "nor", "nos", "no-one", "none", "not",
			"notwithstanding", "nothings", "nothing", "nathless", "natheless", "t", "ten", "tills",
			"till", "tilled", "tilling", "to", "towards", "toward", "towardest", "towarder",
			"together", "too", "thy", "thyself", "thus", "than", "that", "those", "thou", "though",
			"thous", "thouses", "thoroughest", "thorougher", "thorough", "thoroughly", "thru",
			"thruer", "thruest", "thro", "through", "throughout", "throughest", "througher",
			"thine", "this", "thises", "they", "thee", "the", "then", "thence", "thenest",
			"thener", "them", "themselves", "these", "therer", "there", "thereby", "therest",
			"thereafter", "therein", "thereupon", "therefore", "their", "theirs", "thing",
			"things", "three", "two", "o", "oh", "owt", "owning", "owned", "own", "owns", "others",
			"other", "otherwise", "otherwisest", "otherwiser", "of", "often", "oftener",
			"oftenest", "off", "offs", "offest", "one", "ought", "oughts", "our", "ours",
			"ourselves", "ourself", "out", "outest", "outed", "outwith", "outs", "outside", "over",
			"overallest", "overaller", "overalls", "overall", "overs", "or", "orer", "orest", "on",
			"oneself", "onest", "ons", "onto", "a", "atween", "at", "athwart", "atop", "afore",
			"afterward", "afterwards", "after", "afterest", "afterer", "ain", "an", "any",
			"anything", "anybody", "anyone", "anyhow", "anywhere", "anent", "anear", "and",
			"andor", "another", "around", "ares", "are", "aest", "aer", "against", "again",
			"accordingly", "abaft", "abafter", "abaftest", "abovest", "above", "abover", "abouter",
			"aboutest", "about", "aid", "amidst", "amid", "among", "amongst", "apartest",
			"aparter", "apart", "appeared", "appears", "appear", "appearing", "appropriating",
			"appropriate", "appropriatest", "appropriates", "appropriater", "appropriated",
			"already", "always", "also", "along", "alongside", "although", "almost", "all",
			"allest", "aller", "allyou", "alls", "albeit", "awfully", "as", "aside", "asides",
			"aslant", "ases", "astrider", "astride", "astridest", "astraddlest", "astraddler",
			"astraddle", "availablest", "availabler", "available", "aughts", "aught", "vs", "v",
			"variousest", "variouser", "various", "via", "vis-a-vis", "vis-a-viser",
			"vis-a-visest", "viz", "very", "veriest", "verier", "versus", "k", "g", "go", "gone",
			"good", "got", "gotta", "gotten", "get", "gets", "getting", "b", "by", "byandby",
			"by-and-by", "bist", "both", "but", "buts", "be", "beyond", "because", "became",
			"becomes", "become", "becoming", "becomings", "becominger", "becomingest", "behind",
			"behinds", "before", "beforehand", "beforehandest", "beforehander", "bettered",
			"betters", "better", "bettering", "betwixt", "between", "beneath", "been", "below",
			"besides", "beside", "m", "my", "myself", "mucher", "muchest", "much", "must", "musts",
			"musths", "musth", "main", "make", "mayest", "many", "mauger", "maugre", "me",
			"meanwhiles", "meanwhile", "mostly", "most", "moreover", "more", "might", "mights",
			"midst", "midsts", "h", "huh", "humph", "he", "hers", "herself", "her", "hereby",
			"herein", "hereafters", "hereafter", "hereupon", "hence", "hadst", "had", "having",
			"haves", "have", "has", "hast", "hardly", "hae", "hath", "him", "himself", "hither",
			"hitherest", "hitherer", "his", "how-do-you-do", "however", "how", "howbeit",
			"howdoyoudo", "hoos", "hoo", "w", "woulded", "woulding", "would", "woulds", "was",
			"wast", "we", "wert", "were", "with", "withal", "without", "within", "why", "what",
			"whatever", "whateverer", "whateverest", "whatsoeverer", "whatsoeverest", "whatsoever",
			"whence", "whencesoever", "whenever", "whensoever", "when", "whenas", "whether",
			"wheen", "whereto", "whereupon", "wherever", "whereon", "whereof", "where", "whereby",
			"wherewithal", "wherewith", "whereinto", "wherein", "whereafter", "whereas",
			"wheresoever", "wherefrom", "which", "whichever", "whichsoever", "whilst", "while",
			"whiles", "whithersoever", "whither", "whoever", "whosoever", "whoso", "whose",
			"whomever", "s", "syne", "syn", "shalling", "shall", "shalled", "shalls", "shoulding",
			"should", "shoulded", "shoulds", "she", "sayyid", "sayid", "said", "saider", "saidest",
			"same", "samest", "sames", "samer", "saved", "sans", "sanses", "sanserifs", "sanserif",
			"so", "soer", "soest", "sobeit", "someone", "somebody", "somehow", "some", "somewhere",
			"somewhat", "something", "sometimest", "sometimes", "sometimer", "sometime", "several",
			"severaler", "severalest", "serious", "seriousest", "seriouser", "senza", "send",
			"sent", "seem", "seems", "seemed", "seemingest", "seeminger", "seemings", "seven",
			"summat", "sups", "sup", "supping", "supped", "such", "since", "sine", "sines", "sith",
			"six", "stop", "stopped", "p", "plaintiff", "plenty", "plenties", "please", "pleased",
			"pleases", "per", "perhaps", "particulars", "particularly", "particular",
			"particularest", "particularer", "pro", "providing", "provides", "provided", "provide",
			"probably", "l", "layabout", "layabouts", "latter", "latterest", "latterer",
			"latterly", "latters", "lots", "lotting", "lotted", "lot", "lest", "less", "ie", "ifs",
			"if", "i", "info", "information", "itself", "its", "it", "is", "idem", "idemer",
			"idemest", "immediate", "immediately", "immediatest", "immediater", "in", "inwards",
			"inwardest", "inwarder", "inward", "inasmuch", "into", "instead", "insofar",
			"indicates", "indicated", "indicate", "indicating", "indeed", "inc", "f", "fact",
			"facts", "fs", "figupon", "figupons", "figuponing", "figuponed", "few", "fewer",
			"fewest", "frae", "from", "failing", "failings", "five", "furthers", "furtherer",
			"furthered", "furtherest", "further", "furthering", "furthermore", "fourscore",
			"followthrough", "for", "forwhy", "fornenst", "formerly", "former", "formerer",
			"formerest", "formers", "forbye", "forby", "fore", "forever", "forer", "fores", "four",
			"d", "ddays", "dday", "do", "doing", "doings", "doe", "does", "doth", "downwarder",
			"downwardest", "downward", "downwards", "downs", "done", "doner", "dones", "donest",
			"dos", "dost", "did", "differentest", "differenter", "different", "describing",
			"describe", "describes", "described", "despiting", "despites", "despited", "despite",
			"during", "c", "cum", "circa", "chez", "cer", "certain", "certainest", "certainer",
			"cest", "canst", "cannot", "cant", "cants", "canting", "cantest", "canted", "co",
			"could", "couldst", "comeon", "comeons", "come-ons", "come-on", "concerning",
			"concerninger", "concerningest", "consequently", "considering", "e", "eg", "eight",
			"either", "even", "evens", "evenser", "evensest", "evened", "evenest", "ever",
			"everyone", "everything", "everybody", "everywhere", "every", "ere", "each", "et",
			"etc", "elsewhere", "else", "ex", "excepted", "excepts", "except", "excepting", "exes",
			"enough" };

	public LuceneTokenizer() {

	}

	private TokenStream tokenStream(Reader reader) {
		StandardTokenizer tokenStream = new StandardTokenizer(reader, replaceInvalidAcronym);
		tokenStream.setMaxTokenLength(maxTokenLength);
		TokenStream result = new StandardFilter(tokenStream);

		result = new LowerCaseFilter(result);
		result = new StopFilter(result, TERRIER_STOP_WORDS);
		result = new PorterStemFilter(result);

		return result;
	}

	public String[] processContent(String text) {
		StringBuffer str = new StringBuffer();
		TokenStream stream = tokenStream(new StringReader(text));
		Token token = new Token();

		try {
			while ((token = stream.next(token)) != null) {
				str.append(token.termBuffer(), 0, token.termLength());
				str.append(" ");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// By default, the Lucene tokenizer doesn't tokenize on hyphens and
		// slashes. This does turn out to have an impact on effectiveness, at
		// least for the TREC genomics05 test collection. On
		// the Google CLuE cluster:
		// 
		// default Lucene tokenization: MAP=0.2294
		// default + break on hyphens: MAP=0.2462
		// default + break on hyphens/slashes: MAP=0.2478
		//
		// -- Jimmy, 2008/10/10

		return str.toString().replace('-', ' ').replace('/', ' ').trim().split("\\s+");
		// Yes, this is ugly... but surprisingly efficient. This implementation
		// is much faster than a more sensible alternative of temporarily
		// storing the terms in an ArrayList and then generating an array at the
		// end (which I tried). -- Jimmy, 2008/10/09
	}
	
	public void configure(Configuration conf) {
	}
}
