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

package ivory.tokenize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.TagTokenizer;
import org.tartarus.snowball.ext.englishStemmer;

public class GalagoTokenizer implements Tokenizer {

	englishStemmer mStemmer = new englishStemmer();
	HashMap<String, String> mCache = new HashMap<String, String>();

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

	static private HashSet<String> stopwords = new HashSet<String>();

	static {
		for (int i = 0; i < TERRIER_STOP_WORDS.length; i++) {
			stopwords.add(TERRIER_STOP_WORDS[i]);
		}
	}

	public boolean isStopWord(String word) {
		return stopwords.contains(word);
	}

	public String[] processContent(String text) {
		TagTokenizer tokenizer = new TagTokenizer();
		Document doc = null;

		try {
			doc = tokenizer.tokenize(text);
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<String> toks = doc.terms;
		List<String> words = new ArrayList<String>();

		for (String tok : toks) {
			if (!stopwords.contains(tok)) {
				words.add(tok);
			}
		}

		for (int i = 0; i < words.size(); i++) {
			String word = words.get(i);

			if (word != null) {
				if (mCache.containsKey(word)) {
					words.set(i, mCache.get(word));
				} else {
					mStemmer.setCurrent(word);
					if (mStemmer.stem()) {
						String stem = mStemmer.getCurrent();
						words.set(i, stem);
						mCache.put(word, stem);
					} else {
						mCache.put(word, word);
					}
				}

				if (mCache.size() > 50000) {
					mCache.clear();
				}
			}
		}

		String[] arr = new String[words.size()];
		return (String[]) words.toArray(arr);
	}

	public void configure(Configuration conf) {
	}

	public static void main(String[] args) {
		String text = " this is a the <test> for the teokenizer 101 546 345-543543545436-4656765865865 rgger <xml> ergtre 456435klj345lj34590";

		Tokenizer tokenizer;
		String[] tokens;

		System.out.println("tokenization according to Galago: ");
		tokenizer = new GalagoTokenizer();
		tokens = tokenizer.processContent(text);
		for (String t : tokens)
			System.out.println(t);

		System.out.println("\n\ntokenization according to Lucene: ");
		tokenizer = new LuceneTokenizer();
		tokens = tokenizer.processContent(text);
		for (String t : tokens)
			System.out.println(t);
	}
}
