package ivory.core.tokenize;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.tartarus.snowball.SnowballStemmer;

import com.google.common.collect.Sets;

import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;

public class OpenNLPTokenizer extends ivory.core.tokenize.Tokenizer {
  private static final Logger sLogger = Logger.getLogger(OpenNLPTokenizer.class);
  static{
    sLogger.setLevel(Level.WARN);
  }
  Tokenizer tokenizer;
  SnowballStemmer stemmer;
  private int lang;
  protected static int NUM_PREDS, MIN_LENGTH = 2, MAX_LENGTH = 50;
  String delims = "`~!@#$%^&*()-_=+]}[{\\|'\";:/?.>,<";
  private static final int ENGLISH=0, FRENCH=1, GERMAN=2;
  private static final String[] languages = {"english", "french", "german"};
  private static final String[] TERRIER_STOP_WORDS = {
    "a",
    "abaft",
    "abafter",
    "abaftest",
    "about",
    "abouter",
    "aboutest",
    "above",
    "abover",
    "abovest",
    "accordingly",
    "aer",
    "aest",
    "afore",
    "after",
    "afterer",
    "afterest",
    "afterward",
    "afterwards",
    "again",
    "against",
    "aid",
    "ain",
    "albeit",
    "all",
    "aller",
    "allest",
    "alls",
    "allyou",
    "almost",
    "along",
    "alongside",
    "already",
    "also",
    "although",
    "always",
    "amid",
    "amidst",
    "among",
    "amongst",
    "an",
    "and",
    "andor",
    "anear",
    "anent",
    "another",
    "any",
    "anybody",
    "anyhow",
    "anyone",
    "anything",
    "anywhere",
    "apart",
    "aparter",
    "apartest",
    "appear",
    "appeared",
    "appearing",
    "appears",
    "appropriate",
    "appropriated",
    "appropriater",
    "appropriates",
    "appropriatest",
    "appropriating",
    "are",
    "ares",
    "around",
    "as",
    "ases",
    "aside",
    "asides",
    "aslant",
    "astraddle",
    "astraddler",
    "astraddlest",
    "astride",
    "astrider",
    "astridest",
    "at",
    "athwart",
    "atop",
    "atween",
    "aught",
    "aughts",
    "available",
    "availabler",
    "availablest",
    "awfully",
    "b",
    "be",
    "became",
    "because",
    "become",
    "becomes",
    "becoming",
    "becominger",
    "becomingest",
    "becomings",
    "been",
    "before",
    "beforehand",
    "beforehander",
    "beforehandest",
    "behind",
    "behinds",
    "below",
    "beneath",
    "beside",
    "besides",
    "better",
    "bettered",
    "bettering",
    "betters",
    "between",
    "betwixt",
    "beyond",
    "bist",
    "both",
    "but",
    "buts",
    "by",
    "by-and-by",
    "byandby",
    "c",
    "cannot",
    "canst",
    "cant",
    "canted",
    "cantest",
    "canting",
    "cants",
    "cer",
    "certain",
    "certainer",
    "certainest",
    "cest",
    "chez",
    "circa",
    "co",
    "come-on",
    "come-ons",
    "comeon",
    "comeons",
    "concerning",
    "concerninger",
    "concerningest",
    "consequently",
    "considering",
    "could",
    "couldst",
    "cum",
    "d",
    "dday",
    "ddays",
    "describe",
    "described",
    "describes",
    "describing",
    "despite",
    "despited",
    "despites",
    "despiting",
    "did",
    "different",
    "differenter",
    "differentest",
    "do",
    "doe",
    "does",
    "doing",
    "doings",
    "done",
    "doner",
    "dones",
    "donest",
    "dos",
    "dost",
    "doth",
    "downs",
    "downward",
    "downwarder",
    "downwardest",
    "downwards",
    "during",
    "e",
    "each",
    "eg",
    "eight",
    "either",
    "else",
    "elsewhere",
    "enough",
    "ere",
    "et",
    "etc",
    "even",
    "evened",
    "evenest",
    "evens",
    "evenser",
    "evensest",
    "ever",
    "every",
    "everybody",
    "everyone",
    "everything",
    "everywhere",
    "ex",
    "except",
    "excepted",
    "excepting",
    "excepts",
    "exes",
    "f",
    "fact",
    "facts",
    "failing",
    "failings",
    "few",
    "fewer",
    "fewest",
    "figupon",
    "figuponed",
    "figuponing",
    "figupons",
    "five",
    "followthrough",
    "for",
    "forby",
    "forbye",
    "fore",
    "forer",
    "fores",
    "forever",
    "former",
    "formerer",
    "formerest",
    "formerly",
    "formers",
    "fornenst",
    "forwhy",
    "four",
    "fourscore",
    "frae",
    "from",
    "fs",
    "further",
    "furthered",
    "furtherer",
    "furtherest",
    "furthering",
    "furthermore",
    "furthers",
    "g",
    "get",
    "gets",
    "getting",
    "go",
    "gone",
    "good",
    "got",
    "gotta",
    "gotten",
    "h",
    "had",
    "hadst",
    "hae",
    "hardly",
    "has",
    "hast",
    "hath",
    "have",
    "haves",
    "having",
    "he",
    "hence",
    "her",
    "hereafter",
    "hereafters",
    "hereby",
    "herein",
    "hereupon",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "hither",
    "hitherer",
    "hitherest",
    "hoo",
    "hoos",
    "how",
    "how-do-you-do",
    "howbeit",
    "howdoyoudo",
    "however",
    "huh",
    "humph",
    "i",
    "idem",
    "idemer",
    "idemest",
    "ie",
    "if",
    "ifs",
    "immediate",
    "immediately",
    "immediater",
    "immediatest",
    "in",
    "inasmuch",
    "inc",
    "indeed",
    "indicate",
    "indicated",
    "indicates",
    "indicating",
    "info",
    "information",
    "insofar",
    "instead",
    "into",
    "inward",
    "inwarder",
    "inwardest",
    "inwards",
    "is",
    "it",
    "its",
    "itself",
    "j",
    "k",
    "l",
    "latter",
    "latterer",
    "latterest",
    "latterly",
    "latters",
    "layabout",
    "layabouts",
    "less",
    "lest",
    "lot",
    "lots",
    "lotted",
    "lotting",
    "m",
    "main",
    "make",
    "many",
    "mauger",
    "maugre",
    "mayest",
    "me",
    "meanwhile",
    "meanwhiles",
    "midst",
    "midsts",
    "might",
    "mights",
    "more",
    "moreover",
    "most",
    "mostly",
    "much",
    "mucher",
    "muchest",
    "must",
    "musth",
    "musths",
    "musts",
    "my",
    "myself",
    "n",
    "natheless",
    "nathless",
    "neath",
    "neaths",
    "necessarier",
    "necessariest",
    "necessary",
    "neither",
    "nethe",
    "nethermost",
    "never",
    "nevertheless",
    "nigh",
    "nigher",
    "nighest",
    "nine",
    "no",
    "no-one",
    "nobodies",
    "nobody",
    "noes",
    "none",
    "noone",
    "nor",
    "nos",
    "not",
    "nothing",
    "nothings",
    "notwithstanding",
    "nowhere",
    "nowheres",
    "o",
    "of",
    "off",
    "offest",
    "offs",
    "often",
    "oftener",
    "oftenest",
    "oh",
    "on",
    "one",
    "oneself",
    "onest",
    "ons",
    "onto",
    "or",
    "orer",
    "orest",
    "other",
    "others",
    "otherwise",
    "otherwiser",
    "otherwisest",
    "ought",
    "oughts",
    "our",
    "ours",
    "ourself",
    "ourselves",
    "out",
    "outed",
    "outest",
    "outs",
    "outside",
    "outwith",
    "over",
    "overall",
    "overaller",
    "overallest",
    "overalls",
    "overs",
    "own",
    "owned",
    "owning",
    "owns",
    "owt",
    "p",
    "particular",
    "particularer",
    "particularest",
    "particularly",
    "particulars",
    "per",
    "perhaps",
    "plaintiff",
    "please",
    "pleased",
    "pleases",
    "plenties",
    "plenty",
    "pro",
    "probably",
    "provide",
    "provided",
    "provides",
    "providing",
    "q",
    "qua",
    "que",
    "quite",
    "r",
    "rath",
    "rathe",
    "rather",
    "rathest",
    "re",
    "really",
    "regarding",
    "relate",
    "related",
    "relatively",
    "res",
    "respecting",
    "respectively",
    "s",
    "said",
    "saider",
    "saidest",
    "same",
    "samer",
    "sames",
    "samest",
    "sans",
    "sanserif",
    "sanserifs",
    "sanses",
    "saved",
    "sayid",
    "sayyid",
    "seem",
    "seemed",
    "seeminger",
    "seemingest",
    "seemings",
    "seems",
    "send",
    "sent",
    "senza",
    "serious",
    "seriouser",
    "seriousest",
    "seven",
    "several",
    "severaler",
    "severalest",
    "shall",
    "shalled",
    "shalling",
    "shalls",
    "she",
    "should",
    "shoulded",
    "shoulding",
    "shoulds",
    "since",
    "sine",
    "sines",
    "sith",
    "six",
    "so",
    "sobeit",
    "soer",
    "soest",
    "some",
    "somebody",
    "somehow",
    "someone",
    "something",
    "sometime",
    "sometimer",
    "sometimes",
    "sometimest",
    "somewhat",
    "somewhere",
    "stop",
    "stopped",
    "such",
    "summat",
    "sup",
    "supped",
    "supping",
    "sups",
    "syn",
    "syne",
    "t",
    "ten",
    "than",
    "that",
    "the",
    "thee",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "thence",
    "thener",
    "thenest",
    "there",
    "thereafter",
    "thereby",
    "therefore",
    "therein",
    "therer",
    "therest",
    "thereupon",
    "these",
    "they",
    "thine",
    "thing",
    "things",
    "this",
    "thises",
    "thorough",
    "thorougher",
    "thoroughest",
    "thoroughly",
    "those",
    "thou",
    "though",
    "thous",
    "thouses",
    "three",
    "thro",
    "through",
    "througher",
    "throughest",
    "throughout",
    "thru",
    "thruer",
    "thruest",
    "thus",
    "thy",
    "thyself",
    "till",
    "tilled",
    "tilling",
    "tills",
    "to",
    "together",
    "too",
    "toward",
    "towarder",
    "towardest",
    "towards",
    "two",
    "u",
    "umpteen",
    "under",
    "underneath",
    "unless",
    "unlike",
    "unliker",
    "unlikest",
    "until",
    "unto",
    "up",
    "upon",
    "uponed",
    "uponing",
    "upons",
    "upped",
    "upping",
    "ups",
    "us",
    "use",
    "used",
    "usedest",
    "username",
    "usually",
    "v",
    "various",
    "variouser",
    "variousest",
    "verier",
    "veriest",
    "versus",
    "very",
    "via",
    "vis-a-vis",
    "vis-a-viser",
    "vis-a-visest",
    "viz",
    "vs",
    "w",
    "was",
    "wast",
    "we",
    "were",
    "wert",
    "what",
    "whatever",
    "whateverer",
    "whateverest",
    "whatsoever",
    "whatsoeverer",
    "whatsoeverest",
    "wheen",
    "when",
    "whenas",
    "whence",
    "whencesoever",
    "whenever",
    "whensoever",
    "where",
    "whereafter",
    "whereas",
    "whereby",
    "wherefrom",
    "wherein",
    "whereinto",
    "whereof",
    "whereon",
    "wheresoever",
    "whereto",
    "whereupon",
    "wherever",
    "wherewith",
    "wherewithal",
    "whether",
    "which",
    "whichever",
    "whichsoever",
    "while",
    "whiles",
    "whilst",
    "whither",
    "whithersoever",
    "whoever",
    "whomever",
    "whose",
    "whoso",
    "whosoever",
    "why",
    "with",
    "withal",
    "within",
    "without",
    "would",
    "woulded",
    "woulding",
    "woulds",
    "x",
    "y",
    "ye",
    "yet",
    "yon",
    "yond",
    "yonder",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
    "z",
    "zillion",
  };
  private final Set<String> stopwords = Sets.newHashSet(TERRIER_STOP_WORDS);
  VocabularyWritable vocab;
  private boolean isStopwordRemoval = true;

  public OpenNLPTokenizer(){
    super();
  }

  @Override
  public void configure(Configuration mJobConf){
    FileSystem fs;
    try {
      fs = FileSystem.get(mJobConf);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } 
    setTokenizer(fs, new Path(mJobConf.get("Ivory.TokenizerModel")));
    setLanguageAndStemmer(mJobConf.get("Ivory.Lang"));

    VocabularyWritable vocab;
    try {
      vocab = (VocabularyWritable) HadoopAlign.loadVocab(new Path(mJobConf.get("Ivory.CollectionVocab")), fs);
      setVocab(vocab);
    } catch (Exception e) {
      sLogger.warn("No vocabulary provided to tokenizer.");
      vocab = null;
    }
  }

  @Override
  public void configure(Configuration mJobConf, FileSystem fs){
    setTokenizer(fs, new Path(mJobConf.get("Ivory.TokenizerModel")));
    setLanguageAndStemmer(mJobConf.get("Ivory.Lang"));
    VocabularyWritable vocab;
    try {
      vocab = (VocabularyWritable) HadoopAlign.loadVocab(new Path(mJobConf.get("Ivory.CollectionVocab")), fs);
      setVocab(vocab);
    } catch (Exception e) {
      sLogger.warn("No vocabulary provided to tokenizer.");
      vocab = null;
    }
  }

  public void setTokenizer(FileSystem fs, Path p){
    try {
      FSDataInputStream in = fs.open(p);
      TokenizerModel model;
      model = new TokenizerModel(in);
      tokenizer = new TokenizerME(model);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unchecked")
  public void setLanguageAndStemmer(String l){
    if(l.startsWith("en")){
      lang = ENGLISH;//"english";
    }else if(l.startsWith("fr")){
      lang = FRENCH;//"french";
    }else if(l.equals("german") || l.startsWith("de")){
      lang = GERMAN;//"german";
    }else{
      sLogger.warn("Language not recognized, setting to English!");
    }
    Class stemClass;
    try {
      stemClass = Class.forName("org.tartarus.snowball.ext." +
          languages[lang] + "Stemmer");
      stemmer = (SnowballStemmer) stemClass.newInstance();
    } catch (ClassNotFoundException e) {
      sLogger.warn("Stemmer class not recognized!\n"+"org.tartarus.snowball.ext." +
          languages[lang] + "Stemmer");
      stemmer = null;
      return;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } 
  }

  public void setVocab(VocabularyWritable v){
    vocab = v;
  }

  @Override
  public String[] processContent(String text) {
    String[] tokens = tokenizer.tokenize(text.toLowerCase());
    List<String> stemmedTokens = new ArrayList<String>();
    for(String token : tokens){
      token = removeNonUnicodeChars(token);
      if(isDiscard(token)){
      //  sLogger.warn("Discarded stopword "+token);
        continue;
      }

      //apply stemming on token
      String stemmed = token;
      if(stemmer!=null){
        stemmer.setCurrent(token);
        stemmer.stem();
        stemmed = stemmer.getCurrent();
      }

      //skip if out of vocab
      if(vocab!=null && vocab.get(stemmed)<=0){
        continue;
      }
      stemmedTokens.add(stemmed);
    }

    String[] tokensArray = new String[stemmedTokens.size()];
    int i=0;
    for(String ss : stemmedTokens){
      tokensArray[i++]=ss;
    }
    return tokensArray;

  }

  public String getLanguage() {
    return languages[lang];
  }
  
  @Override
  public int getNumberTokens(String string){
    return tokenizer.tokenize(string).length;
  }

  private boolean isDiscard(String token) {
    return ((lang==ENGLISH && isStopwordRemoval && stopwords.contains(token)) || delims.contains(token) || token.length() < MIN_LENGTH || token.length() > MAX_LENGTH);
  }

  /* 
   * For external use. returns true if token is a Galago stopword or a delimiter: `~!@#$%^&*()-_=+]}[{\\|'\";:/?.>,<
   */
  @Override
  public boolean isStopWord(String token) {
    return (stopwords.contains(token) || delims.contains(token));
  }
  
//  /**
//   * @param token
//   * @return
//   *    if stemmer is available, returns stemmed string. otherwise, throws RuntimeException
//   */
//  public String stem(String token) {
//    if(stemmer!=null){
//      stemmer.setCurrent(token);
//      stemmer.stem();
//      return stemmer.getCurrent().toLowerCase();
//    }else {
//      throw new RuntimeException("Stemmer is not initialized for language " + getLanguage() +". Use method setLanguageandStemmer(String lang_code)");
//    }
//  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException{
    if(args.length < 4){
      System.err.println("usage: [input] [language] [tokenizer-model-path] [output-file]");
    }
    ivory.core.tokenize.Tokenizer tokenizer = TokenizerFactory.createTokenizer(args[1], args[2], null);
    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[3]), "UTF8"));
    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(args[0]), "UTF8"));

    //    DataInput in = new DataInputStream(new BufferedInputStream(FileSystem.getLocal(new Configuration()).open(new Path(args[0]))));
    String line = null;
    while((line = in.readLine()) != null){
      String[] tokens = tokenizer.processContent(line);
      String s = "";
      for (String token : tokens) {
        s += token+" ";
      }
      out.write(s+"\n");
    }
    out.close();
  }
}