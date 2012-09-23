package ivory.core.tokenize;

import ivory.core.Constants;

import java.io.IOException;
import java.io.StringReader;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.tr.TurkishLowerCaseFilter;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.turkishStemmer;

import com.google.common.collect.Sets;

import edu.umd.hooka.VocabularyWritable;

public class LuceneTurkishAnalyzer extends ivory.core.tokenize.Tokenizer {
  private Tokenizer tokenizer;
  private SnowballStemmer stemmer;
  private boolean isStemming;
  private final Set<String> turkishStopwords = Sets.newHashSet(LUCENE_STOP_WORDS);
  private final Set<String> turkishStemmedStopwords = Sets.newHashSet(LUCENE_STEMMED_STOP_WORDS);
  private static final String[] LUCENE_STOP_WORDS = { 
    "acaba",
    "altmış",
    "altı",
    "ama",
    "ancak",
    "arada",
    "aslında",
    "ayrıca",
    "bana",
    "bazı",
    "belki",
    "ben",
    "benden",
    "beni",
    "benim",
    "beri",
    "beş",
    "bile",
    "bin",
    "bir",
    "birçok",
    "biri",
    "birkaç",
    "birkez",
    "birşey",
    "birşeyi",
    "biz",
    "bize",
    "bizden",
    "bizi",
    "bizim",
    "böyle",
    "böylece",
    "bu",
    "buna",
    "bunda",
    "bundan",
    "bunlar",
    "bunları",
    "bunların",
    "bunu",
    "bunun",
    "burada",
    "çok",
    "çünkü",
    "da",
    "daha",
    "dahi",
    "de",
    "defa",
    "değil",
    "diğer",
    "diye",
    "doksan",
    "dokuz",
    "dolayı",
    "dolayısıyla",
    "dört",
    "edecek",
    "eden",
    "ederek",
    "edilecek",
    "ediliyor",
    "edilmesi",
    "ediyor",
    "eğer",
    "elli",
    "en",
    "etmesi",
    "etti",
    "ettiği",
    "ettiğini",
    "gibi",
    "göre",
    "halen",
    "hangi",
    "hatta",
    "hem",
    "henüz",
    "hep",
    "hepsi",
    "her",
    "herhangi",
    "herkesin",
    "hiç",
    "hiçbir",
    "için",
    "iki",
    "ile",
    "ilgili",
    "ise",
    "işte",
    "itibaren",
    "itibariyle",
    "kadar",
    "karşın",
    "katrilyon",
    "kendi",
    "kendilerine",
    "kendini",
    "kendisi",
    "kendisine",
    "kendisini",
    "kez",
    "ki",
    "kim",
    "kimden",
    "kime",
    "kimi",
    "kimse",
    "kırk",
    "milyar",
    "milyon",
    "mu",
    "mü",
    "mı",
    "nasıl",
    "ne",
    "neden",
    "nedenle",
    "nerde",
    "nerede",
    "nereye",
    "niye",
    "niçin",
    "o",
    "olan",
    "olarak",
    "oldu",
    "olduğu",
    "olduğunu",
    "olduklarını",
    "olmadı",
    "olmadığı",
    "olmak",
    "olması",
    "olmayan",
    "olmaz",
    "olsa",
    "olsun",
    "olup",
    "olur",
    "olursa",
    "oluyor",
    "on",
    "ona",
    "ondan",
    "onlar",
    "onlardan",
    "onları",
    "onların",
    "onu",
    "onun",
    "otuz",
    "oysa",
    "öyle",
    "pek",
    "rağmen",
    "sadece",
    "sanki",
    "sekiz",
    "seksen",
    "sen",
    "senden",
    "seni",
    "senin",
    "siz",
    "sizden",
    "sizi",
    "sizin",
    "şey",
    "şeyden",
    "şeyi",
    "şeyler",
    "şöyle",
    "şu",
    "şuna",
    "şunda",
    "şundan",
    "şunları",
    "şunu",
    "tarafından",
    "trilyon",
    "tüm",
    "üç",
    "üzere",
    "var",
    "vardı",
    "ve",
    "veya",
    "ya",
    "yani",
    "yapacak",
    "yapılan",
    "yapılması",
    "yapıyor",
    "yapmak",
    "yaptı",
    "yaptığı",
    "yaptığını",
    "yaptıkları",
    "yedi",
    "yerine",
    "yetmiş",
    "yine",
    "yirmi",
    "yoksa",
    "yüz",
    "zaten"
    };
  private static final String[] LUCENE_STEMMED_STOP_WORDS = {
    "acap",
    "altmış",
    "al",
    "am",
    "ancak",
    "ara",
    "asl",
    "ayrıç",
    "ba",
    "baz",
    "belki",
    "ben",
    "be",
    "be",
    "be",
    "ber",
    "beş",
    "bil",
    "bin",
    "bir",
    "birçok",
    "bir",
    "birkaç",
    "birkez",
    "birşey",
    "birşe",
    "biz",
    "biz",
    "biz",
    "biz",
    "biz",
    "bö",
    "böyleç",
    "bu",
    "p",
    "p",
    "p",
    "bun",
    "bun",
    "p",
    "p",
    "bu",
    "bura",
    "çok",
    "çünkü",
    "da",
    "dah",
    "dahi",
    "de",
    "defa",
    "değil",
    "diğer",
    "di",
    "dok",
    "dok",
    "dola",
    "dolayı",
    "dört",
    "edecek",
    "e",
    "ederek",
    "edilecek",
    "ediliyor",
    "edilmes",
    "ediyor",
    "eğer",
    "elli",
    "en",
    "etmes",
    "et",
    "ettik",
    "ettik",
    "gip",
    "gör",
    "hale",
    "hangi",
    "hat",
    "hem",
    "he",
    "hep",
    "hepsi",
    "her",
    "herhangi",
    "herke",
    "hiç",
    "hiçbir",
    "iç",
    "ik",
    "il",
    "ilgil",
    "is",
    "iş",
    "itibare",
    "itibar",
    "kadar",
    "karş",
    "katrilyo",
    "ke",
    "kendi",
    "kendi",
    "kendis",
    "kendi",
    "kendi",
    "kez",
    "ki",
    "kim",
    "k",
    "k",
    "k",
    "k",
    "kırk",
    "milyar",
    "milyo",
    "mu",
    "mü",
    "mı",
    "nasıl",
    "ne",
    "ne",
    "nede",
    "ner",
    "nere",
    "nere",
    "ni",
    "niç",
    "o",
    "ola",
    "olarak",
    "ol",
    "olduk",
    "olduk",
    "olduk",
    "olmadı",
    "olmadık",
    "olmak",
    "olmas",
    "olmaya",
    "olmaz",
    "ol",
    "ol",
    "olup",
    "olur",
    "olur",
    "oluyor",
    "on",
    "on",
    "on",
    "on",
    "on",
    "on",
    "on",
    "on",
    "o",
    "ot",
    "o",
    "ö",
    "pek",
    "rağme",
    "sadeç",
    "sanki",
    "sek",
    "sek",
    "sen",
    "se",
    "se",
    "se",
    "siz",
    "siz",
    "siz",
    "siz",
    "şey",
    "şey",
    "şe",
    "şey",
    "şö",
    "şu",
    "ş",
    "ş",
    "ş",
    "şun",
    "ş",
    "taraf",
    "trilyo",
    "tüm",
    "üç",
    "üzer",
    "var",
    "var",
    "ve",
    "veya",
    "ya",
    "yani",
    "yapacak",
    "yapıla",
    "yapılmas",
    "yapıyor",
    "yapmak",
    "yap",
    "yaptık",
    "yaptık",
    "yaptık",
    "yedi",
    "yer",
    "yet",
    "y",
    "yirmi",
    "yok",
    "yüz",
    "zate"
  };

  @Override
  public void configure(Configuration conf) {
    configure(conf, null);
  }

  @Override
  public void configure(Configuration conf, FileSystem fs) {
    isStopwordRemoval = conf.getBoolean(Constants.Stopword, true);      
    isStemming = conf.getBoolean(Constants.Stemming, true);
    
    if (isStemming) {
      stemmer = new turkishStemmer();
    }
  }

  @Override
  public String[] processContent(String text) {  
    tokenizer = new StandardTokenizer(Version.LUCENE_35, new StringReader(text));
    TokenStream tokenStream = new StandardFilter(Version.LUCENE_35, tokenizer);
    tokenStream = new TurkishLowerCaseFilter(tokenStream);
    if (isStopwordRemoval) {
      tokenStream = new StopFilter( Version.LUCENE_35, tokenStream, (CharArraySet) TurkishAnalyzer.getDefaultStopSet());
    }

    CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.clearAttributes();
    String tokenized = "";
    try {
      while (tokenStream.incrementToken()) {
        String token = termAtt.toString();
        if ( stemmer != null ) {
          stemmer.setCurrent(token);
          stemmer.stem();
          token = stemmer.getCurrent();
        }
        if ( vocab != null && vocab.get(token) <= 0) {
          continue;
        }
        tokenized += ( token + " " );
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return tokenized.trim().split("\\s+");
  }
  
  @Override
  public boolean isStopWord(String token) {
    return turkishStopwords.contains(token) || delims.contains(token);
  }
  
  @Override
  public boolean isStemmedStopWord(String token) {
    return turkishStemmedStopwords.contains(token) || delims.contains(token);
  }
  
  @Override
  public String stem(String token) {
    tokenizer = new StandardTokenizer(Version.LUCENE_35, new StringReader(token));
    TokenStream tokenStream = new StandardFilter(Version.LUCENE_35, tokenizer);
    tokenStream = new TurkishLowerCaseFilter(tokenStream);
    
    CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.clearAttributes();
    try {
      while (tokenStream.incrementToken()) {
        return termAtt.toString();
      }
    }catch (IOException e) {
      e.printStackTrace();
    }
    return token;
  }

  @Override
  public void setVocab(VocabularyWritable v) {
    
  }
}
