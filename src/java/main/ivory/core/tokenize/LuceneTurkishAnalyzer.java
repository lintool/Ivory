package ivory.core.tokenize;

import ivory.core.Constants;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.tr.TurkishLowerCaseFilter;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.turkishStemmer;

public class LuceneTurkishAnalyzer extends ivory.core.tokenize.Tokenizer {
  protected static int MIN_LENGTH = 2, MAX_LENGTH = 50;
  private Tokenizer tokenizer;
  private SnowballStemmer stemmer;
  private boolean isStemming, isStopwordRemoval;
//  private static final Map<String, Integer> TURKISH_LUCENE_STOP_WORDS = new HashMap<String, Integer>();
//  static {
//    TURKISH_LUCENE_STOP_WORDS.put("acaba", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("altmış", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("altı", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ama", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ancak", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("arada", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("aslında", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ayrıca", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bana", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bazı", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("belki", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ben", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("benden", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("beni", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("benim", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("beri", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("beş", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bile", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bin", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bir", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("birçok", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("biri", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("birkaç", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("birkez", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("birşey", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("birşeyi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("biz", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bize", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bizden", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bizi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bizim", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("böyle", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("böylece", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bu", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("buna", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bunda", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bundan", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bunlar", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bunları", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bunların", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bunu", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("bunun", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("burada", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("çok", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("çünkü", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("da", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("daha", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("dahi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("de", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("defa", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("değil", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("diğer", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("diye", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("doksan", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("dokuz", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("dolayı", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("dolayısıyla", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("dört", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("edecek", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("eden", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ederek", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("edilecek", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ediliyor", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("edilmesi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ediyor", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("eğer", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("elli", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("en", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("etmesi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("etti", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ettiği", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ettiğini", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("gibi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("göre", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("halen", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("hangi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("hatta", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("hem", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("henüz", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("hep", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("hepsi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("her", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("herhangi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("herkesin", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("hiç", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("hiçbir", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("için", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("iki", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ile", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ilgili", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ise", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("işte", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("itibaren", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("itibariyle", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kadar", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("karşın", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("katrilyon", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kendi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kendilerine", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kendini", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kendisi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kendisine", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kendisini", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kez", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ki", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kim", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kimden", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kime", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kimi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kimse", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("kırk", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("milyar", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("milyon", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("mu", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("mü", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("mı", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("nasıl", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ne", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("neden", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("nedenle", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("nerde", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("nerede", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("nereye", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("niye", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("niçin", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("o", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olan", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olarak", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("oldu", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olduğu", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olduğunu", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olduklarını", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olmadı", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olmadığı", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olmak", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olması", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olmayan", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olmaz", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olsa", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olsun", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olup", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olur", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("olursa", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("oluyor", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("on", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ona", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ondan", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("onlar", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("onlardan", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("onları", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("onların", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("onu", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("onun", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("otuz", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("oysa", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("öyle", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("pek", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("rağmen", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("sadece", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("sanki", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("sekiz", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("seksen", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("sen", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("senden", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("seni", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("senin", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("siz", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("sizden", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("sizi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("sizin", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şey", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şeyden", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şeyi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şeyler", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şöyle", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şu", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şuna", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şunda", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şundan", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şunları", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("şunu", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("tarafından", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("trilyon", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("tüm", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("üç", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("üzere", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("var", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("vardı", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ve", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("veya", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("ya", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yani", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yapacak", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yapılan", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yapılması", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yapıyor", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yapmak", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yaptı", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yaptığı", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yaptığını", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yaptıkları", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yedi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yerine", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yetmiş", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yine", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yirmi", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yoksa", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("yüz", 1);
//    TURKISH_LUCENE_STOP_WORDS.put("zaten", 1);
//  }

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
//    if (isStemming) {
//      tokenStream = new SnowballFilter(tokenStream, new turkishStemmer());
//    }

    CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.clearAttributes();
    String tokenized = "";
    try {
      while (tokenStream.incrementToken()) {
        String token = termAtt.toString();
        if ( stemmer!=null ) {
          stemmer.setCurrent(token);
          stemmer.stem();
          token = stemmer.getCurrent();
        }
        tokenized += ( token + " " );
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return tokenized.trim().split(" ");
  }
  
//  private boolean isDiscard(String token) {
//    // remove characters that may cause problems when processing further
//    //    token = removeNonUnicodeChars(token);
//    return ( token.length() < MIN_LENGTH || token.length() > MAX_LENGTH  || TURKISH_LUCENE_STOP_WORDS.containsKey(token));
//  }
}
