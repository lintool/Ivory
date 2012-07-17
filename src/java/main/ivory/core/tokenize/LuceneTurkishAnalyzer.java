package ivory.core.tokenize;

import ivory.core.Constants;
import java.io.IOException;
import java.io.StringReader;
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

public class LuceneTurkishAnalyzer extends ivory.core.tokenize.Tokenizer {
  protected static int MIN_LENGTH = 2, MAX_LENGTH = 50;
  private Tokenizer tokenizer;
  private SnowballStemmer stemmer;
  private boolean isStemming, isStopwordRemoval;

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
        tokenized += ( token + " " );
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return tokenized.trim().split(" ");
  }
}
