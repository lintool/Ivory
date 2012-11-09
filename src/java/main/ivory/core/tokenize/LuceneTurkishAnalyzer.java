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
import edu.umd.hooka.VocabularyWritable;

public class LuceneTurkishAnalyzer extends ivory.core.tokenize.Tokenizer {
  private Tokenizer tokenizer;
  private SnowballStemmer stemmer;
  private boolean isStemming;
  private Set<String> stopwords;
  private Set<String> stemmedStopwords;

  @Override
  public void configure(Configuration conf) {
    configure(conf, null);
  }

  @Override
  public void configure(Configuration conf, FileSystem fs) {
    isStemming = conf.getBoolean(Constants.Stemming, true);    
    if (isStemming) {
      stemmer = new turkishStemmer();
    }
    
    // read stopwords from file (stopwords will be empty set if file does not exist or is empty)
    String stopwordsFile = conf.get(Constants.StopwordList);
    stopwords = readInput(stopwordsFile);      
    String stemmedStopwordsFile = conf.get(Constants.StemmedStopwordList);
    stemmedStopwords = readInput(stemmedStopwordsFile);
    isStopwordRemoval = !stopwords.isEmpty();
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
        if ( vocab != null && vocab.get(token) <= 0 ) {
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
    return stopwords.contains(token) || delims.contains(token);
  }
  
  @Override
  public boolean isStemmedStopWord(String token) {
    return stemmedStopwords.contains(token) || delims.contains(token);
  }
  
  @Override
  public String stem(String token) {
    token = postNormalize(preNormalize(token)).toLowerCase();
    if ( stemmer!=null ) {
      stemmer.setCurrent(token);
      stemmer.stem();
      return stemmer.getCurrent();
    }else {
      return token;
    }
  }

  @Override
  public void setVocab(VocabularyWritable v) {
    
  }
}
