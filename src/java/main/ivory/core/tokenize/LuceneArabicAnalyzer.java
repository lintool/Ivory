package ivory.core.tokenize;

import ivory.core.Constants;
import java.io.IOException;
import java.io.StringReader;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class LuceneArabicAnalyzer extends ivory.core.tokenize.Tokenizer {
  private static final Logger LOG = Logger.getLogger(LuceneArabicAnalyzer.class);
  static{
    LOG.setLevel(Level.WARN);
  }
  private boolean isStemming;
  private org.apache.lucene.analysis.Tokenizer tokenizer;
  private Set<String> stopwords;
  private Set<String> stemmedStopwords;

  @Override
  public void configure(Configuration conf) {
    configure(conf, null);
  }

  @Override
  public void configure(Configuration conf, FileSystem fs) {    
    // read stopwords from file (stopwords will be empty set if file does not exist or is empty)
    String stopwordsFile = conf.get(Constants.StopwordList);
    stopwords = readInput(fs, stopwordsFile);      
    String stemmedStopwordsFile = conf.get(Constants.StemmedStopwordList);
    stemmedStopwords = readInput(fs, stemmedStopwordsFile);
    isStopwordRemoval = !stopwords.isEmpty();

    isStemming = conf.getBoolean(Constants.Stemming, true);
    
    LOG.warn("Stemming is " + isStemming + "; Stopword removal is " + isStopwordRemoval +"; number of stopwords: " + stopwords.size() +"; stemmed: " + stemmedStopwords.size());
  }
  
  @Override
  public String[] processContent(String text) {   
    tokenizer = new StandardTokenizer(Version.LUCENE_35, new StringReader(text));
    TokenStream tokenStream = new LowerCaseFilter(Version.LUCENE_35, tokenizer);
    if (isStopwordRemoval) {
      tokenStream = new StopFilter( Version.LUCENE_35, tokenStream, (CharArraySet) ArabicAnalyzer.getDefaultStopSet());
    }
    tokenStream = new ArabicNormalizationFilter(tokenStream);
    if (isStemming) {
      tokenStream = new ArabicStemFilter(tokenStream);
    }

    CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.clearAttributes();
    String tokenized = "";
    try {
      while (tokenStream.incrementToken()) {
        String token = termAtt.toString();
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
    return stopwords.contains(token) || delims.contains(token) || token.length()==1;
  }

  @Override
  public String stem(String token) {
    tokenizer = new StandardTokenizer(Version.LUCENE_35, new StringReader(token));
    TokenStream tokenStream = new LowerCaseFilter(Version.LUCENE_35, tokenizer);
    tokenStream = new ArabicNormalizationFilter(tokenStream);
    tokenStream = new ArabicStemFilter(tokenStream);

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
  public boolean isStemmedStopWord(String token) {
    return stemmedStopwords.contains(token) || delims.contains(token) || token.length()==1;
  }
}
