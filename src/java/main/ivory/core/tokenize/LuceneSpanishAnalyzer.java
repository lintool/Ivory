package ivory.core.tokenize;

import ivory.core.Constants;
import java.io.StringReader;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.spanishStemmer;
import edu.umd.hooka.VocabularyWritable;

public class LuceneSpanishAnalyzer extends ivory.core.tokenize.Tokenizer {
  private static final Logger LOG = Logger.getLogger(LuceneSpanishAnalyzer.class);
  static{
    LOG.setLevel(Level.WARN);
  }
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
    // read stopwords from file (stopwords will be empty set if file does not exist or is empty)
    String stopwordsFile = conf.get(Constants.StopwordList);
    stopwords = readInput(fs, stopwordsFile);      
    String stemmedStopwordsFile = conf.get(Constants.StemmedStopwordList);
    stemmedStopwords = readInput(fs, stemmedStopwordsFile);
    isStopwordRemoval = !stopwords.isEmpty();
    
    isStemming = conf.getBoolean(Constants.Stemming, true);
    if (isStemming) {
      stemmer = new spanishStemmer();
    }
    
    LOG.warn("Stemming is " + isStemming + "; Stopword removal is " + isStopwordRemoval +"; number of stopwords: " + stopwords.size() +"; stemmed: " + stemmedStopwords.size());
  }
  
  @Override
  public String[] processContent(String text) {  
    tokenizer = new StandardTokenizer(Version.LUCENE_35, new StringReader(text));
    TokenStream tokenStream = new StandardFilter(Version.LUCENE_35, tokenizer);
    tokenStream = new LowerCaseFilter(Version.LUCENE_35, tokenStream);
    String tokenized = postNormalize(streamToString(tokenStream));

    StringBuilder finalTokenized = new StringBuilder();
    for (String token : tokenized.split(" ")) {
      if ( isStopwordRemoval && isDiscard(token) ) {
        continue;
      }
      if ( stemmer != null ) {
        stemmer.setCurrent(token);
        stemmer.stem();
        token = stemmer.getCurrent();
      }
      if ( vocab != null && vocab.get(token) <= 0) {
        continue;
      } 
      finalTokenized.append(token + " ");
    }
    return finalTokenized.toString().trim().split(" ");
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
