package ivory.core.tokenize;

import ivory.core.Constants;
import java.io.StringReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;
import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;
import java.util.Map;
import java.util.HashMap;

public class LuceneAnalyzer extends ivory.core.tokenize.Tokenizer {
  private static final Logger LOG = Logger.getLogger(LuceneAnalyzer.class);
  static{
    LOG.setLevel(Level.WARN);
  }
  private Tokenizer tokenizer;
  private Stemmer stemmer;
  private int lang;
  private static final int SPANISH = 0, TURKISH = 1, CZECH = 2;
  private static final String[] classes = {
    "org.tartarus.snowball.ext.spanishStemmer", 
    "org.tartarus.snowball.ext.turkishStemmer", 
    "ivory.core.tokenize.CzechStemmer"};
  
  @Override
  public void configure(Configuration conf) {
    configure(conf, null);
  }

  @Override
  public void configure(Configuration conf, FileSystem fs) {
    if (conf.getBoolean(Constants.Stemming, true)) {
      setLanguageAndStemmer(conf.get(Constants.Language));
      isStemming = true;
    }else {
      setLanguage(conf.get(Constants.Language));
    }
    
    // read stopwords from file (stopwords will be empty set if file does not exist or is empty)
    String stopwordsFile = conf.get(Constants.StopwordList);
    stopwords = readInput(fs, stopwordsFile);      
    String stemmedStopwordsFile = conf.get(Constants.StemmedStopwordList);
    stemmedStopwords = readInput(fs, stemmedStopwordsFile);
    isStopwordRemoval = !stopwords.isEmpty();
    
    VocabularyWritable vocab;
    try {
      vocab = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get(Constants.CollectionVocab)), fs);
      setVocab(vocab);
    } catch (Exception e) {
      LOG.warn("No vocabulary provided to tokenizer.");
      vocab = null;
    }
    
    LOG.warn("Stemming is " + isStemming + "; Stopword removal is " + isStopwordRemoval +"; number of stopwords: " + stopwords.size() +"; stemmed: " + stemmedStopwords.size());
  }
  
  public void setLanguage(String l){
    if(l.equalsIgnoreCase("spanish") || l.equalsIgnoreCase("es")){
      lang = SPANISH;
    }else if (l.equalsIgnoreCase("turkish") || l.equalsIgnoreCase("tr")){
      lang = TURKISH;
    }else if (l.equalsIgnoreCase("czech") || l.equalsIgnoreCase("cs") || l.equalsIgnoreCase("cz")){
      lang = CZECH;
    }else{
      LOG.warn("Language not recognized, setting to English!");
    }
  }

  @SuppressWarnings("unchecked")
  public void setLanguageAndStemmer(String l){
    setLanguage(l);
    Class<? extends Stemmer> stemClass;
    try {
      stemClass = (Class<? extends Stemmer>) Class.forName(classes[lang]);
      stemmer = (Stemmer) stemClass.newInstance();
    } catch (ClassNotFoundException e) {
      LOG.warn("Stemmer class not recognized!\n" + classes[lang]);
      stemmer = null;
      return;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } 
  }
  
  @Override
  public String[] processContent(String text) {  
    tokenizer = new StandardTokenizer(Version.LUCENE_35, new StringReader(text));
    TokenStream tokenStream = new StandardFilter(Version.LUCENE_35, tokenizer);
    tokenStream = new LowerCaseFilter(Version.LUCENE_35, tokenStream);
    String tokenized = postNormalize(streamToString(tokenStream));

    StringBuilder finalTokenized = new StringBuilder();
    for (String token : tokenized.split(" ")) {
      if ( isStopwordRemoval() && isDiscard(false, token) ) {
        continue;
      }
      String stemmedToken = stem(token);
      
      if ( vocab != null && vocab.get(stemmedToken) <= 0) {
        continue;
      } 
      finalTokenized.append(stemmedToken + " ");
    }
    return finalTokenized.toString().trim().split(" ");
  }

  @Override
  public Map<String, String> getStem2NonStemMapping(String text) {
    Map<String, String> stem2NonStemMapping = new HashMap<String, String>();
    tokenizer = new StandardTokenizer(Version.LUCENE_35, new StringReader(text));
    TokenStream tokenStream = new StandardFilter(Version.LUCENE_35, tokenizer);
    tokenStream = new LowerCaseFilter(Version.LUCENE_35, tokenStream);
    String tokenized = postNormalize(streamToString(tokenStream));
    
    StringBuilder finalTokenized = new StringBuilder();
    for (String token : tokenized.split(" ")) {
      if ( isStopwordRemoval() && isDiscard(false, token) ) {
        continue;
      }
      String stemmedToken = stem(token);
      
      if ( vocab != null && vocab.get(stemmedToken) <= 0) {
        continue;
      }
      stem2NonStemMapping.put(stemmedToken, token);
    }
    return stem2NonStemMapping;

  }

  @Override
  public String stem(String token) {
    if ( stemmer != null ) {
      return stemmer.toStem(token);
    }else {
      return token;
    }
  }

  @Override
  public float getOOVRate(String text, VocabularyWritable vocab) {
    int countOOV = 0, countAll = 0;
    tokenizer = new StandardTokenizer(Version.LUCENE_35, new StringReader(text));
    TokenStream tokenStream = new StandardFilter(Version.LUCENE_35, tokenizer);
    tokenStream = new LowerCaseFilter(Version.LUCENE_35, tokenStream);
    String tokenized = postNormalize(streamToString(tokenStream));

    for (String token : tokenized.split(" ")) {
      if ( isStopwordRemoval() && isDiscard(false, token) ) {
        continue;
      }
      String stemmedToken = stem(token);
      
      if ( vocab != null && vocab.get(stemmedToken) <= 0) {
        countOOV++;
      } 
      countAll++;
    }
    return (countOOV / (float) countAll);
  }
}
