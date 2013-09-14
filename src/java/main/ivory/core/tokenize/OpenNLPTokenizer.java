package ivory.core.tokenize;

import ivory.core.Constants;
import java.io.IOException;
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
import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;
import java.util.Map;
import java.util.HashMap;

public class OpenNLPTokenizer extends ivory.core.tokenize.Tokenizer {
  private static final Logger sLogger = Logger.getLogger(OpenNLPTokenizer.class);
  static{
    sLogger.setLevel(Level.INFO);
  }
  private Tokenizer tokenizer;
  private SnowballStemmer stemmer;
  private int lang;
  private static final int ENGLISH = 0, FRENCH = 1, GERMAN = 2;
  private static final String[] classes = {
    "org.tartarus.snowball.ext.englishStemmer", 
    "org.tartarus.snowball.ext.frenchStemmer", 
    "org.tartarus.snowball.ext.germanStemmer"};

  public OpenNLPTokenizer(){
    super();
  }

  @Override
  public void configure(Configuration conf){
    FileSystem fs;
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } 
    configure(conf, fs);
  }

  @Override
  public void configure(Configuration conf, FileSystem fs){
    setTokenizer(fs, new Path(conf.get(Constants.TokenizerData)));
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

    VocabularyWritable vocab;
    try {
      vocab = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get(Constants.CollectionVocab)), fs);
      setVocab(vocab);
    } catch (Exception e) {
      sLogger.warn("No vocabulary provided to tokenizer.");
      vocab = null;
    }
    isStopwordRemoval = !stopwords.isEmpty();

    sLogger.info("Stemmer: " + stemmer + "\nStopword removal is " + isStopwordRemoval +"; number of stopwords: " + stopwords.size() +"; stemmed: " + stemmedStopwords.size());
  }

  public void setTokenizer(FileSystem fs, Path p){
    try {
      FSDataInputStream in = fs.open(p);
      TokenizerModel model;
      model = new TokenizerModel(in);
      tokenizer = new TokenizerME(model);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("OpenNLPTokenizer model not available at " + p); 
    }
  }

  public void setLanguage(String l){
    if(l.startsWith("en")){
      lang = ENGLISH;//"english";
    }else if(l.startsWith("fr")){
      lang = FRENCH;//"french";
    }else if(l.equals("german") || l.startsWith("de")){
      lang = GERMAN;//"german";
    }else{
      sLogger.warn("Language not recognized, setting to English!");
    }
  }

  @SuppressWarnings("unchecked")
  public void setLanguageAndStemmer(String l){
    setLanguage(l);
    Class<? extends SnowballStemmer> stemClass;
    try {
      stemClass = (Class<? extends SnowballStemmer>) Class.forName(classes[lang]);
      stemmer = (SnowballStemmer) stemClass.newInstance();
    } catch (ClassNotFoundException e) {
      sLogger.warn("Stemmer class not recognized!\n" + classes[lang]);
      stemmer = null;
      return;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } 
  }
    
  @Override
  public String[] processContent(String text) {
    text = preNormalize(text);
    if ( lang == FRENCH ) {
      text = text.replaceAll("'", "' ");    // openNLP does not separate what comes after the apostrophe, which seems to work better
    }

    String[] tokens = tokenizer.tokenize(text);
    StringBuilder tokenized = new StringBuilder();
    for ( String token : tokens ){
      tokenized.append(token + " ");
    }

    // do post-normalizations before any stemming or stopword removal 
    String[] normalizedTokens = postNormalize(tokenized.toString().trim()).split(" ");
    tokenized.delete(0, tokenized.length());
    for ( int i = 0; i < normalizedTokens.length; i++ ){
      String token = normalizedTokens[i].toLowerCase();
      if ( isStopwordRemoval() && isDiscard(false, token) ) {
        //        sLogger.warn("Discarded stopword "+token);
        continue;
      }

      //apply stemming on token
      String stemmedToken = stem(token);

      //skip if out of vocab
      if ( vocab != null && vocab.get(stemmedToken) <= 0) {
        //        sLogger.warn("Discarded OOV "+token);
        continue;
      }
      tokenized.append( stemmedToken + " " );
    }

    return tokenized.toString().trim().split(" ");
  }
  
  @Override
  public Map<String, String> getStem2NonStemMapping(String text) {
    Map<String, String> stem2NonStemMapping = new HashMap<String, String>();
    text = preNormalize(text);
    if ( lang == FRENCH ) {
      text = text.replaceAll("'", "' ");    // openNLP does not separate what comes after the apostrophe, which seems to work better
    }
    
    String[] tokens = tokenizer.tokenize(text);
    StringBuilder tokenized = new StringBuilder();
    for ( String token : tokens ){
      tokenized.append(token + " ");
    }
    
    // do post-normalizations before any stemming or stopword removal
    String[] normalizedTokens = postNormalize(tokenized.toString().trim()).split(" ");
    tokenized.delete(0, tokenized.length());
    for ( int i = 0; i < normalizedTokens.length; i++ ){
      String token = normalizedTokens[i].toLowerCase();
      if ( isStopwordRemoval() && isDiscard(false, token) ) {
        //        sLogger.warn("Discarded stopword "+token);
        continue;
      }
      
      //apply stemming on token
      String stemmedToken = stem(token);
      
      //skip if out of vocab
      if ( vocab != null && vocab.get(stemmedToken) <= 0) {
        //        sLogger.warn("Discarded OOV "+token);
        continue;
      }
      stem2NonStemMapping.put(stemmedToken, token);
    }
    return stem2NonStemMapping;
  }

  @Override
  public int getNumberTokens(String string){
    return tokenizer.tokenize(string).length;
  }

  @Override
  public String stem(String token) {
    if ( stemmer != null ) {
      stemmer.setCurrent(token);
      stemmer.stem();
      return stemmer.getCurrent();
    }else {
      return token;
    }
  }

  @Override
  public float getOOVRate(String text, VocabularyWritable vocab) {
    int countOOV = 0, countAll = 0;
    text = preNormalize(text);
    String[] tokens = tokenizer.tokenize(text);
    StringBuilder tokenized = new StringBuilder();
    for ( String token : tokens ){
      tokenized.append(token + " ");
    }

    String[] normalizedTokens = postNormalize(tokenized.toString().trim()).split(" ");
    for ( int i = 0; i < normalizedTokens.length; i++ ){
      String token = normalizedTokens[i].toLowerCase();
      if ( isStopwordRemoval() && isDiscard(false, token) ) {
        continue;
      }

      //apply stemming on token
      String stemmedToken = stem(token);

      if ( vocab != null && vocab.get(stemmedToken) <= 0) {
        countOOV++;
      } 
      countAll++;
    }
    return (countOOV / (float) countAll);
  }
}
