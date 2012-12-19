package ivory.core.tokenize;

import ivory.core.Constants;

import java.io.IOException;
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
import org.mortbay.log.Log;
import org.tartarus.snowball.SnowballStemmer;

import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;

public class OpenNLPTokenizer extends ivory.core.tokenize.Tokenizer {
  private static final Logger sLogger = Logger.getLogger(OpenNLPTokenizer.class);
  static{
    sLogger.setLevel(Level.WARN);
  }
  private Tokenizer tokenizer;
  private SnowballStemmer stemmer;
  private int lang;
  private static final int ENGLISH = 0, FRENCH = 1, GERMAN = 2;
  private static final String[] languages = {"english", "french", "german"};
  private Set<String> stopwords;
  private Set<String> stemmedStopwords;

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
  public void configure(Configuration mJobConf, FileSystem fs){
    setTokenizer(fs, new Path(mJobConf.get(Constants.TokenizerData)));
    if (mJobConf.getBoolean(Constants.Stemming, true)) {
      setLanguageAndStemmer(mJobConf.get(Constants.Language));
    }else {
      setLanguage(mJobConf.get(Constants.Language));
    }

    // read stopwords from file (stopwords will be empty set if file does not exist or is empty)
    String stopwordsFile = mJobConf.get(Constants.StopwordList);
    stopwords = readInput(fs, stopwordsFile);      
    String stemmedStopwordsFile = mJobConf.get(Constants.StemmedStopwordList);
    stemmedStopwords = readInput(fs, stemmedStopwordsFile);

    VocabularyWritable vocab;
    try {
      vocab = (VocabularyWritable) HadoopAlign.loadVocab(new Path(mJobConf.get(Constants.CollectionVocab)), fs);
      setVocab(vocab);
    } catch (Exception e) {
      sLogger.warn("No vocabulary provided to tokenizer.");
      vocab = null;
    }
    isStopwordRemoval = !stopwords.isEmpty();
  
    sLogger.warn("Stemmer: " + stemmer + "\nStopword removal is " + isStopwordRemoval +"; number of stopwords: " + stopwords.size() +"; stemmed: " + stemmedStopwords.size());
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
    if(l.startsWith("en")){
      lang = ENGLISH;//"english";
    }else if(l.startsWith("fr")){
      lang = FRENCH;//"french";
    }else if(l.equals("german") || l.startsWith("de")){
      lang = GERMAN;//"german";
    }else{
      sLogger.warn("Language not recognized, setting to English!");
    }
    Class<? extends SnowballStemmer> stemClass;
    try {
      stemClass = (Class<? extends SnowballStemmer>)
          Class.forName("org.tartarus.snowball.ext." + languages[lang] + "Stemmer");
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

  @Override
  public String[] processContent(String text) {
    text = preNormalize(text);
    if ( lang == FRENCH ) {
      text = text.replaceAll("'", "' ");    // openNLP does not separate what comes after the apostrophe, which seems to work better
    }

    String[] tokens = tokenizer.tokenize(text);
    String tokenizedText = "";
    for ( String token : tokens ){
      tokenizedText += token + " ";
    }

    // do post-normalizations before any stemming or stopword removal 
    String[] normalizedTokens = postNormalize(tokenizedText).split(" ");
    tokenizedText = "";
    for ( int i = 0; i < normalizedTokens.length; i++ ){
      String token = normalizedTokens[i].toLowerCase();
      if ( isStopwordRemoval && isDiscard(token) ) {
//        sLogger.warn("Discarded stopword "+token);
        continue;
      }

      //apply stemming on token
      String stemmedToken = token;
      if ( stemmer!=null ) {
        stemmer.setCurrent(token);
        stemmer.stem();
        stemmedToken = stemmer.getCurrent();
      }

      //skip if out of vocab
      if ( vocab != null && vocab.get(stemmedToken) <= 0) {
        //        sLogger.warn("Discarded OOV "+token);
        continue;
      }
      tokenizedText += (stemmedToken + " ");
    }

    return tokenizedText.trim().split(" ");
  }

  public String getLanguage() {
    return languages[lang];
  }

  @Override
  public int getNumberTokens(String string){
    return tokenizer.tokenize(string).length;
  }

  private boolean isDiscard(String token) {
    // remove characters that may cause problems when processing further
    //    token = removeNonUnicodeChars(token);

    return ( token.length() < MIN_LENGTH || token.length() > MAX_LENGTH || delims.contains(token) || stopwords.contains(token) );
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

  /* 
   * For external use. returns true if token is a Galago stopword or a delimiter: `~!@#$%^&*()-_=+]}[{\\|'\";:/?.>,<
   */
  @Override
  public boolean isStopWord(String token) {
    if (stopwords == null) {
      Log.warn("Tokenizer does not have stopwords loaded!");
      return false;
    }else {
      return ( stopwords.contains(token) || delims.contains(token) );
    }
  }

  @Override
  public boolean isStemmedStopWord(String token) {
    if (stemmedStopwords == null) {
      Log.warn("Tokenizer does not have stopwords loaded!");
      return false;
    }else {
      return ( stemmedStopwords.contains(token) || delims.contains(token) );
    }
  }

}
