package ivory.core.tokenize;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.mortbay.log.Log;
import ivory.core.Constants;
import edu.umd.hooka.VocabularyWritable;

public class TokenizerFactory {
  private static final Map<String, Integer> acceptedLanguages = new HashMap<String, Integer>();
  static {
    acceptedLanguages.put("zh", 1);
    acceptedLanguages.put("en", 1);
    acceptedLanguages.put("es", 1);
    acceptedLanguages.put("ar", 1);
    acceptedLanguages.put("de", 1);
    acceptedLanguages.put("fr", 1);
    acceptedLanguages.put("tr", 1);
  }

  public static Tokenizer createTokenizer(String lang, String modelPath, VocabularyWritable vocab){
    return createTokenizer(lang, modelPath, true, true, vocab);
  }
  
  public static Tokenizer createTokenizer(String lang, String modelPath, boolean isStemming, boolean isStopword, VocabularyWritable vocab){
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      return createTokenizer(fs, lang, modelPath, isStemming, isStopword, vocab);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Tokenizer createTokenizer(FileSystem fs, String lang, String modelPath, VocabularyWritable vocab){
    return createTokenizer(fs, lang, modelPath, true, true, vocab);
  }
  
  public static Tokenizer createTokenizer(FileSystem fs, String lang, String modelPath, boolean isStemming, boolean isStopword, VocabularyWritable vocab){
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.Stemming, isStemming);
    conf.setBoolean(Constants.Stopword, isStopword);
    return createTokenizer(fs, conf, lang, modelPath, vocab);
  }
  
  public static Tokenizer createTokenizer(FileSystem fs, Configuration conf, String lang, String modelPath, boolean isStemming, boolean isStopword, VocabularyWritable vocab){
    conf.setBoolean(Constants.Stemming, isStemming);
    conf.setBoolean(Constants.Stopword, isStopword);
    return createTokenizer(fs, conf, lang, modelPath, vocab);
  }

  private static Tokenizer createTokenizer(FileSystem fs, Configuration conf, String lang, String modelPath, VocabularyWritable vocab){
    try {
      if (!acceptedLanguages.containsKey(lang)) {
        throw new RuntimeException("Unknown language code: "+lang);
      }
      
      conf.set(Constants.Language, lang);
      conf.set(Constants.TokenizerData, modelPath);
      
      Class tokenizerClass = getTokenizerClass(lang);
      Tokenizer tokenizer = (Tokenizer) tokenizerClass.newInstance();
//      if(lang.equals("zh")){
//        tokenizer = new StanfordChineseTokenizer();
//      }else if(lang.equals("de") || lang.equals("en") || lang.equals("fr")){
//        tokenizer = new OpenNLPTokenizer();
//      }else if(lang.equals("ar")){
//        tokenizer = new LuceneArabicAnalyzer();
//      }else if(lang.equals("tr")){
//        tokenizer = new LuceneTurkishAnalyzer();
//      }
      
      if (vocab != null) {
        tokenizer.setVocab(vocab);
      }
      tokenizer.configure(conf, fs);
      return tokenizer;
    } catch (Exception e) {
      e.printStackTrace();
      Log.info("Something went wrong during tokenizer creation. Language code:"+lang);
      throw new RuntimeException(e);
    }
  }

  public static Class getTokenizerClass(String lang) {
    if (lang.equals("zh")) {
      return StanfordChineseTokenizer.class;
    }else if(lang.equals("de") || lang.equals("en") || lang.equals("fr")) {
      return OpenNLPTokenizer.class;
    }else if(lang.equals("ar")) {
      return LuceneArabicAnalyzer.class;
    }else if(lang.equals("tr")) {
      return LuceneTurkishAnalyzer.class;
    }else if(lang.equals("es")) {
      return LuceneSpanishAnalyzer.class;
    }else {
      Log.info("Unknown class for language: " + lang);
      throw new RuntimeException("Unknown class for language: " + lang);
    }
  }
}
