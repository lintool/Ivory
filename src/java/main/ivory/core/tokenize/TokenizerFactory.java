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
    acceptedLanguages.put("cs", 1);
    acceptedLanguages.put("zh", 1);
    acceptedLanguages.put("en", 1);
    acceptedLanguages.put("es", 1);
    acceptedLanguages.put("ar", 1);
    acceptedLanguages.put("de", 1);
    acceptedLanguages.put("fr", 1);
    acceptedLanguages.put("tr", 1);
  }

  public static Tokenizer createTokenizer(FileSystem fs, String lang, String modelPath, boolean isStemming){
    return createTokenizer(fs, lang, modelPath, isStemming, null, null, null);
  }

  public static Tokenizer createTokenizer(String lang, String modelPath, boolean isStemming){
    return createTokenizer(lang, null, isStemming, null);
  }

  public static Tokenizer createTokenizer(String lang, boolean isStemming, VocabularyWritable vocab){
    return createTokenizer(lang, null, isStemming, vocab);
  }

  public static Tokenizer createTokenizer(String lang, String modelPath, boolean isStemming, VocabularyWritable vocab){
    return createTokenizer(lang, modelPath, isStemming, null, null, vocab);
  }

  public static Tokenizer createTokenizer(String lang, String modelPath, boolean isStemming, String stopwordFile, String stemmedStopwordFile, VocabularyWritable vocab){
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      return createTokenizer(fs, lang, modelPath, isStemming, stopwordFile, stemmedStopwordFile, vocab);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Tokenizer createTokenizer(FileSystem fs, String lang, String modelPath, boolean isStemming, String stopwordFile, String stemmedStopwordFile, VocabularyWritable vocab){
    Configuration conf = new Configuration();
    return createTokenizer(fs, conf, lang, modelPath, isStemming, stopwordFile, stemmedStopwordFile, vocab);
  }

  public static Tokenizer createTokenizer(FileSystem fs, Configuration conf, String lang, String modelPath, boolean isStemming, String stopwordFile, String stemmedStopwordFile, VocabularyWritable vocab){
    conf.setBoolean(Constants.Stemming, isStemming);
    if (stopwordFile != null) {
      conf.set(Constants.StopwordList, stopwordFile);
    }
    if (stemmedStopwordFile != null) {
      conf.set(Constants.StemmedStopwordList, stemmedStopwordFile);
    }
    return createTokenizer(fs, conf, lang, modelPath, vocab);
  }

  public static Tokenizer createTokenizer(FileSystem fs, Configuration conf, String lang, String modelPath, VocabularyWritable vocab){
    try {
      if (!acceptedLanguages.containsKey(lang)) {
        throw new RuntimeException("Unknown language code: "+lang);
      }

      conf.set(Constants.Language, lang);
      if (modelPath != null) {
        conf.set(Constants.TokenizerData, modelPath);
      }

      Class<? extends Tokenizer> tokenizerClass = getTokenizerClass(lang, modelPath);
      Tokenizer tokenizer = (Tokenizer) tokenizerClass.newInstance();

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

  public static Class<? extends Tokenizer> getTokenizerClass(String lang, String modelPath) {
    if (lang.equals("zh")) {
      return StanfordChineseTokenizer.class;
    }else if(lang.equals("de") || lang.equals("fr")) {
      return OpenNLPTokenizer.class;
    }else if(lang.equals("ar")) {
      return LuceneArabicAnalyzer.class;
    }else if(lang.equals("tr") || lang.equals("es") || lang.equals("cs")) {
      return LuceneAnalyzer.class;
    }else if(lang.equals("en")) {
      if (modelPath == null) {
        return GalagoTokenizer.class;
      }else {
        return OpenNLPTokenizer.class;
      }
    }else {
      Log.info("Unknown class for language: " + lang);
      throw new RuntimeException("Unknown class for language: " + lang);
    }
  }
}
