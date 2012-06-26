package ivory.core.tokenize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.mortbay.log.Log;
import ivory.core.Constants;
import edu.umd.hooka.VocabularyWritable;

public class TokenizerFactory {
  public static Tokenizer createTokenizer(String lang, String modelPath, VocabularyWritable vocab){
    return createTokenizer(lang, modelPath, true, true, vocab);
  }
  
  public static Tokenizer createTokenizer(String lang, String modelPath, boolean isStemming, boolean isStopword, VocabularyWritable vocab){
    Configuration conf = new Configuration();
    try {
      if(lang.equals("zh")){
        StanfordChineseTokenizer fTok = new StanfordChineseTokenizer();
        conf.set(Constants.TokenizerData, modelPath);		//can't use tokenizer file because it points to local path, which isn't supported in StanfordTokenizer at the moment
        fTok.configure(conf);
        return fTok;
      }else if(lang.equals("de") || lang.equals("en") || lang.equals("fr")){
        OpenNLPTokenizer fTok = new OpenNLPTokenizer();
        conf.set(Constants.Language, lang);
        conf.set(Constants.TokenizerData, modelPath);
        conf.setBoolean("IsStemming", isStemming);
        conf.setBoolean("IsStopword", isStopword);
        fTok.configure(conf);
        if(vocab!=null){
          fTok.setVocab(vocab);	
        }

        return fTok;
      }else{
        throw new RuntimeException("Unknown language code: "+lang);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static Tokenizer createTokenizer(FileSystem fs, String lang, String modelPath, VocabularyWritable vocab){
    return createTokenizer(fs, lang, modelPath, true, true, vocab);
  }
  
  public static Tokenizer createTokenizer(FileSystem fs, String lang, String modelPath, boolean isStemming, boolean isStopword, VocabularyWritable vocab){
    Configuration conf = new Configuration();
    try {
      if(lang.equals("zh")){
        StanfordChineseTokenizer fTok = new StanfordChineseTokenizer();
        conf.set(Constants.TokenizerData, modelPath);		//can't use tokenizer file because it points to local path, which isn't supported in StanfordTokenizer at the moment
        fTok.configure(conf, fs);
        return fTok;
      }else if(lang.equals("de") || lang.equals("en") || lang.equals("fr")){
        OpenNLPTokenizer fTok = new OpenNLPTokenizer();
        conf.set(Constants.Language, lang);
        conf.set(Constants.TokenizerData, modelPath);
        conf.setBoolean("IsStemming", isStemming);
        conf.setBoolean("IsStopword", isStopword);
        fTok.configure(conf, fs);
        if (vocab != null) {
          fTok.setVocab(vocab);	
        }

        return fTok;
      }else{
        throw new RuntimeException("Unknown language code: "+lang);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Log.info("Language code:"+lang);
      throw new RuntimeException(e);
    }
  }

  public static Tokenizer createTokenizer(FileSystem fs, Configuration conf, String lang, String modelPath, VocabularyWritable vocab){
    try {
      if(lang.equals("zh")){
        StanfordChineseTokenizer fTok = new StanfordChineseTokenizer();
        conf.set(Constants.TokenizerData, modelPath);		//can't use tokenizer file because it points to local path, which isn't supported in StanfordTokenizer at the moment
        fTok.configure(conf, fs);
        return fTok;
      }else if(lang.equals("de") || lang.equals("en") || lang.equals("fr")){
        OpenNLPTokenizer fTok = new OpenNLPTokenizer();
        conf.set(Constants.Language, lang);
        conf.set(Constants.TokenizerData, modelPath);
        fTok.configure(conf, fs);
        if(vocab!=null){
          fTok.setVocab(vocab);	
        }
        return fTok;
      }else{
        throw new RuntimeException("Unknown language code: "+lang);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

}
