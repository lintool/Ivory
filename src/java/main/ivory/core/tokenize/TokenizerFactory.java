package ivory.core.tokenize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import edu.umd.hooka.VocabularyWritable;

public class TokenizerFactory {
	public static Tokenizer createTokenizer(String lang, String modelPath, VocabularyWritable vocab){
		Configuration conf = new Configuration();
		try {
			if(lang.equals("zh")){
				StanfordChineseTokenizer fTok = new StanfordChineseTokenizer();
				conf.set("Ivory.TokenizerModel", modelPath);		//can't use tokenizer file because it points to local path, which isn't supported in StanfordTokenizer at the moment
				fTok.configure(conf);
				return fTok;
			}else if(lang.equals("de") || lang.equals("en")){
				OpenNLPTokenizer fTok = new OpenNLPTokenizer();
				conf.set("Ivory.Lang", lang);
				conf.set("Ivory.TokenizerModel", modelPath);
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
		Configuration conf = new Configuration();
		try {
			if(lang.equals("zh")){
				StanfordChineseTokenizer fTok = new StanfordChineseTokenizer();
				conf.set("Ivory.TokenizerModel", modelPath);		//can't use tokenizer file because it points to local path, which isn't supported in StanfordTokenizer at the moment
				fTok.configure(conf, fs);
				return fTok;
			}else if(lang.equals("de") || lang.equals("en")){
				OpenNLPTokenizer fTok = new OpenNLPTokenizer();
				conf.set("Ivory.Lang", lang);
				conf.set("Ivory.TokenizerModel", modelPath);
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
	
	public static Tokenizer createTokenizer(FileSystem fs, Configuration conf, String lang, String modelPath, VocabularyWritable vocab){
		try {
			if(lang.equals("zh")){
				StanfordChineseTokenizer fTok = new StanfordChineseTokenizer();
				conf.set("Ivory.TokenizerModel", modelPath);		//can't use tokenizer file because it points to local path, which isn't supported in StanfordTokenizer at the moment
				fTok.configure(conf, fs);
				return fTok;
			}else if(lang.equals("de") || lang.equals("en")){
				OpenNLPTokenizer fTok = new OpenNLPTokenizer();
				conf.set("Ivory.Lang", lang);
				conf.set("Ivory.TokenizerModel", modelPath);
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
