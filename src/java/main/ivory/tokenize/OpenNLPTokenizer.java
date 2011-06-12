package ivory.tokenize;

import ivory.util.CLIRUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class OpenNLPTokenizer implements ivory.tokenize.Tokenizer {
	private static final Logger sLogger = Logger.getLogger(DocumentProcessingUtils.class);
	static{
		sLogger.setLevel(Level.WARN);
	}
	Tokenizer tokenizer;
	SnowballStemmer stemmer;
	String lang;
	protected static int NUM_PREDS, MIN_LENGTH = 2, MAX_LENGTH = 50;
	String delims = "`~!@#$%^&*()-_=+]}[{\\|'\";:/?.>,<";
	VocabularyWritable vocab;

	public OpenNLPTokenizer(){
		super();
	}

	public void configure(Configuration mJobConf){
		FileSystem fs;
		try {
			fs = FileSystem.get(mJobConf);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} 
		setTokenizer(fs, new Path(mJobConf.get("Ivory.TokenizerModel")));
		setLanguageAndStemmer(mJobConf.get("Ivory.Lang"));
		VocabularyWritable vocab;
		try {
			vocab = (VocabularyWritable) HadoopAlign.loadVocab(new Path(mJobConf.get("Ivory.CollectionVocab")), fs);
			setVocab(vocab);
		} catch (IOException e) {
			sLogger.warn("VOCAB IS NULL!");
			vocab = null;
		}
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

	public void setLanguageAndStemmer(String l){
		if(l.startsWith("en")){
			lang = "english";
		}else if(l.startsWith("fr")){
			lang = "french";
		}else if(l.equals("german") || l.startsWith("de")){
			lang = "german";
		}else{
			sLogger.warn("Language not recognized!");
		}
		Class stemClass;
		try {
			stemClass = Class.forName("org.tartarus.snowball.ext." +
					lang + "Stemmer");
			stemmer = (SnowballStemmer) stemClass.newInstance();
		} catch (ClassNotFoundException e) {
			sLogger.warn("Stemmer class not recognized!\n"+"org.tartarus.snowball.ext." +
					lang + "Stemmer");
			stemmer = null;
			return;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} 
	}

	public void setVocab(VocabularyWritable v){
		vocab = v;
	}

	public String[] processContent(String text) {
		String[] tokens = tokenizer.tokenize(text);
		List<String> stemmedTokens = new ArrayList<String>();
		for(String token : tokens){
			token = removeNonUnicodeChars(token);

			if(token.length() < MIN_LENGTH || token.length() > MAX_LENGTH || delims.contains(token))	continue;
			String stemmed = token;
			if(stemmer!=null){
				stemmer.setCurrent(token);
				stemmer.stem();
				stemmed = stemmer.getCurrent().toLowerCase();
			}
			//skip if out of vocab
			if(vocab!=null && vocab.get(stemmed)<=0){
				continue;
			}
			stemmedTokens.add(stemmed);
		}

		String[] tokensArray = new String[stemmedTokens.size()];
		int i=0;
		for(String ss : stemmedTokens){
			tokensArray[i++]=ss;
		}
		return tokensArray;

	}
	
	public String processContent2(String text) {
		String[] tokens = tokenizer.tokenize(text);
		String line = "";
		for(String token : tokens){
			token = removeNonUnicodeChars(token);
			String stemmed = token;
			if(stemmer!=null){
				stemmer.setCurrent(token);
				stemmer.stem();
				stemmed = stemmer.getCurrent().toLowerCase();
			}
			if(!stemmed.isEmpty()){
				line += stemmed+" ";
			}
		}
		return line;
	}
	
	private String removeNonUnicodeChars(String token) {
		StringBuffer fixedToken = new StringBuffer();
		for(int i=0; i<token.length(); i++){
			char c = token.charAt(i);
			if(Character.getNumericValue(c)>=0){
				fixedToken.append(c);
			}
		}
		return fixedToken.toString();
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException{
		String s ="965 v. Chr.\n#redirect [[10. Jahrhundert v. Chr.]]";
		//		OpenNLPTokenizer eTokenizer = new OpenNLPTokenizer();
		//		eTokenizer.setTokenizer(FileSystem.get(new Configuration()), new Path("/Users/ferhanture/edu/research/programs/opennlp-tools-1.4.3/models/EnglishTok.bin"));
		//		eTokenizer.setLanguageAndStemmer("english");
		//		eTokenizer.setIsWiki(true);
		//		FileSystem localFs=null;
		//		try {
		//			localFs = FileSystem.get(new Configuration());
		//		} catch (IOException e2) {
		//		}
		//		String indexPath = "/Users/ferhanture/edu/research/data/de-en/eu-nc-wmt08";
		//		String srcVocabFile = indexPath+"/berkeleyaligner.vocab.eng";
		//		VocabularyWritable engVocabH = (VocabularyWritable) HadoopAlign.loadVocab(new Path(srcVocabFile), localFs);
		//
		//
		//		eTokenizer.setVocab(engVocabH);
		//		String[] ss = eTokenizer.processContent(s);
		//		System.out.println();

		OpenNLPTokenizer eTokenizer = new OpenNLPTokenizer();
		eTokenizer.setTokenizer(FileSystem.get(new Configuration()), new Path("/Users/ferhanture/edu/research/programs/opennlp-tools-1.4.3/models/GermanTok.bin"));
		eTokenizer.setLanguageAndStemmer("german");
//		eTokenizer.setIsWiki(true);
		FileSystem localFs=null;
		try {
			localFs = FileSystem.get(new Configuration());
		} catch (IOException e2) {
		}
		String indexPath = "/Users/ferhanture/edu/research/data/de-en/eu-nc-wmt08";
		String srcVocabFile = indexPath+"/berkeleyaligner.vocab.ger";
		VocabularyWritable engVocabH = (VocabularyWritable) HadoopAlign.loadVocab(new Path(srcVocabFile), localFs);


		eTokenizer.setVocab(engVocabH);
		String[] ss = eTokenizer.processContent(s);
		System.out.println();
	}
}