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

import edu.umd.clip.mt.VocabularyWritable;

public class OpenNLPTokenizer implements ivory.tokenize.Tokenizer {
	private static final Logger sLogger = Logger.getLogger(DocumentProcessingUtils.class);
	static{
		sLogger.setLevel(Level.WARN);
	}
	Tokenizer tokenizer;
	SnowballStemmer stemmer;
	String lang;
//	boolean isWiki;
	protected static int NUM_PREDS, MIN_LENGTH = 2, MAX_LENGTH = 50;
	String delims = "`~!@#$%^&*()-_=+]}[{\\|'\";:/?.>,<";
	VocabularyWritable vocab;

	public OpenNLPTokenizer(){
		super();
//		isWiki = false;
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
//		setIsWiki(mJobConf.getBoolean("Ivory.IsWiki", true));
		VocabularyWritable vocab;
		try {
			vocab = (VocabularyWritable) CLIRUtils.loadVocab(new Path(mJobConf.get("Ivory.CollectionVocab")), fs);
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
					l + "Stemmer");
			stemmer = (SnowballStemmer) stemClass.newInstance();
		} catch (ClassNotFoundException e) {
			sLogger.warn("Stemmer class not recognized!");
			stemmer = null;
			return;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} 
	}

//	public void setIsWiki(boolean b){
//		isWiki = b;
//	}

	public void setVocab(VocabularyWritable v){
		vocab = v;
	}

	public String[] processContent(String text) {
//		if(isWiki){
//			text = WikipediaPage.parseAndCleanPage(text).trim();
//		}
		String[] tokens = tokenizer.tokenize(text);
		List<String> stemmedTokens = new ArrayList<String>();
		for(String token : tokens){
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
		VocabularyWritable engVocabH = (VocabularyWritable) CLIRUtils.loadVocab(new Path(srcVocabFile), localFs);


		eTokenizer.setVocab(engVocabH);
		String[] ss = eTokenizer.processContent(s);
		System.out.println();
	}


	//	/**
	//	 * Following code in opennlp.tools.lang.english.Tokenizer, I copied code from opennlp.maxent.io.GISModelReader to suit reading from HDFS.
	//	 * @param fs
	//	 * @param path
	//	 * @return
	//	 */
	//	public static GISModel createMaxentModel(FileSystem fs, Path path) {
	//		GISModel model = null;
	//		try {
	//			FSDataInputStream in = fs.open(path);
	//			String modelType = in.readUTF();
	//			if (!modelType.equals("GIS"))
	//				System.out.println("Error: attempting to load a "+modelType+
	//						" model as a GIS model."+
	//				" You should expect problems.");
	//
	//			int correctionConstant = in.readInt();
	//			double correctionParam = in.readDouble();
	//			int numOutcomes = in.readInt();
	//			String[] outcomeLabels = new String[numOutcomes];
	//			for (int i=0; i<numOutcomes; i++) outcomeLabels[i] = in.readUTF();
	//			int numOCTypes =  in.readInt();
	//			int[][] outcomePatterns = new int[numOCTypes][];
	//			for (int i=0; i<numOCTypes; i++) {
	//				StringTokenizer tok = new StringTokenizer( in.readUTF(), " ");
	//				int[] infoInts = new int[tok.countTokens()];
	//				for (int j = 0; tok.hasMoreTokens(); j++) {
	//					infoInts[j] = Integer.parseInt(tok.nextToken());
	//				}
	//				outcomePatterns[i] = infoInts;
	//			}
	//			NUM_PREDS = in.readInt();
	//			String[] predLabels = new String[NUM_PREDS];
	//			for (int i=0; i<NUM_PREDS; i++)
	//				predLabels[i] = in.readUTF();
	//			Context[] params = new Context[NUM_PREDS];
	//			int pid=0;
	//			for (int i=0; i<outcomePatterns.length; i++) {
	//				//construct outcome pattern
	//				int[] outcomePattern = new int[outcomePatterns[i].length-1];
	//				for (int k=1; k<outcomePatterns[i].length; k++) {
	//					outcomePattern[k-1] = outcomePatterns[i][k];
	//				}
	//				//populate parameters for each context which uses this outcome pattern. 
	//				for (int j=0; j<outcomePatterns[i][0]; j++) {
	//					double[] contextParameters = new double[outcomePatterns[i].length-1];
	//					for (int k=1; k<outcomePatterns[i].length; k++) {
	//						contextParameters[k-1] = in.readDouble();
	//					}
	//					params[pid] = new Context(outcomePattern,contextParameters);
	//					pid++;
	//				}
	//			}
	//			model =  new GISModel(params,
	//					predLabels,
	//					outcomeLabels,
	//					correctionConstant,
	//					correctionParam);
	//		} catch (IOException e) {
	//			e.printStackTrace();
	//		}	
	//		return model;
	//	}

}