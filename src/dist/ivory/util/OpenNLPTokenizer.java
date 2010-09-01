package ivory.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.tartarus.snowball.SnowballStemmer;
import edu.umd.clip.mt.VocabularyWritable;
import edu.umd.clip.mt.alignment.HadoopAlign;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import opennlp.maxent.Context;
import opennlp.maxent.GISModel;
import opennlp.tools.lang.german.Tokenizer;

public class OpenNLPTokenizer implements ivory.util.Tokenizer {
	Tokenizer tokenizer;
	SnowballStemmer stemmer;
	String lang;
	boolean isWiki;
	protected static int NUM_PREDS;
	VocabularyWritable vocab;

	public OpenNLPTokenizer(){
		super();
		isWiki = false;
	}

	public void configure(Configuration mJobConf){
		Path[] localFiles;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(mJobConf);
			setTokenizer(FileSystem.getLocal(mJobConf), localFiles[1]);
			setLanguageAndStemmer(mJobConf.get("Ivory.Lang"));
			setIsWiki(mJobConf.getBoolean("Ivory.IsWiki", true));
			VocabularyWritable vocab = (VocabularyWritable) HadoopAlign.loadVocab(localFiles[2], FileSystem.getLocal(mJobConf));
			setVocab(vocab);
		} catch (Exception e) {
			throw new RuntimeException("Local cache files not read properly.");
		}
	}
	
	public void setTokenizer(FileSystem fs, Path p){
		try {
			GISModel m = createMaxentModel(fs, p);
			tokenizer = new Tokenizer(m);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setLanguageAndStemmer(String l) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		lang = l;
		Class stemClass;
		stemClass = Class.forName("org.tartarus.snowball.ext." +
				l + "Stemmer");
		stemmer = (SnowballStemmer) stemClass.newInstance();
	}

	public void setIsWiki(boolean b){
		isWiki = b;
	}

	public void setVocab(VocabularyWritable v){
		vocab = v;
	}

	public String[] processContent(String text) {
		if(isWiki){
			text = WikipediaPage.parseAndCleanPage(text).trim();
		}
		String[] tokens = tokenizer.tokenize(text);
		List<String> stemmedTokens = new ArrayList<String>();
		for(String token : tokens){
			String stemmed = token;
			if(stemmer!=null){
				stemmer.setCurrent(token);
				stemmer.stem();
				stemmed = stemmer.getCurrent().toLowerCase();
			}
			if(vocab!=null && vocab.get(stemmed)==-1){
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
		eTokenizer.setIsWiki(true);
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

	public static GISModel createMaxentModel(FileSystem fs, Path path) {
		GISModel model = null;
		try {
			FSDataInputStream in = fs.open(path);
			String modelType = in.readUTF();
			if (!modelType.equals("GIS"))
				System.out.println("Error: attempting to load a "+modelType+
						" model as a GIS model."+
				" You should expect problems.");
			int correctionConstant = in.readInt();
			double correctionParam = in.readDouble();
			int numOutcomes = in.readInt();
			String[] outcomeLabels = new String[numOutcomes];
			for (int i=0; i<numOutcomes; i++) outcomeLabels[i] = in.readUTF();
			int numOCTypes =  in.readInt();
			int[][] outcomePatterns = new int[numOCTypes][];
			for (int i=0; i<numOCTypes; i++) {
				StringTokenizer tok = new StringTokenizer( in.readUTF(), " ");
				int[] infoInts = new int[tok.countTokens()];
				for (int j = 0; tok.hasMoreTokens(); j++) {
					infoInts[j] = Integer.parseInt(tok.nextToken());
				}
				outcomePatterns[i] = infoInts;
			}
			NUM_PREDS = in.readInt();
			String[] predLabels = new String[NUM_PREDS];
			for (int i=0; i<NUM_PREDS; i++)
				predLabels[i] = in.readUTF();
			Context[] params = new Context[NUM_PREDS];
			int pid=0;
			for (int i=0; i<outcomePatterns.length; i++) {
				//construct outcome pattern
				int[] outcomePattern = new int[outcomePatterns[i].length-1];
				for (int k=1; k<outcomePatterns[i].length; k++) {
					outcomePattern[k-1] = outcomePatterns[i][k];
				}
				//populate parameters for each context which uses this outcome pattern. 
				for (int j=0; j<outcomePatterns[i][0]; j++) {
					double[] contextParameters = new double[outcomePatterns[i].length-1];
					for (int k=1; k<outcomePatterns[i].length; k++) {
						contextParameters[k-1] = in.readDouble();
					}
					params[pid] = new Context(outcomePattern,contextParameters);
					pid++;
				}
			}
			model =  new GISModel(params,
					predLabels,
					outcomeLabels,
					correctionConstant,
					correctionParam);
		} catch (IOException e) {
			e.printStackTrace();
		}	
		return model;
	}

}