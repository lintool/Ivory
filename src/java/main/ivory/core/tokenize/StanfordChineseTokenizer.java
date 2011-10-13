package ivory.core.tokenize;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.sequences.DocumentReaderAndWriter;

public class StanfordChineseTokenizer extends ivory.tokenize.Tokenizer {
	private static final Logger sLogger = Logger.getLogger(StanfordChineseTokenizer.class);
	static{
		sLogger.setLevel(Level.INFO);
	}
	@SuppressWarnings("unchecked")
	CRFClassifier classifier;
	@SuppressWarnings("unchecked")
	DocumentReaderAndWriter readerWriter;

	public StanfordChineseTokenizer(){
		super();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Configuration conf) {
		Properties props = new Properties();
		props.setProperty("sighanCorporaDict", conf.get("Ivory.TokenizerModel"));		//data
		props.setProperty("serDictionary",conf.get("Ivory.TokenizerModel")+"/dict-chris6.ser");//"data/dict-chris6.ser.gz");
		props.setProperty("inputEncoding", "UTF-8");
		props.setProperty("sighanPostProcessing", "true");

		try {
			FileSystem fs = FileSystem.get(conf);		
			classifier = new CRFClassifier(props);
			FSDataInputStream in = fs.open(new Path(conf.get("Ivory.TokenizerModel")+"/pku"));
			FSDataInputStream inDict = fs.open(new Path(conf.get("Ivory.TokenizerModel")+"/dict-chris6.ser"));
			sLogger.info("STR "+in.available());
			classifier.loadClassifier(in, props);			//data/pku.gz
			classifier.flags.setConf(conf);
			readerWriter = classifier.makeReaderAndWriter(inDict);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String[] processContent(String text) {
		String[] tokens = null;
		try {
			tokens = classifier.classifyStringAndReturnAnswers(text, readerWriter);
		} catch (IOException e) {
			sLogger.info("Problem in tokenizing Chinese");
			e.printStackTrace();
		}
		return tokens; 
	}

}