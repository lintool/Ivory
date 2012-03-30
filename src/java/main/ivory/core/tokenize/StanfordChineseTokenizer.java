package ivory.core.tokenize;

import ivory.core.Constants;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.sequences.DocumentReaderAndWriter;

public class StanfordChineseTokenizer extends Tokenizer {
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
    props.setProperty("sighanCorporaDict", conf.get(Constants.TokenizerData));		//data
    props.setProperty("serDictionary",conf.get(Constants.TokenizerData)+"/dict-chris6.ser");//"data/dict-chris6.ser.gz");
    props.setProperty("inputEncoding", "UTF-8");
    props.setProperty("sighanPostProcessing", "true");

    try {
      FileSystem fs = FileSystem.get(conf);		
      classifier = new CRFClassifier(props);
      FSDataInputStream in = fs.open(new Path(conf.get(Constants.TokenizerData)+"/pku"));
      FSDataInputStream inDict = fs.open(new Path(conf.get(Constants.TokenizerData)+"/dict-chris6.ser"));
      classifier.loadClassifier(in, props);			//data/pku.gz
      classifier.flags.setConf(conf);
      readerWriter = classifier.makeReaderAndWriter(inDict);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void configure(Configuration conf, FileSystem fs) {
    Properties props = new Properties();
    props.setProperty("sighanCorporaDict", conf.get(Constants.TokenizerData));		//data
    props.setProperty("serDictionary",conf.get(Constants.TokenizerData)+"/dict-chris6.ser");//"data/dict-chris6.ser.gz");
    props.setProperty("inputEncoding", "UTF-8");
    props.setProperty("sighanPostProcessing", "true");

    try {
      classifier = new CRFClassifier(props);
      FSDataInputStream in = fs.open(new Path(conf.get(Constants.TokenizerData)+"/pku"));
      FSDataInputStream inDict = fs.open(new Path(conf.get(Constants.TokenizerData)+"/dict-chris6.ser"));
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
      text = text.toLowerCase();
      tokens = classifier.classifyStringAndReturnAnswers(text, readerWriter);
    } catch (IOException e) {
      sLogger.info("Problem in tokenizing Chinese");
      e.printStackTrace();
    }
    return tokens; 
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException{
    if(args.length < 4){
      System.err.println("usage: [input] [language] [tokenizer-model-path] [output-file]");
      System.exit(-1);
    }
    ivory.core.tokenize.Tokenizer tokenizer = TokenizerFactory.createTokenizer(args[1], args[2], null);
    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[3]), "UTF8"));
    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(args[0]), "UTF8"));

    //	  DataInput in = new DataInputStream(new BufferedInputStream(FileSystem.getLocal(new Configuration()).open(new Path(args[0]))));
    String line = null;
    while((line = in.readLine()) != null){
      String[] tokens = tokenizer.processContent(line);
      String s = "";
      for (String token : tokens) {
        s += token+" ";
      }
      out.write(s+"\n");
    }
    out.close();
  }

}