package ivory.lsh.bitext;

import ivory.core.RetrievalEnvironment;
import ivory.core.tokenize.Tokenizer;
import ivory.core.util.CLIRUtils;
import java.io.IOException;
import java.net.URI;
import opennlp.model.RealValueFileEventStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.map.HMapSIW;

/**
  Step 2 of the bitext extraction algorithm.

 * @author ferhanture
 * 
 */
@SuppressWarnings("deprecation")
public class FilterSentencePairs extends Configured implements Tool {

  private static final Logger sLogger = Logger.getLogger(FilterSentencePairs.class);

  enum Sentences{
    parallel, ignored, dbg, OOV
  }

  private static Options options;

  public FilterSentencePairs() {
  }

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "FilterSentencePairs", options );
    System.exit(-1);    
  }

  //	Map: (eSent, fSent) --> (eSent, fSent)
  //	     (vect1, vect2) = convert sentences into tf-idf vectors
  //	     compute features for vector pair
  //		   emit pair if complex classifier confidence met
  private static class MyMapper extends MapReduceBase implements
  Mapper<LongWritable, Text, Text, Text> {

    private PreprocessHelper helper;
    private String eSent, fSent;
    private int eLen, fLen;
    private HMapSFW eVector, fVector;
    private Tokenizer eTok, fTok;
    private Text outSent1, outSent2;
    private float classifierThreshold;
    private int classifierPositiveId;
    
    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);

      try {
        helper = new PreprocessHelper(CLIRUtils.MinVectorTerms, CLIRUtils.MinSentenceLength, job);
      } catch (Exception e) {
        e.printStackTrace();
      }
      classifierThreshold = job.getFloat("ClassifierThreshold", 0.0f);
      classifierPositiveId = job.getInt("ClassifierId", -1);
      if(classifierPositiveId != 0 && classifierPositiveId != 1){
        throw new RuntimeException("Id of parallel label in MaxEnt classifier not specified properly: "+classifierPositiveId);
      }
      sLogger.info(classifierThreshold);
      eTok = helper.getETokenizer();
      fTok = helper.getFTokenizer();
      outSent1 = new Text();
      outSent2 = new Text();
    }

    public void map(LongWritable key, Text sentencePair, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String sentences[] = sentencePair.toString().split(CLIRUtils.BitextSeparator);
      if (sentences.length < 2) {
        // happens in Arabic, might be due to the right-to-left writing corrupting some pairs
        // havent figured it out yet, but negligible number of sents affected
        reporter.incrCounter(Sentences.ignored, 1);   
        return;
      }
      eSent = sentences[1];
      fSent = sentences[0];
      eLen = eTok.getNumberTokens(eSent);
      fLen = fTok.getNumberTokens(fSent);
      HMapSIW eSrcTfs = new HMapSIW();
      eVector = helper.createEDocVector(eSent, eSrcTfs);
      HMapSIW fSrcTfs = new HMapSIW();
      fVector = helper.createFDocVector(fSent, fSrcTfs);
   
      if (eVector == null || fVector == null) {
        reporter.incrCounter(Sentences.ignored, 1);	
        return;
      }

      sLogger.debug("-------------\n"+fSent+"\n"+eSent+"\n----\n"+fVector+"\n"+fSrcTfs+"\n"+eVector+"\n"+fLen+","+eLen+"\n------------");

      String[] instance = CLIRUtils.computeFeaturesF2(eSrcTfs, eVector, fSrcTfs, fVector, eLen, fLen, 
          helper.getESrc(), helper.getETrg(), helper.getFSrc(), helper.getFTrg(), helper.getE2F(), helper.getF2E(), 0.1f);
      
//      String[] instance = CLIRUtils.computeFeaturesF3(eSent, eSrcTfs, eVector, fSent, fSrcTfs, fVector, eLen, fLen, 
//          helper.getESrc(), helper.getETrg(), helper.getFSrc(), helper.getFTrg(), helper.getE2F(), helper.getF2E());
      String s ="";
      for (String feat : instance) {
        s+=feat+" ";
      }
      
      // classify w/ maxent model
      // emit if labeled parallel
      if(instance == null){
        throw new RuntimeException("SHOULD NOT HAPPEN!");
      }

      //apply MaxEnt classifier to instance
      float[] values = RealValueFileEventStream.parseContexts(instance);
      double[] probs = helper.getClassifier().eval(instance, values);

      // the index of <i>probs</i> that gives the prob. of label=parallel depends on the classifier object  
      // e.g., for the F3 de-en classifier probs[1] gives pr(parallel), in F1 classifier probs[0] does
      // we pass this information as a program argument
      double confidence = probs[classifierPositiveId];
          
      if (confidence > classifierThreshold) {
        reporter.incrCounter(Sentences.parallel, 1);
        outSent1.set(fSent + CLIRUtils.BitextSeparator + eSent + CLIRUtils.BitextSeparator + s + CLIRUtils.BitextSeparator + confidence);
        output.collect(outSent1, outSent2);
      }
    }
  }
  
  /**
   * Runs this tool.
   */

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), FilterSentencePairs.class);

    // Read commandline arguments
    CommandLine cmdline = parseArgs(args);
    if (cmdline == null) {
      printUsage();
    }
    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
    String eDir = cmdline.getOptionValue(EINDEX_OPTION);
    String fDir = cmdline.getOptionValue(FINDEX_OPTION);
    String dataDir = cmdline.getOptionValue(DATADIR_OPTION);
    String eLang = cmdline.getOptionValue(ELANG_OPTION);
    String fLang = cmdline.getOptionValue(FLANG_OPTION);
    String bitextName = cmdline.getOptionValue(BITEXTNAME_OPTION);
    float classifierThreshold = Float.parseFloat(cmdline.getOptionValue(CLASSIFIERTHRESHOLD_OPTION));
    int classifierId = Integer.parseInt(cmdline.getOptionValue(CLASSIFIERID_OPTION));

    RetrievalEnvironment eEnv = new RetrievalEnvironment(eDir, FileSystem.get(conf));

    String eSentDetect = dataDir+"/sent/"+eLang+"-sent.bin";
    String eTokenizer = dataDir+"/token/"+eLang+"-token.bin";
    String eVocabSrc = dataDir+"/"+bitextName+"/vocab."+eLang+"-"+fLang+"."+eLang;
    String eVocabTrg = dataDir+"/"+bitextName+"/vocab."+fLang+"-"+eLang+"."+eLang;
    String eStopwords = dataDir+"/token/"+eLang+".stop";
    String eStemmedStopwords = dataDir+"/token/"+eLang+".stop.stemmed";
    
    String fSentDetect = dataDir+"/sent/"+fLang+"-sent.bin";
    String fTokenizer = dataDir+"/token/"+fLang+"-token.bin";
    String fVocabSrc = dataDir+"/"+bitextName+"/vocab."+fLang+"-"+eLang+"."+fLang;
    String fVocabTrg = dataDir+"/"+bitextName+"/vocab."+eLang+"-"+fLang+"."+fLang;
    String fStopwords = dataDir+"/token/"+fLang+".stop";
    String fStemmedStopwords = dataDir+"/token/"+fLang+".stop.stemmed";
    
    String f2e_ttableFile = dataDir+"/"+bitextName+"/ttable."+fLang+"-"+eLang;
    String e2f_ttableFile = dataDir+"/"+bitextName+"/ttable."+eLang+"-"+fLang;

//    String classifierFile = dataDir+"/"+bitextName+"/classifier-complexplus."+fLang+"-"+eLang;
    String classifierFile = dataDir+"/"+bitextName+"/classifier-complex."+fLang+"-"+eLang;

//    conf.setJobName("FilterSentencePairs_" + fLang +"-" + eLang +"_F4="+classifierThreshold+"["+classifierId+"]");
    conf.setJobName("FilterSentencePairs_" + fLang +"-" + eLang +"_F3="+classifierThreshold+"["+classifierId+"]");

    conf.set("eDir", eDir);
    conf.set("fDir", fDir);
    conf.set("eLang", eLang);
    conf.set("fLang", fLang);
    conf.setFloat("ClassifierThreshold", classifierThreshold);
    conf.setInt("ClassifierId", classifierId);
    conf.set("fTokenizer", fTokenizer);
    conf.set("eTokenizer", eTokenizer);
    conf.set("eStopword", eStopwords);
    conf.set("fStopword", fStopwords);
    conf.get("eStemmedStopword", eStemmedStopwords);
    conf.get("fStemmedStopword", fStemmedStopwords);
    
    sLogger.info("caching files...");

    /////en-files

    sLogger.info("caching files...0,1,2,3,4");

    DistributedCache.addCacheFile(new URI(eEnv.getDfByTermData()), conf);
    DistributedCache.addCacheFile(new URI(eSentDetect), conf);
    DistributedCache.addCacheFile(new URI(eTokenizer), conf);
    DistributedCache.addCacheFile(new URI(eVocabSrc), conf);
    DistributedCache.addCacheFile(new URI(eVocabTrg), conf);

    /////de-files
    sLogger.info("caching files...5,6,7,8");

    //		DistributedCache.addCacheFile(new URI(fDir+"/transDf.dat"), conf);
    DistributedCache.addCacheFile(new URI(fSentDetect), conf);
    DistributedCache.addCacheFile(new URI(fTokenizer), conf);
    DistributedCache.addCacheFile(new URI(fVocabSrc), conf);
    DistributedCache.addCacheFile(new URI(fVocabTrg), conf);

    /////cross-ling files

    sLogger.info("caching files...9, 10,11,12");

    DistributedCache.addCacheFile(new URI(f2e_ttableFile), conf);
    DistributedCache.addCacheFile(new URI(e2f_ttableFile), conf);
    DistributedCache.addCacheFile(new URI(eEnv.getIndexTermsData()), conf);
    DistributedCache.addCacheFile(new URI(classifierFile), conf);

    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setInt("mapred.task.timeout", 60000000);
    conf.set("mapred.child.java.opts", "-Xmx2000m");
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    conf.setNumMapTasks(100);
    conf.setNumReduceTasks(1);
    conf.setInt("mapred.min.split.size", 2000000000);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    JobClient.runJob(conf);	
    return 0;
  }

  private static final String FLANG_OPTION = "f_lang";
  private static final String ELANG_OPTION = "e_lang";
  private static final String FINDEX_OPTION = "f_index";
  private static final String EINDEX_OPTION = "e_index";
  private static final String BITEXTNAME_OPTION = "name";
  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String DATADIR_OPTION = "data";
  private static final String PWSIM_OPTION = "pwsim_output";
  private static final String CLASSIFIERID_OPTION = "classifier_id";
  private static final String CLASSIFIERTHRESHOLD_OPTION = "threshold";
  private static final String LIBJARS_OPTION = "libjars";

  @SuppressWarnings("static-access")
  private CommandLine parseArgs(String[] args) throws Exception {
    options.addOption(OptionBuilder.withDescription("two-letter code for f-language").withArgName("en|de|tr|cs|zh|ar|es").hasArg().isRequired().create(FLANG_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter code for e-language").withArgName("en|de|tr|cs|zh|ar|es").hasArg().isRequired().create(ELANG_OPTION));
    options.addOption(OptionBuilder.withDescription("source-side index path").withArgName("path").hasArg().isRequired().create(FINDEX_OPTION));
    options.addOption(OptionBuilder.withDescription("target-side index path").withArgName("path").hasArg().isRequired().create(EINDEX_OPTION));
    options.addOption(OptionBuilder.withDescription("name of bitext").withArgName("string").hasArg().isRequired().create(BITEXTNAME_OPTION));
    options.addOption(OptionBuilder.withDescription("path to data files on HDFS").withArgName("path").hasArg().isRequired().create(DATADIR_OPTION));
    options.addOption(OptionBuilder.withDescription("path to output of pwsim algorithm").withArgName("path").hasArg().isRequired().create(PWSIM_OPTION));
    options.addOption(OptionBuilder.withDescription("classifier id to retrieve P('PARALLEL'|instance)").withArgName("0 or 1").hasArg().isRequired().create(CLASSIFIERID_OPTION));
    options.addOption(OptionBuilder.withDescription("target vocabulary (e-side) of P(e|f)").withArgName("0-1").hasArg().isRequired().create(CLASSIFIERTHRESHOLD_OPTION));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars").withArgName("jar packages").hasArg().create(LIBJARS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return null;
    }

    return cmdline;
  }

  /**
   * Dispatches command-line arguments to the tool via the
   * <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new FilterSentencePairs(), args);
    System.exit(res);
  }

}

