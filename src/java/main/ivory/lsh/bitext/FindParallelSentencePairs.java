package ivory.lsh.bitext;

import ivory.core.util.CLIRUtils;
import ivory.lsh.data.WikiSentenceInfo;
import ivory.lsh.driver.PwsimEnvironment;
import java.io.IOException;
import java.util.Iterator;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.map.HMapIV;

/**
  Step 1 of the bitext extraction algorithm.

 * @author ferhanture
 * 
 */
@SuppressWarnings("deprecation")
public class FindParallelSentencePairs extends Configured implements Tool {

  private static final Logger sLogger = Logger.getLogger(FindParallelSentencePairs.class);

  enum Docs{
    pairsE, pairsF, pairs, pairsIncompleteF, pairsIncompleteE, dbg
  }
  enum Sentences{
    E, F, pairsE, pairsF, pairsProcessed, pairsCandidate, pairsFilteredByVectorSize, pairsFilteredBySentRatio, parallel 
  }

  //AssertTrue
  //pairsCandidate=sum(pairsProcessed, pairsFilteredBySentRatio)

  //SanityCheck
  //pairsCandidate/Docs.pairsF = number of sentence pairs per doc pair 

  public FindParallelSentencePairs() {
  }

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "FindParallelSentencePairs", options );
    System.exit(-1);    
  }

  /**
   * Candidate generation
   * 
   *  Map: (docno, wikiPage) --> (<fDocno, fSentID, eDocno, eSentID>, <lang id,vector,sentence>)
   *  input is union of source and target collections
   *    sentences = extract sentences in wikiPage
   *    vectors = convert sentence text into td-idf vector
   *    similar_pairs = from pwsim output, find if there's any pair corresponding to docno
   *    foreach similar_pair
   *      emit(similar_pair, <lang id,docno,vectors,sentences>)
   * 
   * @author ferhanture
   */
  private static class MyMapper extends MapReduceBase implements
  Mapper<PairOfInts, WikiSentenceInfo, PairOfInts, WikiSentenceInfo> {

    private HMapIV<ArrayListOfIntsWritable> pwsimMapping;   // mapping for pwsim pairs
    private PairOfInts keyOut;
    private JobConf mJob;
    private ArrayListOfIntsWritable similarDocnos;

    public void configure(JobConf job) {
      //      sLogger.setLevel(Level.DEBUG);
      mJob = job;
      pwsimMapping = new HMapIV<ArrayListOfIntsWritable>();
      keyOut = new PairOfInts();
    }

    private static String getFilename(String s) {
      return s.substring(s.lastIndexOf("/") + 1);
    }
    
    /**
     * if lang id points to foreign language, then load pwsim algorithm's output as mapping: {foreign docno N --> list<english docnos> associated with N}
     * otherwise, mapping is like:  {english docno N --> list<foreign docnos> associated with N}
     * 
     * lang id is the same for every Map call of a given mapper, since input sequence files will be uniform in terms of language 
     * (i.e., a mapper will receive either all foreign or all english documents)
     * 
     * @param pwsimMapping
     *    mapping from source (target) docno to list of target (source) docnos associated with it
     * @param lang
     *    language identifier
     * @param job
     *    job configuration object
     * @param reporter
     *    reporter object for counters
     */
    private static void loadPairs(HMapIV<ArrayListOfIntsWritable> pwsimMapping, int langID, JobConf job, Reporter reporter){
      try {
        Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
        String pwsimFile = job.get("PwsimPairs");
        for (Path localFile : localFiles) {
          if (localFile.toString().contains(getFilename(pwsimFile))) {
            SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(job), localFile, job);
            
            PairOfInts key = (PairOfInts) reader.getKeyClass().newInstance();
            IntWritable value = (IntWritable) reader.getValueClass().newInstance();
            int cnt = 0;
            while (reader.next(key, value)) {
              int fDocno = key.getRightElement();
//          fDocno -= 1000000000; 
              int eDocno = key.getLeftElement();
              if(langID == CLIRUtils.E){
                if(!pwsimMapping.containsKey(eDocno)){
                  pwsimMapping.put(eDocno, new ArrayListOfIntsWritable());
                }
                pwsimMapping.get(eDocno).add(fDocno);   // we add 1000000000 to foreign docnos to distinguish them during pwsim algo
              }else{
                if(!pwsimMapping.containsKey(fDocno)){
                  pwsimMapping.put(fDocno, new ArrayListOfIntsWritable());
                }
                pwsimMapping.get(fDocno).add(eDocno);   // we add 1000000000 to foreign docnos to distinguish them during pwsim algo
              }
              cnt++;
              key = (PairOfInts) reader.getKeyClass().newInstance();
              value = (IntWritable) reader.getValueClass().newInstance();
            }
            reader.close();
            sLogger.info(pwsimMapping.size() + "," + cnt + " pairs loaded from " + localFile);
            
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void map(PairOfInts sentenceId, WikiSentenceInfo sentenceInfo, OutputCollector<PairOfInts, WikiSentenceInfo> output, Reporter reporter) throws IOException {
      int docno = sentenceId.getLeftElement();
      int langID = sentenceInfo.getLangID();

      // we only load the mapping once, during the first map() call of a mapper. 
      // this works b/c all input kv pairs of a given mapper will have same lang id (reason explained above)
      if (pwsimMapping.isEmpty()) {
        loadPairs(pwsimMapping, langID, mJob, reporter);
        sLogger.info("Mapping loaded: " + pwsimMapping.size());
      }

      if (langID == CLIRUtils.F) {
        docno += 1000000000;
      }
      
      // if no similar docs for docno, return
      if (pwsimMapping.containsKey(docno)) {
        similarDocnos = pwsimMapping.get(docno);  
      }else{
        return;
      }
      
      if (langID == CLIRUtils.E) {
        reporter.incrCounter(Sentences.E, 1);
        reporter.incrCounter(Sentences.pairsE, similarDocnos.size());
      }else {
        reporter.incrCounter(Sentences.F, 1);        
        reporter.incrCounter(Sentences.pairsF, similarDocnos.size());
      }

      for (int similarDocno : similarDocnos) {
        if (langID == CLIRUtils.E) {
          keyOut.set(similarDocno, docno);
        }else {
          keyOut.set(docno, similarDocno);    
        }
        output.collect(keyOut, sentenceInfo);
      }
    }
  }

  /**
   * Bilingual sentence pair detection with simple classifier
   * 
   * Reduce: (<fDocno, eDocno>, [ <E,eDocno,eVectors,eSentences>,  <F,fDocno,fVectors,fSentences>]) --> (fSentence, eSentence)
   *      
   * @author ferhanture
   *
   */
  private static class MyReducer extends MapReduceBase implements
  Reducer<PairOfInts, WikiSentenceInfo, Text, Text>{
    private int fDocno, eDocno;
    private int classifierPositiveId;
    private ArrayListWritable<HMapSFW> fVectors, eVectors;
    private ArrayListWritable<Text> fSentences, eSentences;
    private PreprocessHelper helper;            // for modularity, helper provides methods to preprocess data
    private float classifierThreshold;
    private Text emptyValue = new Text();

    public void configure(JobConf job) {
      //      sLogger.setLevel(Level.DEBUG);

      try {
        helper = new PreprocessHelper(CLIRUtils.MinVectorTerms, CLIRUtils.MinSentenceLength, job);
      } catch (Exception e) {
        e.printStackTrace();
      }
      classifierPositiveId = job.getInt("ClassifierId", -1);
      if(classifierPositiveId != 0 && classifierPositiveId != 1){
        throw new RuntimeException("Id of parallel label in MaxEnt classifier not specified properly: "+classifierPositiveId);
      }

      classifierThreshold = job.getFloat("ClassifierThreshold", 2);
      if (classifierThreshold > 1f) {
        throw new RuntimeException("Classifier confidence threshold > 1, provide value in [0,1]: "+classifierThreshold);        
      }

      eVectors = new ArrayListWritable<HMapSFW>();
      fVectors = new ArrayListWritable<HMapSFW>();
      eSentences = new ArrayListWritable<Text>();
      fSentences = new ArrayListWritable<Text>();
    }

    public void reduce(PairOfInts docnoPair, Iterator<WikiSentenceInfo> wikiSentences,
        OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      eVectors.clear();
      fVectors.clear();
      eSentences.clear();
      fSentences.clear();

      fDocno = docnoPair.getLeftElement();
      eDocno = docnoPair.getRightElement();

      // parse WikiDocInfo object into sentences and vectors, based on the language id
      WikiSentenceInfo sentenceInfo;
      int eCnt = 0, fCnt = 0;
      while (wikiSentences.hasNext()) {
        sentenceInfo = wikiSentences.next();
        if(sentenceInfo.getLangID() == CLIRUtils.F){
          fCnt++;
          fVectors.add(sentenceInfo.getVector());
          fSentences.add(sentenceInfo.getSentence());
          reporter.incrCounter(Sentences.F, 1);
        }else if(sentenceInfo.getLangID() == CLIRUtils.E){
          eCnt++;
          eVectors.add(sentenceInfo.getVector());
          eSentences.add(sentenceInfo.getSentence());
          reporter.incrCounter(Sentences.E, 1);
        }else {
          throw new RuntimeException("Unknown language ID -- should not happen!");
        }
      }

      /**
       * some sentences in docs are removed in previous step (i.e., Docs2Sentences) due to length etc.
       * if all of the sentences in a document are removed, then it will not show up here
       * therefore the pair will be "incomplete". we simply ignore these pairs for bitext extraction.
       */
      if((eCnt == 0 || fCnt == 0)){
        sLogger.debug("Read "+eCnt+","+fCnt+" sentences: ="+eDocno+","+fDocno);
        if(eCnt == 0){
          reporter.incrCounter(Docs.pairsIncompleteE, 1);
        }else{
          reporter.incrCounter(Docs.pairsIncompleteF, 1);
        }
        return;
      }

      // counters for debug purposes only
      reporter.incrCounter(Docs.pairs, 1);
      reporter.incrCounter(Sentences.pairsCandidate, fVectors.size() * eVectors.size());
      int numProcessed = 0;
      long time = 0;

      sLogger.debug(fSentences.size()+","+eSentences.size());

      // classify each e-f sentence pair in the candidate set
      for (int f = 0; f < fVectors.size(); f++) {
        HMapSFW fVector = fVectors.get(f);
        int fSentLength = fSentences.get(f).getLength();

        for (int e = 0; e < eVectors.size(); e++) {
          HMapSFW eVector = eVectors.get(e);
          int eSentLength = eSentences.get(e).getLength();

          if (eSentLength > 2 * fSentLength || fSentLength > 2 * eSentLength) {
            // sLogger.debug("length filter");
            reporter.incrCounter(Sentences.pairsFilteredBySentRatio, 1);
            continue;
          }

          reporter.incrCounter(Sentences.pairsProcessed, 1);        
          numProcessed++;      

          // compute features
          long start = System.currentTimeMillis();
          String[] instance = CLIRUtils.computeFeaturesF1(eVector, fVector, eSentLength, fSentLength);
          time += (System.currentTimeMillis()-start);

          // classify w/ maxent model
          // emit if labeled parallel
          if (instance == null) {
            throw new RuntimeException("SHOULD NOT HAPPEN!");
          }

          // apply MaxEnt classifier to instance
          float[] values = RealValueFileEventStream.parseContexts(instance);
          double[] probs = helper.getClassifier().eval(instance, values);

          // check if confidence above specified threshold
          double confidence = probs[classifierPositiveId];
          if (confidence > classifierThreshold) {
            reporter.incrCounter(Sentences.parallel, 1);  
            output.collect(new Text(fSentences.get(f) + CLIRUtils.BitextSeparator + eSentences.get(e)), emptyValue);
          }
        }
      }
    }
  }

  /**
   * Runs this tool.
   */

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), FindParallelSentencePairs.class);

    // Read commandline argument
    conf = setupConf(conf, args);
    if (conf == null) {
      printUsage();
      return -1;
    }

    conf.setInt("mapred.task.timeout", 60000000);
    conf.set("mapreduce.map.memory.mb", "3000");
    conf.set("mapreduce.map.java.opts", "-Xmx3000m");
    conf.set("mapreduce.reduce.memory.mb", "3000");
    conf.set("mapreduce.reduce.java.opts", "-Xmx3000m");
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
    conf.setNumMapTasks(100);
    conf.setNumReduceTasks(50);
    conf.setInt("mapred.min.split.size", 2000000000);
    conf.setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setMapOutputKeyClass(PairOfInts.class);
    conf.setMapOutputValueClass(WikiSentenceInfo.class);
    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(MyReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    
    long startTime = System.currentTimeMillis();
    JobClient.runJob(conf); 
    sLogger.info("Job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  private static final String FCOLLECTION_OPTION = "f_collection";
  private static final String ECOLLECTION_OPTION = "e_collection";
  private static final String FLANG_OPTION = "f_lang";
  private static final String ELANG_OPTION = "e_lang";
  private static final String FINDEX_OPTION = "f_index";
  private static final String EINDEX_OPTION = "e_index";
  private static final String BITEXTNAME_OPTION = "name";
  private static final String SENTENCES_OPTION = "sentences";
  private static final String BITEXT_OPTION = "bitext";
  private static final String DATADIR_OPTION = "data";
  private static final String PWSIM_OPTION = "pwsim_output";
  private static final String CLASSIFIERID_OPTION = "classifier_id";
  private static final String CLASSIFIERTHRESHOLD_OPTION = "threshold";
  private static final String LIBJARS_OPTION = "libjars";

  private static Options options;

  @SuppressWarnings("static-access")
  protected JobConf setupConf(JobConf conf, String[] args) throws Exception {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("source-side raw collection path").withArgName("path").hasArg().isRequired().create(FCOLLECTION_OPTION));
    options.addOption(OptionBuilder.withDescription("target-side raw collection path").withArgName("path").hasArg().isRequired().create(ECOLLECTION_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter code for f-language").withArgName("en|de|tr|cs|zh|ar|es").hasArg().isRequired().create(FLANG_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter code for e-language").withArgName("en|de|tr|cs|zh|ar|es").hasArg().isRequired().create(ELANG_OPTION));
    options.addOption(OptionBuilder.withDescription("source-side index path").withArgName("path").hasArg().isRequired().create(FINDEX_OPTION));
    options.addOption(OptionBuilder.withDescription("target-side index path").withArgName("path").hasArg().isRequired().create(EINDEX_OPTION));
    options.addOption(OptionBuilder.withDescription("name of bitext").withArgName("string").hasArg().create(BITEXTNAME_OPTION));
    options.addOption(OptionBuilder.withDescription("path to data files on HDFS").withArgName("path").hasArg().isRequired().create(DATADIR_OPTION));
    options.addOption(OptionBuilder.withDescription("path to output of pwsim algorithm").withArgName("path").hasArg().isRequired().create(PWSIM_OPTION));
    options.addOption(OptionBuilder.withDescription("classifier id to retrieve P('PARALLEL'|instance)").withArgName("0 or 1").hasArg().isRequired().create(CLASSIFIERID_OPTION));
    options.addOption(OptionBuilder.withDescription("target vocabulary (e-side) of P(e|f)").withArgName("0-1").hasArg().isRequired().create(CLASSIFIERTHRESHOLD_OPTION));
    options.addOption(OptionBuilder.withDescription("path to collection sentences").withArgName("path").hasArg().isRequired().create(SENTENCES_OPTION));
    options.addOption(OptionBuilder.withDescription("path to output bitext").withArgName("path").hasArg().isRequired().create(BITEXT_OPTION));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars").withArgName("jar packages").hasArg().create(LIBJARS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return null;
    }

    String pwsimPairsPath = cmdline.getOptionValue(PWSIM_OPTION);
    String eDir = cmdline.getOptionValue(EINDEX_OPTION);
    String fDir = cmdline.getOptionValue(FINDEX_OPTION);
    String dataDir = cmdline.getOptionValue(DATADIR_OPTION);
    String eLang = cmdline.getOptionValue(ELANG_OPTION);
    String fLang = cmdline.getOptionValue(FLANG_OPTION);
    String bitextName = cmdline.hasOption(BITEXTNAME_OPTION) ? cmdline.getOptionValue(BITEXTNAME_OPTION) : "";
    float classifierThreshold = Float.parseFloat(cmdline.getOptionValue(CLASSIFIERTHRESHOLD_OPTION));
    int classifierId = Integer.parseInt(cmdline.getOptionValue(CLASSIFIERID_OPTION));
    String sentsPath = cmdline.getOptionValue(SENTENCES_OPTION);
    String outputPath = cmdline.getOptionValue(BITEXT_OPTION);

    if (!FileSystem.get(conf).exists(new Path(sentsPath))) {
      System.err.println("Input sentences does not exist at: " + sentsPath + ". Exiting...");
      return null;
    }
    
    FileInputFormat.addInputPaths(conf, sentsPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    conf.setJobName(this.getClass().getCanonicalName() + "_" + fLang +"-" + eLang +"_F1="+classifierThreshold+"["+classifierId+"]");

    try {
      conf = PwsimEnvironment.setBitextPaths(conf, dataDir, eLang, fLang, bitextName, eDir, fDir, classifierThreshold, classifierId, pwsimPairsPath, "simple");
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error configuring paths: " + e.getMessage());
      return null;
    }

    sLogger.info("Running job " + conf.getJobName());
    sLogger.info("Pwsim output path: " + pwsimPairsPath);
    sLogger.info("Sentences path: " + sentsPath);
    sLogger.info("Output path: " + outputPath);

    return conf;
  }

  /**
   * Dispatches command-line arguments to the tool via the
   * <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new FindParallelSentencePairs(), args);
    System.exit(res);
  }

}