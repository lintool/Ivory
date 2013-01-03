package ivory.lsh.bitext;

import ivory.core.util.CLIRUtils;
import ivory.lsh.data.WikiSentenceInfo;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.pair.PairOfInts;

/**

 * @author ferhanture
 * 
 */
public class Docs2Sentences extends Configured implements Tool {

  private static final Logger sLogger = Logger.getLogger(Docs2Sentences.class);

  enum Sentences {
    ELength, FLength, E, F, OOV;
  }

  enum Docs {
    EEmpty, FEmpty, E, F;
  }

  private static Options options;
  
  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "Docs2Sentences", options );
    System.exit(-1);    
  }
  
  public Docs2Sentences() {
  }

  public Docs2Sentences(Configuration conf) {
    super(conf);
  }

  /**
   * Candidate generation
   * 
   * Map: (docno, wikiPage) --> (<docno, sentid>, sentence)
   * input is union of source and target collections
   *     sentences = extract sentences in wikiPage
   * 		 vectors = convert sentence text into td-idf vector
   * 	   similar_pairs = from pwsim output, find if there's any pair corresponding to docno
   * 	   foreach similar_pair
   * 		   emit(similar_pair, <lang id,docno,vectors,sentences>)
   * 
   * @author ferhanture
   */
  private static class MyMapper extends MapReduceBase implements
  Mapper<IntWritable, WikipediaPage, PairOfInts, WikiSentenceInfo> {

    private PairOfInts keyOut;
    private WikiSentenceInfo valOut;
    private PreprocessHelper helper;							// for modularity, helper provides methods to preprocess data
    private float minInVocabRate;

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);

      minInVocabRate = job.getFloat("MinInVocabRate", 0.5f);    

      try {
        helper = new PreprocessHelper(CLIRUtils.MinVectorTerms, CLIRUtils.MinSentenceLength, job);
      } catch (Exception e) {
        e.printStackTrace();
      }
      keyOut = new PairOfInts();
      valOut = new WikiSentenceInfo();
    }

    public void map(IntWritable docnoKey, WikipediaPage p, OutputCollector<PairOfInts, WikiSentenceInfo> output, Reporter reporter) throws IOException {
      int docno = docnoKey.get();
      String lang = p.getLanguage();
      int langID;

      ArrayListWritable<Text> sentences;
      ArrayListWritable<HMapSFW> vectors = new ArrayListWritable<HMapSFW>();
      ArrayListOfIntsWritable sentLengths = new ArrayListOfIntsWritable();
      try {
        // identify sentences in document, filter out ones below MinSentLength threshold
        // convert each sentence into a tf-idf vector, using general DF map for collection and a heuristic for avg. doc length
        // filter out sentences for which the vector has less than MinVectorTerms terms
        String article = p.getContent();
        if (lang.equals("en")) {
          sentences = helper.getESentences(article, vectors, sentLengths);		
          langID = CLIRUtils.E;
        }else {
          // Turkish Wiki articles' XML does not encode paragraph breaks
          if (lang.equals("tr")) {
            article = article.replaceAll("\\.", ". ");
          }
          sentences = helper.getFSentences(article, vectors, sentLengths);
          langID = CLIRUtils.F;
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      // documents with no sentences (after we filter out some by length)
      if (sentences.size() == 0) {
        if (langID == CLIRUtils.E) {
          reporter.incrCounter(Docs.EEmpty, 1);	
        }else {
          reporter.incrCounter(Docs.FEmpty, 1);
        }
      }else{
        if (langID == CLIRUtils.E) {
          reporter.incrCounter(Docs.E, 1);
        }else {
          reporter.incrCounter(Docs.F, 1);
        }
      }   

      for (int i = 0; i < sentences.size(); i++) {
        if (langID == CLIRUtils.E) {
          if (helper.getEInVocabRate(sentences.get(i).toString()) < minInVocabRate ) {
            reporter.incrCounter(Sentences.OOV, 1);
            return;
          }          
          reporter.incrCounter(Sentences.ELength, sentLengths.get(i));
          reporter.incrCounter(Sentences.E, 1);    
        }else {
          if (helper.getFInVocabRate(sentences.get(i).toString()) < minInVocabRate ) {
            reporter.incrCounter(Sentences.OOV, 1);
            return;
          }
          reporter.incrCounter(Sentences.FLength, sentLengths.get(i));
          reporter.incrCounter(Sentences.F, 1);    
        }
        keyOut.set(docno, langID);      
        valOut.set(langID, sentences.get(i), vectors.get(i));
        output.collect(keyOut, valOut);
      }
    }
  }

  private static class MyPartitioner implements Partitioner<PairOfInts,WikiSentenceInfo> {

    public int getPartition(PairOfInts key, WikiSentenceInfo value, int numReducers) {
      int p = (int) (Math.random() * numReducers/2);
      // we want sentences from different languages to be in different files
      // to make things easier in the sentence pair classification job: FindParallel...
      if (key.getRightElement() == CLIRUtils.E) {
        return p;                                   // 0 <= p <= (numReducers/2)-1
      }else {
        return p + (numReducers - numReducers/2);   // numReducers/2 <= p <= numReducers-1
      }
    }

    @Override
    public void configure(JobConf conf) { }
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), Docs2Sentences.class);

    // Read commandline arguments
    CommandLine cmdline = parseArgs(args);
    if (cmdline == null) {
      printUsage();
    }
    String eCollectionPath = cmdline.getOptionValue(ECOLLECTION_OPTION);
    String fCollectionPath = cmdline.getOptionValue(FCOLLECTION_OPTION);
    String sentsPath = cmdline.getOptionValue(SENTENCES_OPTION);

    conf.setJobName("Docs2Sentences_"+conf.get("fLang")+"-"+conf.get("eLang"));  

    FileInputFormat.addInputPaths(conf, eCollectionPath);
    FileInputFormat.addInputPaths(conf, fCollectionPath);
    FileOutputFormat.setOutputPath(conf, new Path(sentsPath));

    conf.setInt("mapred.task.timeout", 60000000);
    conf.set("mapred.child.java.opts", "-Xmx2000m");
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    conf.setNumMapTasks(100);
    conf.setNumReduceTasks(200);
    conf.setInt("mapred.min.split.size", 2000000000);
    conf.setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapOutputKeyClass(PairOfInts.class);
    conf.setMapOutputValueClass(WikiSentenceInfo.class);
    conf.setOutputKeyClass(PairOfInts.class);
    conf.setOutputValueClass(WikiSentenceInfo.class);
    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setPartitionerClass(MyPartitioner.class);

    long startTime = System.currentTimeMillis();
    RunningJob j = JobClient.runJob(conf);
    System.out.println("Job finished in " + (System.currentTimeMillis() - startTime)
        + " milliseconds");
    Counters counters = j.getCounters();
    long sumSentLengthsE = (long) counters.findCounter(Sentences.ELength).getCounter();
    long sumSentLengthsF = (long) counters.findCounter(Sentences.FLength).getCounter();
    long numSentsE = (long) counters.findCounter(Sentences.E).getCounter();
    long numSentsF = (long) counters.findCounter(Sentences.F).getCounter();
    long numDocsE = (long) counters.findCounter(Docs.E).getCounter();
    long numDocsF = (long) counters.findCounter(Docs.F).getCounter();
    long numEmptyDocsE = (long) counters.findCounter(Docs.EEmpty).getCounter();
    long numEmptyDocsF = (long) counters.findCounter(Docs.FEmpty).getCounter();

    sLogger.info("<STATS> "+conf.get("eLang") + " documents = " + numDocsE);
    sLogger.info("<STATS> "+conf.get("fLang") + " documents = " + numDocsF);
    sLogger.info("<STATS> "+conf.get("eLang") + " documents discarded due to too few sentences = " + numEmptyDocsE);
    sLogger.info("<STATS> "+conf.get("fLang") + " documents discarded due to too few sentences = " + numEmptyDocsF);
    sLogger.info("<STATS> Number of " + conf.get("eLang") + " sentences (total, per doc) = " + numSentsE + "," + (numSentsE+0.0f)/numDocsE);
    sLogger.info("<STATS> Number of " + conf.get("fLang") + " sentences (total, per doc) = " + numSentsF + "," + (numSentsF+0.0f)/numDocsF);
    sLogger.info("<STATS> Average num of tokens per " + conf.get("eLang") + " sentence = " + sumSentLengthsE + "," + (sumSentLengthsE+0.0f)/numSentsE);
    sLogger.info("<STATS> Average num of tokens per " + conf.get("fLang") + " sentence = " + sumSentLengthsF + "," + (sumSentLengthsF+0.0f)/numSentsF);

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

  @SuppressWarnings("static-access")
  private CommandLine parseArgs(String[] args) throws Exception {
    options.addOption(OptionBuilder.withDescription("source-side raw collection path").withArgName("path").hasArg().isRequired().create(FCOLLECTION_OPTION));
    options.addOption(OptionBuilder.withDescription("target-side raw collection path").withArgName("path").hasArg().isRequired().create(ECOLLECTION_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter code for f-language").withArgName("en|de|tr|cs|zh|ar|es").hasArg().isRequired().create(FLANG_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter code for e-language").withArgName("en|de|tr|cs|zh|ar|es").hasArg().isRequired().create(ELANG_OPTION));
    options.addOption(OptionBuilder.withDescription("source-side index path").withArgName("path").hasArg().isRequired().create(FINDEX_OPTION));
    options.addOption(OptionBuilder.withDescription("target-side index path").withArgName("path").hasArg().isRequired().create(EINDEX_OPTION));
    options.addOption(OptionBuilder.withDescription("name of bitext").withArgName("string").hasArg().isRequired().create(BITEXTNAME_OPTION));
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

    return cmdline;
  }

  /**
   * Dispatches command-line arguments to the tool via the
   * <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Docs2Sentences(), args);
    System.exit(res);
  }

}


