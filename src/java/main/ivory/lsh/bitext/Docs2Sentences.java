package ivory.lsh.bitext;

import ivory.core.util.CLIRUtils;
import ivory.lsh.data.WikiSentenceInfo;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.pair.PairOfInts;

/**

 * @author ferhanture
 * 
 */
@SuppressWarnings("deprecation")
public class Docs2Sentences extends Configured implements Tool {

  private static final Logger sLogger = Logger.getLogger(Docs2Sentences.class);

  enum Sentences {
    ELength, FLength, E, F;
  }

  enum Docs {
    EEmpty, FEmpty, E, F;
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
  Mapper<Writable, Indexable, PairOfInts, WikiSentenceInfo> {

    private PairOfInts keyOut;
    private WikiSentenceInfo valOut;
    private PreprocessHelper helper;							// for modularity, helper provides methods to preprocess data

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);

      try {
        helper = new PreprocessHelper(CLIRUtils.MinVectorTerms, CLIRUtils.MinSentenceLength, job);
      } catch (Exception e) {
        e.printStackTrace();
      }

      keyOut = new PairOfInts();
      valOut = new WikiSentenceInfo();
    }

    public void map(Writable docnoKey, Indexable page, OutputCollector<PairOfInts, WikiSentenceInfo> output, Reporter reporter) throws IOException {
      int docno = ((IntWritable)docnoKey).get();

      WikipediaPage p = (WikipediaPage) page;
      String lang = p.getLanguage();
      int langID;

      ArrayListWritable<Text> sentences;
      ArrayListWritable<HMapSFW> vectors = new ArrayListWritable<HMapSFW>();
      ArrayListOfIntsWritable sentLengths = new ArrayListOfIntsWritable();
      try {
        // identify sentences in document, filter out ones below MinSentLength threshold
        // convert each sentence into a tf-idf vector, using general DF map for collection and a heuristic for avg. doc length
        // filter out sentences for which the vector has less than MinVectorTerms terms
        if (lang.equals("en")) {
          sentences = helper.getESentences(p.getContent(), vectors, sentLengths);		
          langID = CLIRUtils.E;
        }else {
          sentences = helper.getFSentences(p.getContent(), vectors, sentLengths);
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
          if (docno % 10000 == 0) {
            sLogger.info("debugging "+p.getTitle());
          }
          reporter.incrCounter(Docs.F, 1);
        }
      }

      
      
      for (int i = 0; i < sentences.size(); i++) {
        if (langID == CLIRUtils.E) {
          reporter.incrCounter(Sentences.ELength, sentLengths.get(i));
          reporter.incrCounter(Sentences.E, 1);    
        }else {
          if (docno % 10000 == 0) {
            sLogger.info(i+":"+sentences.get(i));
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

    String eCollectionPath = args[0];
    String fCollectionPath = args[1];
    String sentsPath = args[2];

    conf.setJobName("Docs2Sentences");  

    FileInputFormat.addInputPaths(conf, eCollectionPath);
    FileInputFormat.addInputPaths(conf, fCollectionPath);
    FileOutputFormat.setOutputPath(conf, new Path(sentsPath));

    conf.setInt("mapred.task.timeout", 60000000);
    conf.set("mapred.child.java.opts", "-Xmx2000m");
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    conf.setNumMapTasks(100);
    conf.setNumReduceTasks(100);
    conf.setInt("mapred.min.split.size", 2000000000);
    conf.setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapOutputKeyClass(PairOfInts.class);
    conf.setMapOutputValueClass(WikiSentenceInfo.class);
    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setPartitionerClass(MyPartitioner.class);
    conf.setOutputKeyClass(PairOfInts.class);
    conf.setOutputValueClass(WikiSentenceInfo.class);
   
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
    
    sLogger.info("Number of " + conf.get("eLang") + " sentences (total, per doc) = " + numSentsE + "," + (numSentsE+0.0f/numDocsE));
    sLogger.info("Number of " + conf.get("fLang") + " sentences (total, per doc) = " + numSentsF + "," + (numSentsF+0.0f/numDocsF));
    sLogger.info("Average num of tokens per " + conf.get("eLang") + " sentence = " + sumSentLengthsE + "," + (sumSentLengthsE+0.0f/numSentsE));
    sLogger.info("Average num of tokens per " + conf.get("fLang") + " sentence = " + sumSentLengthsF + "," + (sumSentLengthsF+0.0f/numSentsF));
    sLogger.info(conf.get("eLang") + " documents discarded due to too few sentences = " + numEmptyDocsE);
    sLogger.info(conf.get("fLang") + " documents discarded due to too few sentences = " + numEmptyDocsF);

    return 0;
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


