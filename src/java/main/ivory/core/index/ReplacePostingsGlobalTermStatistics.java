package ivory.core.index;

import ivory.core.Constants;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.dictionary.DefaultFrequencySortedDictionary;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsListDocSortedPositional;
import ivory.core.data.stat.CfTable;
import ivory.core.data.stat.CfTableArray;
import ivory.core.data.stat.DfTable;
import ivory.core.data.stat.DfTableArray;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.PowerTool;

public class ReplacePostingsGlobalTermStatistics extends PowerTool {
  private static final Logger LOG = Logger.getLogger(ReplacePostingsGlobalTermStatistics.class);

  private static class MyMapper extends
      Mapper<IntWritable, PostingsList, IntWritable, PostingsList> {
    private DefaultFrequencySortedDictionary dictionary = null;
    private DefaultFrequencySortedDictionary globalDictionary = null;
    private DfTable dfTable;
    private CfTable cfTable;

    @Override
    public void setup(Context context) {
      try {
        Configuration conf = context.getConfiguration();
        dictionary = loadDictionary(conf, conf.get(Constants.IndexPath));

        String globalPath = conf.get("Ivory.DictionaryPath");
        globalDictionary = loadDictionary(conf, globalPath);
        RetrievalEnvironment env = new RetrievalEnvironment(globalPath, FileSystem.get(conf));
        
        dfTable = new DfTableArray(new Path(env.getDfByIntData()), FileSystem.get(conf));
        cfTable = new CfTableArray(new Path(env.getCfByIntData()), FileSystem.get(conf));
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing data!", e);
      }
    }

    private DefaultFrequencySortedDictionary loadDictionary(Configuration conf, String path)
        throws Exception {
      FileSystem fs = FileSystem.get(conf);
      RetrievalEnvironment env = new RetrievalEnvironment(path, fs);

      Path termsFile = new Path(env.getIndexTermsData());
      Path termidsFile = new Path(env.getIndexTermIdsData());
      Path idToTermFile = new Path(env.getIndexTermIdMappingData());

      LOG.info(" - terms: " + termsFile);
      LOG.info(" - id: " + termidsFile);
      LOG.info(" - idToTerms: " + idToTermFile);

      return new DefaultFrequencySortedDictionary(termsFile, termidsFile, idToTermFile, fs);
    }

    @Override
    public void map(IntWritable key, PostingsList p, Context context) 
        throws IOException, InterruptedException {

      String term = dictionary.getTerm(key.get());
      int globalTermid = globalDictionary.getId(term);
      if (globalTermid > 0) {
        p.setCf(cfTable.getCf(globalTermid));
        p.setDf(dfTable.getDf(globalTermid));
        context.write(key, p);
      }
    }
  }

  public static final String[] RequiredParameters = { Constants.IndexPath, "Ivory.DictionaryPath" };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public ReplacePostingsGlobalTermStatistics(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    String indexPath = conf.get(Constants.IndexPath);
    String dictionaryPath = conf.get("Ivory.DictionaryPath");

    LOG.info("Tool: " + ReplacePostingsGlobalTermStatistics.class.getCanonicalName());
    LOG.info(String.format(" - %s: %s", Constants.IndexPath, indexPath));
    LOG.info(String.format(" - %s: %s", "Ivory.DictionaryPath", dictionaryPath));

    // preserve old postings
    Path postingsPath1 = new Path(indexPath + "/postings/");
    Path postingsPath2 = new Path(indexPath + "/postings.old/");

    if (fs.exists(postingsPath1)) {
      LOG.info("renaming " + postingsPath1.getName() + " to " + postingsPath2.getName());
      fs.rename(postingsPath1, postingsPath2);
    }

    Job job = Job.getInstance(conf,
        ReplacePostingsGlobalTermStatistics.class.getSimpleName() + ":" + indexPath);

    job.setJarByClass(ReplacePostingsGlobalTermStatistics.class);

    FileInputFormat.addInputPath(job, postingsPath2);
    FileOutputFormat.setOutputPath(job, postingsPath1);

    job.setNumReduceTasks(0);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PostingsListDocSortedPositional.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PostingsListDocSortedPositional.class);

    job.setMapperClass(MyMapper.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }
}
