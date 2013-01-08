/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.core.index;

import ivory.core.Constants;
import ivory.core.RetrievalEnvironment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfIntLong;
import edu.umd.cloud9.util.PowerTool;

public class MergeGlobalTermStatistics extends PowerTool {
  private static final Logger LOG = Logger.getLogger(MergeGlobalTermStatistics.class);

  protected static enum Statistics { Terms }

  private static class MyCombiner extends Reducer<Text, PairOfIntLong, Text, PairOfIntLong> {
    private static final PairOfIntLong PAIR = new PairOfIntLong();

    @Override
    public void reduce(Text key, Iterable<PairOfIntLong> values, Context context)
        throws IOException, InterruptedException {
      int df = 0;
      long cf = 0;
      for (PairOfIntLong pair : values) {
        df += pair.getLeftElement();
        cf += pair.getRightElement();
      }

      PAIR.set(df, cf);
      context.write(key, PAIR);
    }
  }

  private static class MyReducer extends Reducer<Text, PairOfIntLong, Text, PairOfIntLong> {
    private static final PairOfIntLong PAIR = new PairOfIntLong();
    private int minDf;

    @Override
    public void setup(Reducer<Text, PairOfIntLong, Text, PairOfIntLong>.Context context) {
      minDf = context.getConfiguration().getInt(Constants.MinDf, 2);
    }

    @Override
    public void reduce(Text key, Iterable<PairOfIntLong> values, Context context)
    throws IOException, InterruptedException {
      int df = 0;
      long cf = 0;
      for (PairOfIntLong pair : values) {
        df += pair.getLeftElement();
        cf += pair.getRightElement();
      }
      if (df < minDf) {
        return;
      }
      context.getCounter(Statistics.Terms).increment(1);
      PAIR.set(df, cf);
      context.write(key, PAIR);
    }
  }

  public static final String[] RequiredParameters = { Constants.CollectionName, Constants.MinDf,
      "Ivory.IndexPaths", "Ivory.DictionaryOutputPath" };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public MergeGlobalTermStatistics(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {
    Configuration conf = getConf();

    String collectionName = conf.get(Constants.CollectionName);
    int dfThreshold = conf.getInt(Constants.MinDf, 2);
    String indexPaths = conf.get("Ivory.IndexPaths");
    String outputPath = conf.get("Ivory.DictionaryOutputPath");

    LOG.info("Tool: " + MergeGlobalTermStatistics.class.getCanonicalName());
    LOG.info(String.format(" - %s: %s", Constants.CollectionName, collectionName));
    LOG.info(String.format(" - %s: %d", Constants.MinDf, dfThreshold));
    LOG.info(String.format(" - %s: %s", "Ivory.IndexPaths", indexPaths));
    LOG.info(String.format(" - %s: %s", "Ivory.DictionaryOutputPath", outputPath));

    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(new Path(outputPath));
    RetrievalEnvironment env = new RetrievalEnvironment(outputPath, fs);

    Job job = Job.getInstance(conf,
        MergeGlobalTermStatistics.class.getSimpleName() + ":" + collectionName);

    job.setJarByClass(MergeGlobalTermStatistics.class);
    job.setNumReduceTasks(1);

    long collectionLength = 0;
    int docCount = 0;

    for (String index : indexPaths.split(",")) {
      RetrievalEnvironment shardEnv = new RetrievalEnvironment(index, fs);
      collectionLength += shardEnv.readCollectionLength();
      docCount += shardEnv.readCollectionDocumentCount();

      Path p = RetrievalEnvironment.getTermDfCfDirectory(index);
      LOG.info("Adding shard term statistics at " + p);
      FileInputFormat.addInputPath(job, p);
    }
    FileOutputFormat.setOutputPath(job, RetrievalEnvironment.getTermDfCfDirectory(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfIntLong.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfIntLong.class);

    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters counters = job.getCounters();

    int numTerms = (int) counters.findCounter(Statistics.Terms).getValue();
    LOG.info("Number of unique terms in collection: " + numTerms);
    env.writeCollectionTermCount(numTerms);

    LOG.info("Collection length: " + collectionLength);
    env.writeCollectionLength(collectionLength);

    LOG.info("Collection document count: " + docCount);
    env.writeCollectionDocumentCount(docCount);

    float avgDl = (float) collectionLength / docCount;
    LOG.info("Average document length: " + avgDl);
    env.writeCollectionAverageDocumentLength(avgDl);
 
    return 0;
  }
}
