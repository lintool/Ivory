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

package ivory.core.preprocess;

import ivory.core.Constants;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.document.TermDocVector;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfIntLong;
import edu.umd.cloud9.util.PowerTool;

public class ComputeGlobalTermStatistics extends PowerTool {
  private static final Logger LOG = Logger.getLogger(ComputeGlobalTermStatistics.class);

  protected static enum Statistics {
    Docs, Terms, SumOfDocLengths
  }

  private static class MyMapper extends Mapper<IntWritable, TermDocVector, Text, PairOfIntLong> {
    private static final Text term = new Text();
    private static final PairOfIntLong pair = new PairOfIntLong();

    @Override
    public void map(IntWritable key, TermDocVector doc, Context context) throws IOException,
        InterruptedException {
      TermDocVector.Reader r = doc.getReader();
      int dl = 0, tf = 0;
      while (r.hasMoreTerms()) {
        term.set(r.nextTerm());
        tf = r.getTf();
        dl += tf;
        pair.set(1, tf);
        context.write(term, pair);
      }

      context.getCounter(Statistics.Docs).increment(1);
      context.getCounter(Statistics.SumOfDocLengths).increment(dl);
    }
  }

  private static class MyCombiner extends Reducer<Text, PairOfIntLong, Text, PairOfIntLong> {
    private static final PairOfIntLong output = new PairOfIntLong();

    @Override
    public void reduce(Text key, Iterable<PairOfIntLong> values, Context context)
        throws IOException, InterruptedException {
      int df = 0;
      long cf = 0;
      for (PairOfIntLong pair : values) {
        df += pair.getLeftElement();
        cf += pair.getRightElement();
      }

      output.set(df, cf);
      context.write(key, output);
    }
  }

  private static class MyReducer extends Reducer<Text, PairOfIntLong, Text, PairOfIntLong> {
    private static final PairOfIntLong output = new PairOfIntLong();
    private int minDf, maxDf;

    @Override
    public void setup(Reducer<Text, PairOfIntLong, Text, PairOfIntLong>.Context context) {
      minDf = context.getConfiguration().getInt(Constants.MinDf, 2);
      maxDf = context.getConfiguration().getInt(Constants.MaxDf, Integer.MAX_VALUE);
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
      if (df < minDf || df > maxDf) {
        return;
      }
      context.getCounter(Statistics.Terms).increment(1);
      output.set(df, cf);
      context.write(key, output);
    }
  }

  public static final String[] RequiredParameters = { Constants.CollectionName,
      Constants.IndexPath, Constants.MinDf, Constants.MaxDf };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public ComputeGlobalTermStatistics(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {
    Configuration conf = getConf();

    FileSystem fs = FileSystem.get(conf);

    String indexPath = conf.get(Constants.IndexPath);
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

    int reduceTasks = 10;

    String collectionName = env.readCollectionName();
    String termDocVectorsPath = env.getTermDocVectorsDirectory();
    String termDfCfPath = env.getTermDfCfDirectory();

    if (!fs.exists(new Path(indexPath))) {
      LOG.info("index path doesn't existing: skipping!");
      return 0;
    }

    if (!fs.exists(new Path(termDocVectorsPath))) {
      LOG.info("term doc vectors path doesn't existing: skipping!");
      return 0;
    }

    LOG.info("PowerTool: " + ComputeGlobalTermStatistics.class.getCanonicalName());
    LOG.info(String.format(" - %s: %s", Constants.CollectionName, collectionName));
    LOG.info(String.format(" - %s: %s", Constants.IndexPath, indexPath));
    LOG.info(String.format(" - %s: %s", Constants.NumReduceTasks, reduceTasks));

    Path outputPath = new Path(termDfCfPath);
    if (fs.exists(outputPath)) {
      LOG.info("TermDfCf directory exist: skipping!");
      return 0;
    }

    Job job = new Job(getConf(), ComputeGlobalTermStatistics.class.getSimpleName() + ":"
        + collectionName);
    job.setJarByClass(ComputeGlobalTermStatistics.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(termDocVectorsPath));
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfIntLong.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfIntLong.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters counters = job.getCounters();
    // Write out number of postings. NOTE: this value is not the same as
    // number of postings, because postings for non-English terms are
    // discarded, or as result of df cut.
    env.writeCollectionTermCount((int) counters.findCounter(Statistics.Terms).getValue());

    env.writeCollectionLength(counters.findCounter(Statistics.SumOfDocLengths).getValue());
    return 0;
  }
}
