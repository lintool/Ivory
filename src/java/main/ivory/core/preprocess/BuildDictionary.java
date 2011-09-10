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
import ivory.core.data.dictionary.DictionaryTransformationStrategy;
import ivory.core.data.document.TermDocVector;
import ivory.core.util.QuickSort;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.pair.PairOfIntLong;
import edu.umd.cloud9.util.PowerTool;

public class BuildDictionary extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildDictionary.class);

  protected static enum Statistics { Docs, Terms, SumOfDocLengths }

  private static class MyMapper extends Mapper<IntWritable, TermDocVector, Text, PairOfIntLong> {
    private static final Text term = new Text();
    private static final PairOfIntLong pair = new PairOfIntLong();

    @Override
    public void map(IntWritable key, TermDocVector doc, Context context)
        throws IOException, InterruptedException {
      TermDocVector.Reader r = doc.getReader();
      int dl=0, tf=0;
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
      for ( PairOfIntLong pair : values) {
        df += pair.getLeftElement();
        cf += pair.getRightElement();
      }

      output.set(df, cf);
      context.write(key, output);
    }
  }

  private static class TermStats {
    private final String term;
    private final int df;
    private final long cf;
    public TermStats(String term, int df, long cf) {
      this.term = term;
      this.df = df;
      this.cf = cf;
    }
  }

  private static class MyReducer extends Reducer<Text, PairOfIntLong, NullWritable, NullWritable> {
    private final List<TermStats> termStats = Lists.newArrayList();
    private int minDf, maxDf;

    @Override
    public void setup(Reducer<Text, PairOfIntLong, NullWritable, NullWritable>.Context context) {
      minDf = context.getConfiguration().getInt(Constants.MinDf, 2);
      maxDf = context.getConfiguration().getInt(Constants.MaxDf, Integer.MAX_VALUE);
    }

    @Override
    public void reduce(Text key, Iterable<PairOfIntLong> values, Context context)
        throws IOException, InterruptedException {
      int df = 0;
      long cf = 0;
      for ( PairOfIntLong pair : values ) {
        df += pair.getLeftElement();
        cf += pair.getRightElement();
      }
      if (df < minDf || df > maxDf) {
        return;
      }
      context.getCounter(Statistics.Terms).increment(1);
      termStats.add(new TermStats(key.toString(), df, cf));
    }

    private FSDataOutputStream termsOut, idsOut, idsToTermOut,
        dfByTermOut, cfByTermOut, dfByIntOut, cfByIntOut;
    private int[] seqNums = null;
    private int[] dfs = null;
    private long[] cfs = null;
    private int curKeyIndex = 0;

    @Override
    public void cleanup(Reducer<Text, PairOfIntLong, NullWritable, NullWritable>.Context context)
        throws IOException {
      int nTerms = termStats.size();

      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);

      RetrievalEnvironment env = new RetrievalEnvironment(conf.get(Constants.IndexPath), fs);
      termsOut = fs.create(new Path(env.getIndexTermsData()), true);
      idsOut = fs.create(new Path(env.getIndexTermIdsData()), true);
      idsToTermOut = fs.create(new Path(env.getIndexTermIdMappingData()), true);

      dfByTermOut = fs.create(new Path(env.getDfByTermData()), true);
      cfByTermOut = fs.create(new Path(env.getCfByTermData()), true);
      dfByIntOut = fs.create(new Path(env.getDfByIntData()), true);
      cfByIntOut = fs.create(new Path(env.getCfByIntData()), true);

      seqNums = new int[nTerms];
      dfs = new int[nTerms];
      cfs = new long[nTerms];
        
      termsOut.writeInt(nTerms);
      idsOut.writeInt(nTerms);
      idsToTermOut.writeInt(nTerms);
      dfByTermOut.writeInt(nTerms);
      cfByTermOut.writeInt(nTerms);
      dfByIntOut.writeInt(nTerms);
      cfByIntOut.writeInt(nTerms);

      for (int i=0; i<termStats.size(); i++) {
        TermStats stats = termStats.get(i);
        String term = stats.term;
        int df = stats.df;
        long cf = stats.cf;

        WritableUtils.writeVInt(dfByTermOut, df);
        WritableUtils.writeVLong(cfByTermOut, cf);

        termsOut.writeUTF(term);

        seqNums[curKeyIndex] = curKeyIndex;
        dfs[curKeyIndex] = -df;
        cfs[curKeyIndex] = cf;
        curKeyIndex++;
      }

      // Sort based on df and change seqNums accordingly.
      QuickSort.quicksortWithSecondary(seqNums, dfs, cfs, 0, nTerms - 1);

      // Write sorted dfs and cfs by int here.
      for (int i = 0; i < nTerms; i++) {
        WritableUtils.writeVInt(dfByIntOut, -dfs[i]);
        WritableUtils.writeVLong(cfByIntOut, cfs[i]);
      }
      cfs = null;

      // Encode the sorted dfs into ids ==> df values erased and become ids instead. Note that
      // first term id is 1.
      for (int i = 0; i < nTerms; i++) {
        dfs[i] = i + 1;
      }

      // Write current seq nums to be index into the term array.
      for (int i = 0; i < nTerms; i++)
        idsToTermOut.writeInt(seqNums[i]);

      // Sort on seqNums to get the right writing order.
      QuickSort.quicksort(dfs, seqNums, 0, nTerms - 1);
      for (int i = 0; i < nTerms; i++) {
        idsOut.writeInt(dfs[i]);
      }

      termsOut.close();
      idsOut.close();
      idsToTermOut.close();
      dfByTermOut.close();
      cfByTermOut.close();
      dfByIntOut.close();
      cfByIntOut.close();
      LOG.info("Finished writing dictionary data file.");
    }
  }

  public static final String[] RequiredParameters = {
      Constants.CollectionName, Constants.IndexPath, Constants.MinDf, Constants.MaxDf };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public BuildDictionary(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {
    Configuration conf = getConf();

    FileSystem fs = FileSystem.get(conf);

    String indexPath = conf.get(Constants.IndexPath);
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

    String collectionName = env.readCollectionName();
    String termDocVectorsPath = env.getTermDocVectorsDirectory();

    if (!fs.exists(new Path(indexPath))) {
      LOG.info("index path doesn't existing: skipping!");
      return 0;
    }

    LOG.info("PowerTool: " + BuildDictionary.class.getCanonicalName());
    LOG.info(String.format(" - %s: %s", Constants.CollectionName, collectionName));
    LOG.info(String.format(" - %s: %s", Constants.IndexPath, indexPath));

    Job job = new Job(getConf(), BuildDictionary.class.getSimpleName() + ":" + collectionName);
    job.setJarByClass(BuildDictionary.class);

    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(termDocVectorsPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfIntLong.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfIntLong.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);
    job.setSortComparatorClass(DictionaryTransformationStrategy.Comparator.class);

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
