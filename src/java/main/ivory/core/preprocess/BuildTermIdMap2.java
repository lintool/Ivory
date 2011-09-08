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

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.util.FrontCodedStringList;
import ivory.core.Constants;
import ivory.core.RetrievalEnvironment;
import ivory.core.util.QuickSort;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.pair.PairOfIntLong;
import edu.umd.cloud9.util.PowerTool;

public class BuildTermIdMap2 extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildTermIdMap2.class);

  protected static enum Terms { Total }

  private static class MyReducer
      extends Reducer<Text, PairOfIntLong, NullWritable, NullWritable> {
    private FSDataOutputStream termsOut, idsOut, idsToTermOut,
        dfByTermOut, cfByTermOut, dfByIntOut, cfByIntOut;
    private int nTerms, window;
    private int[] seqNums = null;
    private int[] dfs = null;
    private long[] cfs = null;
    private int curKeyIndex = 0;

    @Override
    public void setup(Reducer<Text, PairOfIntLong, NullWritable, NullWritable>.Context context) {
      Configuration conf = context.getConfiguration();
      FileSystem fs;
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException("Error opening the FileSystem!");
      }

      RetrievalEnvironment env;
      try {
        env = new RetrievalEnvironment(conf.get(Constants.IndexPath), fs);
      } catch (IOException e) {
        throw new RuntimeException("Unable to create RetrievalEnvironment!");
      }

      String termsFile = env.getIndexTermsData();
      String idsFile = env.getIndexTermIdsData();
      String idToTermFile = env.getIndexTermIdMappingData();

      String dfByTermFile = env.getDfByTermData();
      String cfByTermFile = env.getCfByTermData();
      String dfByIntFile = env.getDfByIntData();
      String cfByIntFile = env.getCfByIntData();

      nTerms = conf.getInt(Constants.CollectionTermCount, 0);
      window = conf.getInt(Constants.TermIndexWindow, 8);

      seqNums = new int[nTerms];
      dfs = new int[nTerms];
      cfs = new long[nTerms];

      LOG.info("Ivory.PrefixEncodedTermsFile: " + termsFile);
      LOG.info("Ivory.TermIDsFile" + idsFile);
      LOG.info("Ivory.IDToTermFile" + idToTermFile);
      LOG.info("Ivory.CollectionTermCount: " + nTerms);
      LOG.info("Ivory.ForwardIndexWindow: " + window);

      try {
        termsOut = fs.create(new Path(termsFile), true);
        idsOut = fs.create(new Path(idsFile), true);
        idsToTermOut = fs.create(new Path(idToTermFile), true);
        idsOut.writeInt(nTerms);
        idsToTermOut.writeInt(nTerms);
        termsOut.writeInt(nTerms);

        dfByTermOut = fs.create(new Path(dfByTermFile), true);
        cfByTermOut = fs.create(new Path(cfByTermFile), true);
        dfByTermOut.writeInt(nTerms);
        cfByTermOut.writeInt(nTerms);

        dfByIntOut = fs.create(new Path(dfByIntFile), true);
        cfByIntOut = fs.create(new Path(cfByIntFile), true);
        dfByIntOut.writeInt(nTerms);
        cfByIntOut.writeInt(nTerms);
      } catch (Exception e) {
        throw new RuntimeException("error in creating files");
      }
      LOG.info("Finished config.");
    }

    @Override
    public void reduce(Text key, Iterable<PairOfIntLong> values, Context context)
        throws IOException, InterruptedException {
      String term = key.toString();
      Iterator<PairOfIntLong> iter = values.iterator();
      PairOfIntLong p = iter.next();
      int df = p.getLeftElement();
      long cf = p.getRightElement();
      WritableUtils.writeVInt(dfByTermOut, df);
      WritableUtils.writeVLong(cfByTermOut, cf);

      if (iter.hasNext()) {
        throw new RuntimeException("More than one record for term: " + term);
      }

      termsOut.writeUTF(term);

      seqNums[curKeyIndex] = curKeyIndex;
      dfs[curKeyIndex] = -df;
      cfs[curKeyIndex] = cf;
      curKeyIndex++;

      context.getCounter(Terms.Total).increment(1);
    }

    @Override
    public void cleanup(
        Reducer<Text, PairOfIntLong, NullWritable, NullWritable>.Context context)
        throws IOException {
      LOG.info("Finished reduce.");
      if (curKeyIndex != nTerms) {
        throw new RuntimeException("Total expected Terms: " + nTerms +
            ", Total observed terms: " + curKeyIndex + "!");
      }
      // Sort based on df and change seqNums accordingly.
      QuickSort.quicksortWithSecondary(seqNums, dfs, cfs, 0, nTerms - 1);

      // Write sorted dfs and cfs by int here.
      for (int i = 0; i < nTerms; i++) {
        WritableUtils.writeVInt(dfByIntOut, -dfs[i]);
        WritableUtils.writeVLong(cfByIntOut, cfs[i]);
      }
      cfs = null;

      // Encode the sorted dfs into ids ==> df values erased and become
      // ids instead. Note that first term id is 1.
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
      LOG.info("Finished close.");
    }
  }

  public static final String[] RequiredParameters = {
      Constants.CollectionName, Constants.IndexPath, Constants.TermIndexWindow };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public BuildTermIdMap2(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    String indexPath = conf.get(Constants.IndexPath);
    String collectionName = conf.get(Constants.CollectionName);

    LOG.info("PowerTool: BuildTermIdMap2");
    LOG.info(String.format(" - %s: %s", Constants.CollectionName, collectionName));
    LOG.info(String.format(" - %s: %s", Constants.IndexPath, indexPath));

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    if (!fs.exists(new Path(indexPath))) {
      LOG.error("index path doesn't existing: skipping!");
      return 0;
    }

    Path termsFilePath = new Path(env.getIndexTermsData());
    Path termIDsFilePath = new Path(env.getIndexTermIdsData());
    Path idToTermFilePath = new Path(env.getIndexTermIdMappingData());
    Path dfByTermFilePath = new Path(env.getDfByTermData());
    Path cfByTermFilePath = new Path(env.getCfByTermData());
    Path dfByIntFilePath = new Path(env.getDfByIntData());
    Path cfByIntFilePath = new Path(env.getCfByIntData());

    if (fs.exists(termsFilePath) || fs.exists(termIDsFilePath) || fs.exists(idToTermFilePath)
        || fs.exists(dfByTermFilePath) || fs.exists(cfByTermFilePath)
        || fs.exists(dfByIntFilePath) || fs.exists(cfByIntFilePath)) {
      LOG.info("term and term id data exist: skipping!");
      return 0;
    }

    conf.setInt(Constants.CollectionTermCount, (int) env.readCollectionTermCount());

    Path tmpPath = new Path(env.getTempDirectory());
    fs.delete(tmpPath, true);

    Job job = new Job(conf,
        BuildTermIdMap2.class.getSimpleName() + ":" + collectionName);

    job.setJarByClass(BuildTermIdMap2.class);
    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(env.getTermDfCfDirectory()));
    FileOutputFormat.setOutputPath(job, tmpPath);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfIntLong.class);
    job.setOutputKeyClass(Text.class);

    job.setMapperClass(Mapper.class);
    job.setReducerClass(MyReducer.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    fs.delete(tmpPath, true);

    return 0;
  }
}
