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

import it.unimi.dsi.sux4j.mph.TwoStepsLcpMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.util.FrontCodedStringList;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;
import ivory.core.Constants;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.dictionary.DictionaryTransformationStrategy;
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

public class BuildDictionary extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildDictionary.class);

  protected static enum Terms {
    Total
  }

  private static class MyReducer extends Reducer<Text, PairOfIntLong, NullWritable, NullWritable> {
    private FSDataOutputStream termsOut, idsOut, idsToTermOut, dfByTermOut, cfByTermOut,
        dfByIntOut, cfByIntOut;
    private int numTerms;
    private int[] seqNums = null;
    private int[] dfs = null;
    private long[] cfs = null;
    private int curKeyIndex = 0;

    private String[] terms;

    @Override
    public void setup(Reducer<Text, PairOfIntLong, NullWritable, NullWritable>.Context context)
        throws IOException {
      LOG.info("Starting setup.");
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      RetrievalEnvironment env = new RetrievalEnvironment(conf.get(Constants.IndexPath), fs);

      numTerms = conf.getInt(Constants.CollectionTermCount, 0);

      terms = new String[numTerms];
      seqNums = new int[numTerms];
      dfs = new int[numTerms];
      cfs = new long[numTerms];

      termsOut = fs.create(new Path(env.getIndexTermsData()), true);
      // termsOut.writeInt(numTerms);

      idsOut = fs.create(new Path(env.getIndexTermIdsData()), true);
      idsOut.writeInt(numTerms);

      idsToTermOut = fs.create(new Path(env.getIndexTermIdMappingData()), true);
      idsToTermOut.writeInt(numTerms);

      dfByTermOut = fs.create(new Path(env.getDfByTermData()), true);
      dfByTermOut.writeInt(numTerms);

      cfByTermOut = fs.create(new Path(env.getCfByTermData()), true);
      cfByTermOut.writeInt(numTerms);

      dfByIntOut = fs.create(new Path(env.getDfByIntData()), true);
      dfByIntOut.writeInt(numTerms);

      cfByIntOut = fs.create(new Path(env.getCfByIntData()), true);
      cfByIntOut.writeInt(numTerms);
      LOG.info("Finished setup.");
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

      // termsOut.writeUTF(term);

      terms[curKeyIndex] = term;
      seqNums[curKeyIndex] = curKeyIndex;
      dfs[curKeyIndex] = -df;
      cfs[curKeyIndex] = cf;
      curKeyIndex++;

      context.getCounter(Terms.Total).increment(1);
    }

    @Override
    public void cleanup(Reducer<Text, PairOfIntLong, NullWritable, NullWritable>.Context context)
        throws IOException {
      LOG.info("Starting cleanup.");
      if (curKeyIndex != numTerms) {
        throw new RuntimeException("Total expected Terms: " + numTerms + ", Total observed terms: "
            + curKeyIndex + "!");
      }
      // Sort based on df and change seqNums accordingly.
      QuickSort.quicksortWithSecondary(seqNums, dfs, cfs, 0, numTerms - 1);

      // Write sorted dfs and cfs by int here.
      for (int i = 0; i < numTerms; i++) {
        WritableUtils.writeVInt(dfByIntOut, -dfs[i]);
        WritableUtils.writeVLong(cfByIntOut, cfs[i]);
      }
      cfs = null;

      // Encode the sorted dfs into ids ==> df values erased and become ids instead. Note that first
      // term id is 1.
      for (int i = 0; i < numTerms; i++) {
        dfs[i] = i + 1;
      }

      // Write current seq nums to be index into the term array.
      for (int i = 0; i < numTerms; i++)
        idsToTermOut.writeInt(seqNums[i]);

      // Sort on seqNums to get the right writing order.
      QuickSort.quicksort(dfs, seqNums, 0, numTerms - 1);
      for (int i = 0; i < numTerms; i++) {
        idsOut.writeInt(dfs[i]);
      }

      ByteArrayOutputStream bytesOut;
      ObjectOutputStream objOut;
      byte[] bytes;

      List<String> termList = Lists.newArrayList(terms);
      FrontCodedStringList frontcodedList = new FrontCodedStringList(termList, 8, true);

      bytesOut = new ByteArrayOutputStream();
      objOut = new ObjectOutputStream(bytesOut);
      objOut.writeObject(frontcodedList);
      objOut.close();

      bytes = bytesOut.toByteArray();
      termsOut.writeInt(bytes.length);
      termsOut.write(bytes);

      ShiftAddXorSignedStringMap dict = new ShiftAddXorSignedStringMap(termList.iterator(),
          new TwoStepsLcpMonotoneMinimalPerfectHashFunction<CharSequence>(termList,
              new DictionaryTransformationStrategy(true)));

      bytesOut = new ByteArrayOutputStream();
      objOut = new ObjectOutputStream(bytesOut);
      objOut.writeObject(dict);
      objOut.close();

      bytes = bytesOut.toByteArray();
      termsOut.writeInt(bytes.length);
      termsOut.write(bytes);

      termsOut.close();
      idsOut.close();
      idsToTermOut.close();
      dfByTermOut.close();
      cfByTermOut.close();
      dfByIntOut.close();
      cfByIntOut.close();
      LOG.info("Finished cleanup.");
    }
  }

  public static final String[] RequiredParameters = { Constants.CollectionName, Constants.IndexPath };

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
    String collectionName = conf.get(Constants.CollectionName);

    LOG.info("PowerTool: " + BuildDictionary.class.getCanonicalName());
    LOG.info(String.format(" - %s: %s", Constants.CollectionName, collectionName));
    LOG.info(String.format(" - %s: %s", Constants.IndexPath, indexPath));

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    if (!fs.exists(new Path(indexPath))) {
      LOG.error("index path doesn't existing: skipping!");
      return 0;
    }

    if (fs.exists(new Path(env.getIndexTermsData()))
        && fs.exists(new Path(env.getIndexTermIdsData()))
        && fs.exists(new Path(env.getIndexTermIdMappingData()))
        && fs.exists(new Path(env.getDfByTermData())) && fs.exists(new Path(env.getCfByTermData()))
        && fs.exists(new Path(env.getDfByIntData())) && fs.exists(new Path(env.getCfByIntData()))) {
      LOG.info("term and term id data exist: skipping!");
      return 0;
    }

    conf.setInt(Constants.CollectionTermCount, (int) env.readCollectionTermCount());
    conf.set("mapred.child.java.opts", "-Xmx2048m");

    Path tmpPath = new Path(env.getTempDirectory());
    fs.delete(tmpPath, true);

    Job job = new Job(conf, BuildDictionary.class.getSimpleName() + ":" + collectionName);

    job.setJarByClass(BuildDictionary.class);
    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(env.getTermDfCfDirectory()));
    FileOutputFormat.setOutputPath(job, tmpPath);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfIntLong.class);
    job.setOutputKeyClass(Text.class);
    job.setSortComparatorClass(DictionaryTransformationStrategy.WritableComparator.class);

    job.setMapperClass(Mapper.class);
    job.setReducerClass(MyReducer.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    fs.delete(tmpPath, true);

    return 0;
  }
}
