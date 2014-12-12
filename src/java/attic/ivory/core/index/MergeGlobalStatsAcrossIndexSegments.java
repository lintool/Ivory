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

import ivory.core.RetrievalEnvironment;
import ivory.core.data.dictionary.PrefixEncodedLexicographicallySortedDictionary;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfIntLong;
import edu.umd.cloud9.util.PowerTool;

public class MergeGlobalStatsAcrossIndexSegments extends PowerTool {
  private static final Logger LOG = Logger.getLogger(MergeGlobalStatsAcrossIndexSegments.class);

  private static class MyMapper extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, PairOfIntLong> {
    private static final PairOfIntLong stats = new PairOfIntLong();
    private static final Text sTerm = new Text();
    private int mDfThreshold;

    public void configure(JobConf job) {
      mDfThreshold = job.getInt("Ivory.DfThreshold", 0);
    }

    public void map(LongWritable key, Text p, OutputCollector<Text, PairOfIntLong> output,
        Reporter reporter) throws IOException {

      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);

      LOG.info(p);
      String indexPath = p.toString();
      RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

      Path termsFilePath = new Path(env.getIndexTermsData());
      Path dfByTermFilePath = new Path(env.getDfByTermData());
      Path cfByTermFilePath = new Path(env.getCfByTermData());

      FSDataInputStream in = fs.open(termsFilePath);
      FSDataInputStream inDfs = fs.open(dfByTermFilePath);
      FSDataInputStream inCfs = fs.open(cfByTermFilePath);

      // ignore the first int, which is the count
      inDfs.readInt();
      inCfs.readInt();

      int curKeyIndex = 0;
      byte[] keys;

      curKeyIndex = in.readInt();
      String prev = "";

      int window = in.readInt();
      for (int i = 0; i < curKeyIndex; i++) {
        if (i % window != 0) { // not a root
          int suffix = in.readByte();
          if (suffix < 0)
            suffix += 256;
          keys = new byte[suffix];

          int prefix = in.readByte();
          if (prefix < 0) {
            prefix += 256;
          }

          for (int j = 0; j < keys.length; j++)
            keys[j] = in.readByte();

          String term = prev.substring(0, prefix) + new String(keys);
          prev = term;

          sTerm.set(term);
        } else {
          int suffix = in.readByte();
          if (suffix < 0)
            suffix += 256;
          keys = new byte[suffix];
          for (int j = 0; j < keys.length; j++)
            keys[j] = in.readByte();
          String term = new String(keys);
          prev = term;

          sTerm.set(term);
        }

        int df = WritableUtils.readVInt(inDfs);
        long cf = WritableUtils.readVInt(inCfs);

        if (df > mDfThreshold) {
          stats.set(df, cf);
          output.collect(sTerm, stats);
        }
      }

      in.close();
      inDfs.close();
      inCfs.close();
    }
  }

  private static class MyReducer extends MapReduceBase implements
      Reducer<Text, PairOfIntLong, Text, Text> {

    String termsFile;
    String dfStatsFile;
    String cfStatsFile;
    int nTerms;
    int window;

    FileSystem fileSys;

    FSDataOutputStream termsOut;
    FSDataOutputStream dfStatsOut;
    FSDataOutputStream cfStatsOut;

    public void close() throws IOException {
      super.close();
      termsOut.close();
      dfStatsOut.close();
      cfStatsOut.close();
    }

    public void configure(JobConf job) {
      try {
        fileSys = FileSystem.get(job);
      } catch (Exception e) {
        throw new RuntimeException("error in fileSys");
      }

      String path = job.get("Ivory.DataOutputPath");

      termsFile = path + "/dict.terms";
      dfStatsFile = path + "/dict.df";
      cfStatsFile = path + "/dict.cf";

      nTerms = job.getInt("Ivory.IndexNumberOfTerms", 0);
      window = 8;

      LOG.info("Ivory.PrefixEncodedTermsFile: " + termsFile);
      LOG.info("Ivory.DFStatsFile: " + dfStatsFile);
      LOG.info("Ivory.CFStatsFile: " + cfStatsFile);
      LOG.info("Ivory.IndexNumberOfTerms: " + nTerms);
      LOG.info("Ivory.ForwardIndexWindow: " + window);

      try {
        termsOut = fileSys.create(new Path(termsFile), true);
        dfStatsOut = fileSys.create(new Path(dfStatsFile), true);
        cfStatsOut = fileSys.create(new Path(cfStatsFile), true);
        termsOut.writeInt(nTerms);
        termsOut.writeInt(window);
        dfStatsOut.writeInt(nTerms);
        cfStatsOut.writeInt(nTerms);
      } catch (Exception e) {
        throw new RuntimeException("error in creating files");
      }

    }

    int curKeyIndex = 0;
    String lastKey = "";

    public void reduce(Text key, Iterator<PairOfIntLong> values,
        OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

      String term = key.toString();

      int prefixLength;

      int df = 0;
      long cf = 0;

      while (values.hasNext()) {
        PairOfIntLong p = values.next();
        df += p.getLeftElement();
        cf += p.getRightElement();
      }

      LOG.info(key + " " + df + " " + cf);

      if (curKeyIndex % window == 0) {
        byte[] byteArray = term.getBytes();
        termsOut.writeByte((byte) (byteArray.length)); // suffix length
        for (int j = 0; j < byteArray.length; j++)
          termsOut.writeByte(byteArray[j]);
      } else {
        prefixLength = PrefixEncodedLexicographicallySortedDictionary.getPrefix(lastKey, term);
        byte[] suffix = term.substring(prefixLength).getBytes();

        if (prefixLength > Byte.MAX_VALUE || suffix.length > Byte.MAX_VALUE)
          throw new RuntimeException("prefix/suffix length overflow");

        termsOut.writeByte((byte) suffix.length); // suffix length
        termsOut.writeByte((byte) prefixLength); // prefix length
        for (int j = 0; j < suffix.length; j++)
          termsOut.writeByte(suffix[j]);
      }
      lastKey = term;
      curKeyIndex++;

      WritableUtils.writeVInt(dfStatsOut, df);
      WritableUtils.writeVLong(cfStatsOut, cf);
    }
  }

  public static final String[] RequiredParameters = { "Ivory.CollectionName", "Ivory.IndexPaths",
          "Ivory.DfThreshold", "Ivory.DataOutputPath" };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public MergeGlobalStatsAcrossIndexSegments(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {

    JobConf conf = new JobConf(getConf(), MergeGlobalStatsAcrossIndexSegments.class);
    FileSystem fs = FileSystem.get(conf);

    String collectionName = conf.get("Ivory.CollectionName");
    String indexPaths = conf.get("Ivory.IndexPaths");
    String dataOutputPath = conf.get("Ivory.DataOutputPath");
    int dfThreshold = conf.getInt("Ivory.DfThreshold", 0);

    // first, compute size of global term space
    Path tmpPaths = new Path("/tmp/index-paths.txt");

    FSDataOutputStream out = fs.create(tmpPaths, true);
    for (String s : indexPaths.split(",")) {
      out.write(new String(s + "\n").getBytes());
    }
    out.close();

    LOG.info("Job: ComputeNumberOfTermsAcrossIndexSegments");
    conf.setJobName("ComputeNumberOfTermsAcrossIndexSegments:" + collectionName);

    FileInputFormat.addInputPath(conf, tmpPaths);

    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);

    conf.set("mapred.child.java.opts", "-Xmx2048m");

    conf.setInputFormat(NLineInputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(PairOfIntLong.class);
    conf.setOutputFormat(NullOutputFormat.class);

    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(IdentityReducer.class);

    long startTime = System.currentTimeMillis();
    RunningJob job = JobClient.runJob(conf);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    Counters counters = job.getCounters();

    long totalNumTerms = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", 6,
        "REDUCE_INPUT_GROUPS").getCounter();

    LOG.info("total number of terms in global dictionary = " + totalNumTerms);

    // now build the dictionary
    fs.delete(new Path(dataOutputPath), true);

    conf = new JobConf(getConf(), MergeGlobalStatsAcrossIndexSegments.class);

    LOG.info("Job: MergeGlobalStatsAcrossIndexSegments");
    conf.setJobName("MergeGlobalStatsAcrossIndexSegments:" + collectionName);

    FileInputFormat.addInputPath(conf, tmpPaths);

    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);

    conf.set("mapred.child.java.opts", "-Xmx2048m");

    conf.setInputFormat(NLineInputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(PairOfIntLong.class);
    conf.setOutputFormat(NullOutputFormat.class);

    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(MyReducer.class);

    conf.setLong("Ivory.IndexNumberOfTerms", (int) totalNumTerms);

    startTime = System.currentTimeMillis();
    job = JobClient.runJob(conf);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    // compute some # docs, collection length, avg doc length
    long collectionLength = 0;
    int docCount = 0;
    for (String index : indexPaths.split(",")) {
      LOG.info("reading stats for " + index);

      RetrievalEnvironment env = new RetrievalEnvironment(index, fs);

      long l = env.readCollectionLength();
      int n = env.readCollectionDocumentCount();

      LOG.info(" - CollectionLength: " + l);
      LOG.info(" - CollectionDocumentCount: " + n);

      collectionLength += l;
      docCount += n;
    }

    float avgdl = (float) collectionLength / docCount;

    LOG.info("all index segments: ");
    LOG.info(" - CollectionLength: " + collectionLength);
    LOG.info(" - CollectionDocumentCount: " + docCount);
    LOG.info(" - AverageDocumentLenght: " + avgdl);

    RetrievalEnvironment env = new RetrievalEnvironment(dataOutputPath, fs);

    env.writeCollectionAverageDocumentLength(avgdl);
    env.writeCollectionLength(collectionLength);
    env.writeCollectionDocumentCount(docCount);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    if (args.length < 4) {
      System.err
          .println("Usage: [collection-name] [output-path] [df-threshold] [index1] [index2] ...");
      System.exit(-1);
    }

    String collectionName = args[0];
    String outputPath = args[1];
    int dfThreshold = Integer.parseInt(args[2]);

    LOG.info("Merging global statistics across index segments...");
    LOG.info(" CollectionName: " + collectionName);
    LOG.info(" OutputPath: " + outputPath);
    LOG.info(" DfThreshold: " + dfThreshold);

    LOG.info(" IndexPaths: ");
    StringBuffer sb = new StringBuffer();
    for (int i = 3; i < args.length; i++) {
      LOG.info("    Adding" + args[i]);
      sb.append(args[i]);
      if (i != args.length - 1)
        sb.append(",");
    }

    conf.set("Ivory.CollectionName", collectionName);
    conf.set("Ivory.IndexPaths", sb.toString());
    conf.set("Ivory.DataOutputPath", outputPath);
    conf.setInt("Ivory.DfThreshold", dfThreshold);

    new MergeGlobalStatsAcrossIndexSegments(conf).run();
  }
}
