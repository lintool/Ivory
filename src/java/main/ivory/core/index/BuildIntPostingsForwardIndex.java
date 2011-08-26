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
import ivory.core.data.index.PostingsList;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.PowerTool;

@SuppressWarnings("deprecation")
public class BuildIntPostingsForwardIndex extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildIntPostingsForwardIndex.class);
  protected static enum Dictionary { Size };

  private static class MyMapRunner implements
      MapRunnable<IntWritable, PostingsList, IntWritable, Text> {
    private String inputFile;
    private Text outputValue = new Text();

    public void configure(JobConf job) {
      inputFile = job.get("map.input.file");
    }

    public void run(RecordReader<IntWritable, PostingsList> input,
        OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      IntWritable key = input.createKey();
      PostingsList value = input.createValue();
      int fileNo = Integer.parseInt(inputFile.substring(inputFile.lastIndexOf("-") + 1));

      long pos = input.getPos();
      while (input.next(key, value)) {
        outputValue.set(fileNo + "\t" + pos);

        output.collect(key, outputValue);
        reporter.incrCounter(Dictionary.Size, 1);

        pos = input.getPos();
      }
      LOG.info("last termid: " + key + "(" + fileNo + ", " + pos + ")");
    }
  }

  public static final long BIG_LONG_NUMBER = 1000000000;

  private static class MyReducer extends MapReduceBase implements
      Reducer<IntWritable, Text, Text, Text> {
    private String positionsFile;
    private int collectionTermCount;
    private FSDataOutputStream out;
    private int curKeyIndex = 0;

    public void configure(JobConf job) {
      FileSystem fs;
      try {
        fs = FileSystem.get(job);
      } catch (Exception e) {
        throw new RuntimeException("Error opening the FileSystem!");
      }

      String indexPath = job.get("Ivory.IndexPath");

      RetrievalEnvironment env = null;
      try {
        env = new RetrievalEnvironment(indexPath, fs);
      } catch (IOException e) {
        throw new RuntimeException("Unable to create RetrievalEnvironment!");
      }

      positionsFile = env.getPostingsIndexData();
      collectionTermCount = env.readCollectionTermCount();

      LOG.info("Ivory.PostingsPositionsFile: " + positionsFile);
      LOG.info("Ivory.CollectionTermCount: " + collectionTermCount);

      try {
        out = fs.create(new Path(positionsFile), true);
        out.writeInt(collectionTermCount);
      } catch (Exception e) {
        throw new RuntimeException("Error in creating files!");
      }
    }

    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String[] s = values.next().toString().split("\\s+");

      if (values.hasNext()) {
        throw new RuntimeException("There shouldn't be more than one value, key=" + key);
      }

      int fileNo = Integer.parseInt(s[0]);
      long filePos = Long.parseLong(s[1]);
      long pos = BIG_LONG_NUMBER * fileNo + filePos;

      curKeyIndex++;

      // This is subtle point: Ivory.CollectionTermCount specifies the
      // number of terms in the collection, computed by
      // BuildTermIdMap. However, this number is sometimes greater than
      // the number of postings that are in the index (as is the case for
      // ClueWeb09). Here's what happens: when creating TermDocVectors,
      // the vocabulary gets populated with tokens that contain special
      // symbols. The vocabulary then gets compressed with front-coding.
      // However, for whatever reason, the current implementation cannot
      // properly handle these special characters. So when we convert from
      // TermDocVectors to IntDocVectors, we can't find the term id for
      // these special tokens. As a result, no postings list get created
      // for them. So there are assigned term ids for which there are no
      // postings. Since IntPostingsForwardIndex assumes consecutive term
      // ids (since it loads position offsets into an array), we must
      // insert "padding".
      // - Jimmy, 5/29/2010

      while (curKeyIndex < key.get()) {
        out.writeLong(-1);
        curKeyIndex++;
      }

      out.writeLong(pos);
    }

    public void close() throws IOException {
      // insert padding at the end
      while (curKeyIndex < collectionTermCount) {
        out.writeLong(-1);
        curKeyIndex++;
      }

      out.close();

      if (curKeyIndex != collectionTermCount) {
        throw new IOException("Expected " + collectionTermCount + " terms, actually got "
            + curKeyIndex + " terms!");
      }
    }
  }

  public BuildIntPostingsForwardIndex(Configuration conf) {
    super(conf);
  }

  public static final String[] RequiredParameters = { "Ivory.IndexPath", "Ivory.NumMapTasks" };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public int runTool() throws Exception {
    JobConf conf = new JobConf(getConf(), BuildIntPostingsForwardIndex.class);
    FileSystem fs = FileSystem.get(conf);

    int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
    int minSplitSize = conf.getInt("Ivory.MinSplitSize", 0);
    String indexPath = conf.get("Ivory.IndexPath");

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    String collectionName = env.readCollectionName();

    LOG.info("Tool: BuildIntPostingsForwardIndex");
    LOG.info(" - IndexPath: " + indexPath);
    LOG.info(" - CollectionName: " + collectionName);

    conf.setJobName("BuildIntPostingsForwardIndex:" + collectionName);

    Path inputPath = new Path(env.getPostingsDirectory());
    FileInputFormat.setInputPaths(conf, inputPath);

    Path postingsIndexPath = new Path(env.getPostingsIndexData());

    if (fs.exists(postingsIndexPath)) {
      LOG.info("Postings forward index path already exists!");
      return 0;
    }
    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(1);

    conf.setInt("mapred.min.split.size", minSplitSize);
    conf.set("mapred.child.java.opts", "-Xmx2048m");

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setOutputFormat(NullOutputFormat.class);

    conf.setMapRunnerClass(MyMapRunner.class);
    conf.setReducerClass(MyReducer.class);

    JobClient.runJob(conf);

    return 0;
  }
}
