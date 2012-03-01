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
import ivory.core.data.document.IntDocVector;
import ivory.core.data.index.IntPostingsForwardIndex;
import ivory.core.preprocess.PositionalSequenceFileRecordReader;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.PowerTool;

public class BuildIntPostingsForwardIndex extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildIntPostingsForwardIndex.class);

  protected static enum Dictionary { Size };

  private static class MyMapper
      extends Mapper<IntWritable, IntDocVector, IntWritable, Text> {
    @Override
    public void run(Context context) throws IOException, InterruptedException {
      String file = ((FileSplit) context.getInputSplit()).getPath().getName();
      LOG.info("Input file: " + file);

      IntWritable key = new IntWritable();
      Text outputValue = new Text();

      PositionalSequenceFileRecordReader<IntWritable, IntDocVector> reader =
          new PositionalSequenceFileRecordReader<IntWritable, IntDocVector>();
      reader.initialize(context.getInputSplit(), context);

      int fileNo = Integer.parseInt(file.substring(file.lastIndexOf("-") + 1));
      long pos = reader.getPosition();
      while (reader.nextKeyValue()) {
        key = reader.getCurrentKey();
        outputValue.set(fileNo + "\t" + pos);

        context.write(key, outputValue);
        context.getCounter(Dictionary.Size).increment(1);

        pos = reader.getPosition();
      }
      reader.close();
    }
  }

  private static class MyReducer extends Reducer<IntWritable, Text, Text, Text> {
    private String positionsFile;
    private int collectionTermCount;
    private FSDataOutputStream out;
    private int curKeyIndex = 0;

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      FileSystem fs;
      try {
        fs = FileSystem.get(conf);
      } catch (Exception e) {
        throw new RuntimeException("Error opening the FileSystem!");
      }

      String indexPath = conf.get(Constants.IndexPath);

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

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      Iterator<Text> iter = values.iterator();
      String[] s = iter.next().toString().split("\\s+");

      if (iter.hasNext()) {
        throw new RuntimeException("There shouldn't be more than one value, key=" + key);
      }

      int fileNo = Integer.parseInt(s[0]);
      long filePos = Long.parseLong(s[1]);
      long pos = IntPostingsForwardIndex.BigNumber * fileNo + filePos;

      curKeyIndex++;

      // This is subtle point: Ivory.CollectionTermCount specifies the number of terms in the
      // collection, computed by BuildTermIdMap. However, this number is sometimes greater than the
      // number of postings that are in the index (as is the case for ClueWeb09). Here's what
      // happens: when creating TermDocVectors, the vocabulary gets populated with tokens that
      // contain special symbols. The vocabulary then gets compressed with front-coding. However,
      // for whatever reason, the current implementation cannot properly handle these special
      // characters. So when we convert from TermDocVectors to IntDocVectors, we can't find the term
      // id for these special tokens. As a result, no postings list get created for them. So there
      // are assigned term ids for which there are no postings. Since IntPostingsForwardIndex
      // assumes consecutive term ids (since it loads position offsets into an array), we must
      // insert "padding".
      // - Jimmy, 5/29/2010

      while (curKeyIndex < key.get()) {
        out.writeLong(-1);
        curKeyIndex++;
      }

      out.writeLong(pos);
    }

    @Override
    public void cleanup(Context context) throws IOException {
      // Insert padding at the end.
      while (curKeyIndex < collectionTermCount) {
        out.writeLong(-1);
        curKeyIndex++;
      }

      out.close();

      if (curKeyIndex != collectionTermCount) {
        throw new IOException(String.format("Expected %d terms, actually got %d terms!",
            collectionTermCount, curKeyIndex));
      }
    }
  }

  public BuildIntPostingsForwardIndex(Configuration conf) {
    super(conf);
  }

  public static final String[] RequiredParameters = { Constants.IndexPath };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public int runTool() throws Exception {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    int minSplitSize = conf.getInt(Constants.MinSplitSize, 0);
    String indexPath = conf.get(Constants.IndexPath);

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    String collectionName = env.readCollectionName();

    Job job = new Job(getConf(),
        BuildIntPostingsForwardIndex.class.getSimpleName() + ":" + collectionName);
    job.setJarByClass(BuildIntPostingsForwardIndex.class);

    Path inputPath = new Path(env.getPostingsDirectory());
    FileInputFormat.setInputPaths(job, inputPath);

    Path postingsIndexPath = new Path(env.getPostingsIndexData());

    if (fs.exists(postingsIndexPath)) {
      LOG.info("Postings forward index path already exists!");
      return 0;
    }
    job.setNumReduceTasks(1);

    LOG.info("Tool: " + BuildIntPostingsForwardIndex.class.getCanonicalName());
    LOG.info(String.format(" - %s: %s", Constants.IndexPath, indexPath));
    LOG.info(String.format(" - %s: %s", Constants.CollectionName, collectionName));
    LOG.info(String.format(" - %s: %s", "Input Path", inputPath));
    LOG.info(String.format(" - %s: %s", "Output Postings Forward Index Path", postingsIndexPath));

    conf.setInt("mapred.min.split.size", minSplitSize);
    conf.set("mapred.child.java.opts", "-Xmx2048m");

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    job.waitForCompletion(true);

    return 0;
  }
}
