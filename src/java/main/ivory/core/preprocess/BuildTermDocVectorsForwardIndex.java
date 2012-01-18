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
import ivory.core.data.document.IntDocVector;
import ivory.core.data.document.TermDocVectorsForwardIndex;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

public class BuildTermDocVectorsForwardIndex extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildTermDocVectorsForwardIndex.class);

  protected static enum Dictionary {
    Size
  };

  private static class MyMapper extends Mapper<IntWritable, IntDocVector, IntWritable, Text> {
    private static final Text output = new Text();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
      String file = ((FileSplit) context.getInputSplit()).getPath().getName();
      LOG.info("Input file: " + file);

      PositionalSequenceFileRecordReader<IntWritable, IntDocVector> reader = new PositionalSequenceFileRecordReader<IntWritable, IntDocVector>();
      reader.initialize(context.getInputSplit(), context);

      int fileNo = Integer.parseInt(file.substring(file.lastIndexOf("-") + 1));
      long filePos = reader.getPosition();
      while (reader.nextKeyValue()) {
        IntWritable key = reader.getCurrentKey();
        output.set(fileNo + "\t" + filePos);

        context.write(key, output);
        context.getCounter(Dictionary.Size).increment(1);

        filePos = reader.getPosition();
      }
      reader.close();
    }
  }

  private static class MyReducer extends Reducer<IntWritable, Text, NullWritable, NullWritable> {
    FSDataOutputStream out;

    int collectionDocumentCount;
    int curDoc = 0;

    @Override
    public void setup(Reducer<IntWritable, Text, NullWritable, NullWritable>.Context context) {
      Configuration conf = context.getConfiguration();
      FileSystem fs;
      try {
        fs = FileSystem.get(conf);
      } catch (Exception e) {
        throw new RuntimeException("Error opening the FileSystem!");
      }

      RetrievalEnvironment env = null;
      try {
        env = new RetrievalEnvironment(conf.get(Constants.IndexPath), fs);
      } catch (IOException e) {
        throw new RuntimeException("Unable to create RetrievalEnvironment!");
      }

      collectionDocumentCount = env.readCollectionDocumentCount();

      try {
        out = fs.create(new Path(env.getTermDocVectorsForwardIndex()), true);
        out.writeInt(env.readDocnoOffset());
        out.writeInt(collectionDocumentCount);
      } catch (Exception e) {
        throw new RuntimeException("Error in creating files!");
      }
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
      Iterator<Text> iter = values.iterator();
      String[] s = iter.next().toString().split("\\s+");

      if (iter.hasNext()) {
        throw new RuntimeException("There shouldn't be more than one value, key=" + key);
      }

      int fileNo = Integer.parseInt(s[0]);
      long filePos = Long.parseLong(s[1]);
      long pos = TermDocVectorsForwardIndex.BigNumber * fileNo + filePos;

      curDoc++;

      out.writeLong(pos);
    }

    @Override
    public void cleanup(Reducer<IntWritable, Text, NullWritable, NullWritable>.Context context)
        throws IOException {
      out.close();

      if (curDoc != collectionDocumentCount) {
        throw new IOException("Expected " + collectionDocumentCount + " docs, actually got "
            + curDoc + " terms!");
      }
    }
  }

  public BuildTermDocVectorsForwardIndex(Configuration conf) {
    super(conf);
  }

  public static final String[] RequiredParameters = { Constants.IndexPath };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public int runTool() throws Exception {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    String indexPath = conf.get(Constants.IndexPath);
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    String collectionName = env.readCollectionName();

    LOG.info("Tool: " + BuildTermDocVectorsForwardIndex.class.getCanonicalName());
    LOG.info(String.format(" - %s: %s", Constants.CollectionName, collectionName));
    LOG.info(String.format(" - %s: %s", Constants.IndexPath, indexPath));

    if (!fs.exists(new Path(env.getTermDocVectorsDirectory()))) {
      LOG.info("Error: TermDocVectors don't exist!");
      return 0;
    }

    if (fs.exists(new Path(env.getTermDocVectorsForwardIndex()))) {
      LOG.info("TermDocVectorIndex already exists: skipping!");
      return 0;
    }

    Job job = new Job(conf, BuildTermDocVectorsForwardIndex.class.getSimpleName() + ":"
        + collectionName);
    job.setJarByClass(BuildTermDocVectorsForwardIndex.class);

    FileInputFormat.setInputPaths(job, new Path(env.getTermDocVectorsDirectory()));
    job.setNumReduceTasks(1);

    job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }
}
