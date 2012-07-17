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
import ivory.core.data.document.LazyTermDocVector;
import ivory.core.data.document.TermDocVector;
import ivory.core.tokenize.DocumentProcessingUtils;
import ivory.core.tokenize.Tokenizer;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.mapreduce.NullInputFormat;
import edu.umd.cloud9.mapreduce.NullMapper;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.array.ArrayListOfInts;
import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.MapII;

public class BuildTermDocVectors extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildTermDocVectors.class);

  protected static enum Docs { Skipped, Total, Empty }
  protected static enum MapTime { Spilling, Parsing }
  protected static enum DocLengths { Count, SumOfDocLengths, Files }

  protected static class MyMapper extends Mapper<Writable, Indexable, IntWritable, TermDocVector> {
    private static final IntWritable key = new IntWritable();
    private static final LazyTermDocVector docVector = new LazyTermDocVector();
    private static final HMapII doclengths = new HMapII();

    private Tokenizer tokenizer;
    private DocnoMapping docMapping;
    private int docno;

    @Override
    public void setup(Mapper<Writable, Indexable, IntWritable, TermDocVector>.Context context)
        throws IOException {
      Configuration conf = context.getConfiguration();

      try {
        FileSystem localFs = FileSystem.getLocal(conf);
        docMapping =
          (DocnoMapping) Class.forName(conf.get(Constants.DocnoMappingClass)).newInstance();

        // Take a different code path if we're in standalone mode.
        if (conf.get("mapred.job.tracker").equals("local")) {
          RetrievalEnvironment env = new RetrievalEnvironment(
              context.getConfiguration().get(Constants.IndexPath), localFs);
          docMapping.loadMapping(env.getDocnoMappingData(), localFs);
        } else {
          Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
          // Load the docid to docno mappings. Assume file 0.
          docMapping.loadMapping(localFiles[0], localFs);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error initializing docno mapping!", e);
      }

      // Initialize the tokenizer.
      try {
        tokenizer = (Tokenizer) Class.forName(conf.get(Constants.Tokenizer)).newInstance();
        tokenizer.configure(conf);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing tokenizer!");
      }
    }

    @Override
    public void map(Writable in, Indexable doc, Context context)
        throws IOException, InterruptedException {
      docno = docMapping.getDocno(doc.getDocid());

      // Skip invalid docnos.
      if (docno <= 0) {
        context.getCounter(Docs.Skipped).increment(1);
        return;
      }

      long startTime;

      startTime = System.currentTimeMillis();
      Map<String, ArrayListOfInts> termPositionsMap =
          DocumentProcessingUtils.parseDocument(doc, tokenizer);
      context.getCounter(MapTime.Parsing).increment(System.currentTimeMillis() - startTime);

      int doclength;
      if (termPositionsMap.size() == 0) {
        context.getCounter(Docs.Empty).increment(1);
        doclength = 0;
      } else {
        doclength = termPositionsMap.get("").get(0);
        termPositionsMap.remove("");
      }

      startTime = System.currentTimeMillis();
      key.set(docno);
      docVector.setTermPositionsMap(termPositionsMap);
      context.write(key, docVector);
      context.getCounter(MapTime.Spilling).increment(System.currentTimeMillis() - startTime);
      context.getCounter(Docs.Total).increment(1);

      doclengths.put(docno, doclength);
    }

    @Override
    public void cleanup(Mapper<Writable, Indexable, IntWritable, TermDocVector>.Context context)
          throws IOException, InterruptedException {
      // Now we want to write out the doclengths as "side data" onto HDFS.
      // Since speculative execution is on, we'll append the task id to
      // the filename to guarantee uniqueness. However, this means that
      // the may be multiple files with the same doclength information,
      // which we have the handle when we go to write out the binary
      // encoding of the data.

      if (doclengths.size() == 0) {
        throw new RuntimeException("Error: Doclength table empty!");
      }

      long bytesCnt = 0;
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String indexPath = conf.get("Ivory.IndexPath");

      FileSystem fs = FileSystem.get(conf);
      // Use the last processed docno as the file name + task id.
      Path path = new Path(indexPath + "/doclengths/" + docno + "." + taskId);
      FSDataOutputStream out = fs.create(path, false);

      // Iterate through the docs and write out doclengths.
      long dlSum = 0;
      int cnt = 0;
      for (MapII.Entry e : doclengths.entrySet()) {
        String s = e.getKey() + "\t" + e.getValue() + "\n";
        out.write(s.getBytes());
        bytesCnt += s.getBytes().length;
        cnt++;
        dlSum += e.getValue();
      }
      out.close();

      // We want to check if the file has actually been written successfully...
      LOG.info("Expected length of doclengths file: " + bytesCnt);

      long bytesActual = fs.listStatus(path)[0].getLen();
      LOG.info("Actual length of doclengths file: " + bytesActual);

      if (bytesCnt == 0) {
        throw new RuntimeException("Error: zero bytesCnt at " + path);
      } else if (bytesActual == 0) {
        throw new RuntimeException("Error: zero bytesActual at " + path);
      } else if (bytesCnt != bytesActual) {
        throw new RuntimeException(String.format("Error writing Doclengths file: %d %d %s",
            bytesCnt, bytesActual, path.toString()));
      }

      context.getCounter(DocLengths.Count).increment(cnt);
      // Sum of the document lengths, should match sum of tfs.
      context.getCounter(DocLengths.SumOfDocLengths).increment(dlSum);
    }
  }

  private static class DocLengthDataWriterMapper extends NullMapper {
    @Override
    public void run(Mapper<NullWritable, NullWritable, NullWritable, NullWritable>.Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int collectionDocCount = conf.getInt(Constants.CollectionDocumentCount, -1);
      String inputPath = conf.get(InputPath);
      String dataFile = conf.get(DocLengthDataFile);

      int docnoOffset = conf.getInt(Constants.DocnoOffset, 0);

      Path p = new Path(inputPath);

      LOG.info("InputPath: " + inputPath);
      LOG.info("DocLengthDataFile: " + dataFile);
      LOG.info("DocnoOffset: " + docnoOffset);

      FileSystem fs = FileSystem.get(conf);
      FileStatus[] fileStats = fs.listStatus(p);

      int[] doclengths = new int[collectionDocCount + 1]; // Initial array to hold the doclengths.
      int maxDocno = 0; // Largest docno.
      int minDocno = Integer.MAX_VALUE; // Smallest docno.

      int nFiles = fileStats.length;
      for (int i = 0; i < nFiles; i++) {
        // Skip log files
        if (fileStats[i].getPath().getName().startsWith("_")) {
          continue;
        }

        LOG.info("processing " + fileStats[i].getPath());
        LineReader reader = new LineReader(fs.open(fileStats[i].getPath()));

        Text line = new Text();
        while (reader.readLine(line) > 0) {
          String[] arr = line.toString().split("\\t+", 2);

          int docno = Integer.parseInt(arr[0]);
          int len = Integer.parseInt(arr[1]);

          // Note that because of speculative execution there may be
          // multiple copies of doclength data. Therefore, we can't
          // just count number of doclengths read. Instead, keep track
          // of largest docno encountered.
          if (docno < docnoOffset) {
            throw new RuntimeException(
                "Error: docno " + docno + " < docnoOffset " + docnoOffset + "!");
          }

          doclengths[docno - docnoOffset] = len;

          if (docno > maxDocno) {
            maxDocno = docno;
          }
          if (docno < minDocno) {
            minDocno = docno;
          }
        }
        reader.close();
        context.getCounter(DocLengths.Files).increment(1);
      }

      LOG.info("min docno: " + minDocno);
      LOG.info("max docno: " + maxDocno);

      // Write out the doc length data into a single file.
      FSDataOutputStream out = fs.create(new Path(dataFile), true);

      out.writeInt(docnoOffset); // Write out the docno offset.
      out.writeInt(maxDocno - docnoOffset); // Write out the collection size.

      // Write out length of each document (docnos are sequentially
      // ordered, so no need to explicitly keep track).
      int n = 0;
      for (int i = 1; i <= maxDocno - docnoOffset; i++) {
        out.writeInt(doclengths[i]);
        n++;
        context.getCounter(DocLengths.Count).increment(1);
        context.getCounter(DocLengths.SumOfDocLengths).increment(doclengths[i]);
      }
      LOG.info(n + " doc lengths written");

      out.close();
    }
  }

  private static final String InputPath = "Ivory.InputPath";
  private static final String DocLengthDataFile = "Ivory.DocLengthDataFile";

  public static final String[] RequiredParameters = {
          Constants.CollectionName,
          Constants.CollectionPath,
          Constants.IndexPath,
          Constants.InputFormat,
          Constants.Tokenizer,
          Constants.DocnoMappingClass,
          Constants.DocnoOffset };

  @Override
  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public BuildTermDocVectors(Configuration conf) {
    super(conf);
  }

  @SuppressWarnings("unchecked")
  public int runTool() throws Exception {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    String indexPath = conf.get(Constants.IndexPath);
    String collectionName = conf.get(Constants.CollectionName);
    String collectionPath = conf.get(Constants.CollectionPath);
    String inputFormat = conf.get(Constants.InputFormat);
    String tokenizer = conf.get(Constants.Tokenizer);
    String mappingClass = conf.get(Constants.DocnoMappingClass);
    int docnoOffset = conf.getInt(Constants.DocnoOffset, 0);
    int numReducers = conf.getInt(Constants.TermDocVectorSegments, 0);

    LOG.info("PowerTool: " + BuildTermDocVectors.class.getCanonicalName());
    LOG.info(String.format(" - %s: %s", Constants.IndexPath, indexPath));
    LOG.info(String.format(" - %s: %s", Constants.CollectionName, collectionName));
    LOG.info(String.format(" - %s: %s", Constants.CollectionPath, collectionPath));
    LOG.info(String.format(" - %s: %s", Constants.InputFormat, inputFormat));
    LOG.info(String.format(" - %s: %s", Constants.Tokenizer, tokenizer));
    LOG.info(String.format(" - %s: %s", Constants.DocnoMappingClass, mappingClass));
    LOG.info(String.format(" - %s: %s", Constants.DocnoOffset, docnoOffset));
    LOG.info(String.format(" - %s: %s", Constants.TermDocVectorSegments, numReducers));

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    Path mappingFile = env.getDocnoMappingData();

    if (!fs.exists(mappingFile)) {
      LOG.error("Error, docno mapping data file " + mappingFile + " doesn't exist!");
      return 0;
    }

    DistributedCache.addCacheFile(mappingFile.toUri(), conf);

    Path outputPath = new Path(env.getTermDocVectorsDirectory());
    if (fs.exists(outputPath)) {
      LOG.info("TermDocVectors already exist: Skipping!");
      return 0;
    }

    env.writeCollectionName(collectionName);
    env.writeCollectionPath(collectionPath);
    env.writeInputFormat(inputFormat);
    env.writeDocnoMappingClass(mappingClass);
    env.writeTokenizerClass(tokenizer);
    env.writeDocnoOffset(docnoOffset);

    conf.set("mapred.child.java.opts", "-Xmx2048m");
    conf.set("mapred.task.timeout", "6000000");			// needed for stragglers (e.g., very long documents in Wikipedia)

    Job job1 = new Job(conf,
        BuildTermDocVectors.class.getSimpleName() + ":" + collectionName);
    job1.setJarByClass(BuildTermDocVectors.class);

    job1.setNumReduceTasks(numReducers);

    FileInputFormat.addInputPaths(job1, collectionPath);
    FileOutputFormat.setOutputPath(job1, outputPath);
    SequenceFileOutputFormat.setOutputCompressionType(job1, SequenceFile.CompressionType.RECORD);

    job1.setInputFormatClass((Class<? extends InputFormat>) Class.forName(inputFormat));
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

    job1.setMapOutputKeyClass(IntWritable.class);
    job1.setMapOutputValueClass(LazyTermDocVector.class);
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(LazyTermDocVector.class);

    job1.setMapperClass(MyMapper.class);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // Write out number of postings.
    int collectionDocCount = (int) job1.getCounters().findCounter(Docs.Total).getValue();
    env.writeCollectionDocumentCount(collectionDocCount);

    Path dlFile = env.getDoclengthsData();
    if (fs.exists(dlFile)) {
      LOG.info("DocLength data exists: Skipping!");
      return 0;
    }

    conf.setInt(Constants.CollectionDocumentCount, collectionDocCount);
    conf.set(InputPath, env.getDoclengthsDirectory().toString());
    conf.set(DocLengthDataFile, dlFile.toString());

    conf.set("mapred.child.java.opts", "-Xmx2048m");
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    LOG.info("Writing doc length data to " + dlFile + "...");

    Job job2 = new Job(conf, "DocLengthTable:" + collectionName);
    job2.setJarByClass(BuildTermDocVectors.class);

    job2.setNumReduceTasks(0);
    job2.setInputFormatClass(NullInputFormat.class);
    job2.setOutputFormatClass(NullOutputFormat.class);
    job2.setMapperClass(DocLengthDataWriterMapper.class);

    startTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
    LOG.info("Job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    long collectionSumOfDocLengths =
        job2.getCounters().findCounter(DocLengths.SumOfDocLengths).getValue();
    env.writeCollectionAverageDocumentLength(
        (float) collectionSumOfDocLengths / collectionDocCount);

    return 0;
  }
}
