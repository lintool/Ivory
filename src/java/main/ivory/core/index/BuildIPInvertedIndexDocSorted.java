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
import ivory.core.data.document.IntDocVector.Reader;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsListDocSortedPositional;
import ivory.core.data.index.TermPositions;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.MapII;

/**
 * Indexer for building document-sorted inverted indexes.
 *
 * @author Jimmy Lin
 * @author Tamer Elsayed
 */
public class BuildIPInvertedIndexDocSorted extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildIPInvertedIndexDocSorted.class);

  protected static enum Docs { Total }
  protected static enum IndexedTerms { Unique, Total }
  protected static enum MapTime { Total }
  protected static enum ReduceTime { Total }

  private static class MyMapper 
      extends Mapper<IntWritable, IntDocVector, PairOfInts, TermPositions> {
    private static final TermPositions termPositions = new TermPositions();
    private static final PairOfInts pair = new PairOfInts();
    private static final HMapII dfs = new HMapII(); // Holds dfs of terms processed by this mapper.

    private int docno;

    @Override
    public void setup(Context context) {
      dfs.clear();
    }

    @Override
    public void map(IntWritable key, IntDocVector doc, Context context)
        throws IOException, InterruptedException {
      docno = key.get();

      long startTime = System.currentTimeMillis();
      Reader r = doc.getReader();

      int dl = 0;
      while (r.hasMoreTerms()) {
        int term = r.nextTerm();
        r.getPositions(termPositions);

        // Set up the key and value, and emit.
        pair.set(term, docno);
        context.write(pair, termPositions);

        // Document length of the current doc.
        dl += termPositions.getTf();

        // Df of the term in the partition handled by this mapper.
        dfs.increment(term);
      }

      context.getCounter(IndexedTerms.Total).increment(dl); // Update number of indexed terms.
      context.getCounter(Docs.Total).increment(1);          // Update number of docs.
      context.getCounter(MapTime.Total).increment(System.currentTimeMillis() - startTime);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      int[] arr = new int[1];

      // Emit dfs for terms encountered in this partition of the collection.
      for (MapII.Entry e : dfs.entrySet()) {
        arr[0] = e.getValue();
        termPositions.set(arr, (short) 1); // Dummy value.
        // Special docno of "-1" to make sure this key-value pair comes before all other postings in
        // the reducer phase.
        pair.set(e.getKey(), -1);
        context.write(pair, termPositions);
      }
    }
  }

  private static class MyReducer
      extends Reducer<PairOfInts, TermPositions, IntWritable, PostingsList> {
    private static final IntWritable term = new IntWritable();
    private static PostingsList postings;

    private int prevTerm = -1;
    private int numPostings = 0;

    @Override
    public void setup(Context context) {
      LOG.setLevel(Level.WARN);
      int cnt = context.getConfiguration().getInt(Constants.CollectionDocumentCount, 0);
      if (cnt == 0) {
        throw new RuntimeException("Error: size of collection cannot be zero!");
      }

      String postingsType = context.getConfiguration().get(Constants.PostingsListsType,
          ivory.core.data.index.PostingsListDocSortedPositional.class.getCanonicalName());
      try {
        postings = (PostingsList) Class.forName(postingsType).newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      postings.setCollectionDocumentCount(cnt);
    }

    @Override
    public void reduce(PairOfInts pair, Iterable<TermPositions> values, Context context)
        throws IOException, InterruptedException {
      long start = System.currentTimeMillis();
      int curTerm = pair.getLeftElement();

      if (pair.getRightElement() == -1) {
        if (prevTerm != -1 && curTerm != prevTerm) {
          // Encountered next term, so emit postings corresponding to previous term.
          if (numPostings != postings.size()) {
            throw new RuntimeException(String.format(
                "Error: actual number of postings processed is different from expected! " +
                    "expected: %d, got: %d for term %d", numPostings, postings.size(), prevTerm));
          }

          term.set(prevTerm);
          context.write(term, postings);
          context.getCounter(IndexedTerms.Unique).increment(1);

          LOG.info(String.format("Finished processing postings for term %d (num postings=%d)",
              prevTerm, postings.size()));
          postings.clear();
        }

        numPostings = 0;
        Iterator<TermPositions> iter = values.iterator();
        while (iter.hasNext()) {
          TermPositions positions = iter.next();
          numPostings += positions.getPositions()[0];
        }

        postings.setNumberOfPostings(numPostings);
        return;
      }

      Iterator<TermPositions> iter = values.iterator();
      TermPositions positions = iter.next();
      postings.add(pair.getRightElement(), positions.getTf(), positions);

      if (iter.hasNext()) {
        throw new RuntimeException(
            String.format("Error: values with the same (term, docno): docno=%d, term=%d",
                pair.getRightElement(), curTerm));
      }

      prevTerm = curTerm;

      long duration = System.currentTimeMillis() - start;
      context.getCounter(ReduceTime.Total).increment(duration);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      long start = System.currentTimeMillis();

      // We need to flush out the final postings list.
      if (numPostings != postings.size()) {
        throw new RuntimeException(String.format(
            "Error: actual number of postings processed is different from expected! " +
                "expected: %d, got: %d for term %d", numPostings, postings.size(), prevTerm));
      }

      term.set(prevTerm);
      context.write(term, postings);
      context.getCounter(IndexedTerms.Unique).increment(1);

      LOG.info(String.format("Finished processing postings for term %d (num postings=%d)",
          prevTerm, postings.size()));
      context.getCounter(ReduceTime.Total).increment(System.currentTimeMillis() - start);
    }
  }

  private static class MyPartitioner extends Partitioner<PairOfInts, TermPositions> {
    // Keys with the same terms should go to the same reducer.
    @Override
    public int getPartition(PairOfInts key, TermPositions value, int numReduceTasks) {
      return (key.getLeftElement() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  public static final String[] RequiredParameters = { Constants.NumReduceTasks, Constants.IndexPath };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public BuildIPInvertedIndexDocSorted(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    String indexPath = conf.get(Constants.IndexPath);
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

    String collectionName = env.readCollectionName();

    int reduceTasks = conf.getInt(Constants.NumReduceTasks, 0);
    int minSplitSize = conf.getInt(Constants.MinSplitSize, 0);
    int collectionDocCnt = env.readCollectionDocumentCount();
    //int maxHeap = conf.getInt(Constants.MaxHeap, 2048);

    String postingsType = conf.get(Constants.PostingsListsType,
        PostingsListDocSortedPositional.class.getCanonicalName());
    @SuppressWarnings("unchecked")
    Class<? extends PostingsList> postingsClass =
        (Class<? extends PostingsList>) Class.forName(postingsType);

    LOG.info("PowerTool: " + BuildIPInvertedIndexDocSorted.class.getSimpleName());
    LOG.info(String.format(" - %s: %s", Constants.IndexPath, indexPath));
    LOG.info(String.format(" - %s: %s", Constants.CollectionName, collectionName));
    LOG.info(String.format(" - %s: %s", Constants.CollectionDocumentCount, collectionDocCnt));
    LOG.info(String.format(" - %s: %s", Constants.PostingsListsType, postingsClass.getCanonicalName()));
    LOG.info(String.format(" - %s: %s", Constants.NumReduceTasks, reduceTasks));
    LOG.info(String.format(" - %s: %s", Constants.MinSplitSize, minSplitSize));

    if (!fs.exists(new Path(indexPath))) {
      fs.mkdirs(new Path(indexPath));
    }

    Path inputPath = new Path(env.getIntDocVectorsDirectory());
    Path postingsPath = new Path(env.getPostingsDirectory());

    if (fs.exists(postingsPath)) {
      LOG.info("Postings already exist: no indexing will be performed.");
      return 0;
    }

    conf.setInt(Constants.CollectionDocumentCount, collectionDocCnt);

    conf.setInt("mapred.min.split.size", minSplitSize);
    //conf.set("mapred.child.java.opts", "-Xmx" + maxHeap + "m");
    conf.set("mapreduce.map.memory.mb", "3072");
    conf.set("mapreduce.map.java.opts", "-Xmx3072m");
    conf.set("mapreduce.reduce.memory.mb", "3072");
    conf.set("mapreduce.reduce.java.opts", "-Xmx3072m");

    Job job = Job.getInstance(conf,
        BuildIPInvertedIndexDocSorted.class.getSimpleName() + ":" + collectionName);
    job.setJarByClass(BuildIPInvertedIndexDocSorted.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, postingsPath);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(TermPositions.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(postingsClass);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    env.writePostingsType(postingsClass.getCanonicalName());

    return 0;
  }
}
