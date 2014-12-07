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
import ivory.core.data.index.PostingsAccumulator;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsListDocSortedPositional;
import ivory.core.data.index.TermPositions;
import ivory.core.util.QuickSort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import tl.lin.data.map.HMapIV;
import tl.lin.data.map.MapIV;
import edu.umd.cloud9.util.PowerTool;

/**
 * Indexer for building document-sorted inverted indexes.
 *
 * @author Tamer Elsayed
 * @author Jimmy Lin
 */
public class BuildLPInvertedIndexDocSorted extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildLPInvertedIndexDocSorted.class);

  protected static enum Docs { Total }
  protected static enum MapTime { Spilling, Parsing }
  protected static enum MapStats { PL1, df1 }
  protected static enum MemoryFlushes { AfterMemoryFilled, AfterNDocs, AtClose, Total }
  protected static enum ReduceTime { Total, Merging, Spilling }
  protected static enum Reduce { Merges, OnePL }
  protected static enum IndexedTerms { Total }

  private static class MyMapper extends
      Mapper<IntWritable, IntDocVector, IntWritable, PostingsListDocSortedPositional> {
    private static final IntWritable TERM = new IntWritable();
    // Runtime object to get the used amount of memory.
    private static final Runtime runtime = Runtime.getRuntime();

    private static float MAP_MEMORY_THRESHOLD = 0.9f;    // Memory usage threshold.
    private static int MAX_DOCS_BEFORE_FLUSH = 50000;    // Max number of docs before flushing.

    private int docno;                       // Current docno.
    private int collectionDocumentCount;     // Total number of docs in collection.
    private int docs = 0;                    // Number of documents read so far.

    private PostingsListDocSortedPositional postingsList = new PostingsListDocSortedPositional();
    private HMapIV<PostingsAccumulator> partialPostings = new HMapIV<PostingsAccumulator>();

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      MAP_MEMORY_THRESHOLD = conf.getFloat("Ivory.IndexingMapMemoryThreshold", 0.9f);
      MAX_DOCS_BEFORE_FLUSH = conf.getInt("Ivory.MaxNDocsBeforeFlush", 50000);
      collectionDocumentCount = conf.getInt("Ivory.CollectionDocumentCount", 0);
    }

    @Override
    public void map(IntWritable key, IntDocVector doc, Context context)
        throws IOException, InterruptedException {
      docno = key.get();

      // Check if we should flush what we have so far.
      flushPostings(false, context);

      long startTime = System.currentTimeMillis();

      IntDocVector.Reader r = doc.getReader();
      int term;
      int[] tp;

      int dl = 0;
      PostingsAccumulator pl;
      while (r.hasMoreTerms()) {
        term = r.nextTerm();
        tp = r.getPositions();
        pl = partialPostings.get(term);
        if (pl == null) {
          pl = new PostingsAccumulator();
          partialPostings.put(term, pl);
        }
        pl.add(docno, tp);
        dl += tp.length;
      }
      context.getCounter(MapTime.Parsing).increment(System.currentTimeMillis() - startTime);

      // Update number of indexed terms.
      context.getCounter(IndexedTerms.Total).increment(dl);

      docs++;
      flushPostings(false, context);
      context.getCounter(Docs.Total).increment(1);
    }

    private boolean flushPostings(boolean force, Context context)
        throws IOException, InterruptedException {
      if (!force) {
        float memoryUsagePercent = 1 - (runtime.freeMemory() * 1.0f / runtime.totalMemory());
        context.setStatus("m" + memoryUsagePercent);
        if (memoryUsagePercent < MAP_MEMORY_THRESHOLD && docs % MAX_DOCS_BEFORE_FLUSH != 0) {
            return false;
        }
        if (memoryUsagePercent >= MAP_MEMORY_THRESHOLD) {
          context.getCounter(MemoryFlushes.AfterMemoryFilled).increment(1);
        } else {
          context.getCounter(MemoryFlushes.AfterNDocs).increment(1);
        }
      }

      if (partialPostings.size() == 0) {
        return true;
      }

      TermPositions tp = new TermPositions();
      // Start the timer.
      long startTime = System.currentTimeMillis();
      for (MapIV.Entry<PostingsAccumulator> e : partialPostings.entrySet()) {
        // Emit a partial posting list for each term.
        TERM.set(e.getKey());
        context.setStatus("t" + TERM.get());
        PostingsAccumulator pl = e.getValue();
        postingsList.clear();
        postingsList.setCollectionDocumentCount(collectionDocumentCount);
        postingsList.setNumberOfPostings(pl.size());

        int[] docnos = pl.getDocnos();
        int[][] positions = pl.getPositions();
        QuickSort.quicksortWithStack(positions, docnos, 0, pl.size() - 1);
        for (int i = 0; i < pl.size(); i++) {
          tp.set(positions[i], (short) positions[i].length);
          postingsList.add(docnos[i], tp.getTf(), tp);
        }
        context.write(TERM, postingsList);
      }
      context.getCounter(MapTime.Spilling).increment(System.currentTimeMillis() - startTime);
      partialPostings.clear();
      return true;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      // Force flushing.
      if (partialPostings.size() > 0) {
        flushPostings(true, context);
        context.getCounter(MemoryFlushes.AtClose).increment(1);
      }
    }
  }

  public static class MyReducer extends 
      Reducer<IntWritable, PostingsListDocSortedPositional, IntWritable, PostingsListDocSortedPositional> {
    private static float REDUCE_MEMORY_THRESHOLD = 0.9f;
    private static final Runtime runtime = Runtime.getRuntime();

    private int collectionDocumentCount = 0;

    // A list of merged partial lists.
    private List<PostingsList> mergedList = new ArrayList<PostingsList>();

    // A list of incoming partial lists since last merging.
    private List<PostingsList> incomingLists = new ArrayList<PostingsList>();

    // Final merged list.
    private PostingsListDocSortedPositional finalPostingsList = new PostingsListDocSortedPositional();

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      REDUCE_MEMORY_THRESHOLD = conf.getFloat("Ivory.IndexingReduceMemoryThreshold", 0.9f);
      collectionDocumentCount = conf.getInt("Ivory.CollectionDocumentCount", 0);
    }

    @Override
    public void reduce(IntWritable term, Iterable<PostingsListDocSortedPositional> values, Context context)
        throws IOException, InterruptedException {
      context.setStatus("t" + term);
      long start = System.currentTimeMillis();

      Iterator<PostingsListDocSortedPositional> iter = values.iterator();
      PostingsListDocSortedPositional pl = iter.next();
      if (!iter.hasNext()) {
        // It's just one partial list.
        context.write(term, pl);
        context.getCounter(Reduce.OnePL).increment(1);
      } else {
        // Has at least 2 partial lists...
        mergedList.clear();
        incomingLists.clear();

        // Add the first.
        incomingLists.add(PostingsListDocSortedPositional.create(pl.serialize()));

        // Add the rest (at least another one).
        do {
          incomingLists.add(PostingsListDocSortedPositional.create(iter.next().serialize()));
          mergeLists(false, incomingLists, mergedList, context);
        } while (iter.hasNext());

        // Force merging lists at the end.
        mergeLists(true, incomingLists, mergedList, context);

        if (mergedList.size() == 1) {
          context.write(term, (PostingsListDocSortedPositional) mergedList.get(0));
        } else {
          LOG.info("Merging the master list");
          finalPostingsList.clear();
          PostingsListDocSortedPositional.mergeList(finalPostingsList, mergedList, collectionDocumentCount);
          context.write(term, finalPostingsList);
        }
      }
      long duration = System.currentTimeMillis() - start;
      context.getCounter(ReduceTime.Total).increment(duration);
    }

    private boolean mergeLists(boolean forced, List<PostingsList> lists,
        List<PostingsList> mergedList, Context context) throws IOException {
      if (lists.size() == 0) {
        return false;
      }

      float memoryUsagePercent = 1 - (runtime.freeMemory() * 1.0f / runtime.totalMemory());
      context.setStatus("m" + memoryUsagePercent);
      if (!forced && (memoryUsagePercent < REDUCE_MEMORY_THRESHOLD)) {
        return false;
      }

      // Start the timer.
      long startTime = System.currentTimeMillis();
      LOG.info(">> merging a list of " + lists.size() + " partial lists");
      if (lists.size() > 1) {
        PostingsListDocSortedPositional merged = new PostingsListDocSortedPositional();
        PostingsListDocSortedPositional.mergeList(merged, lists, collectionDocumentCount);
        lists.clear();
        mergedList.add(PostingsListDocSortedPositional.create(merged.serialize()));
        context.getCounter(Reduce.Merges).increment(1);
      } else {
        PostingsList pl = lists.remove(0);
        pl.setCollectionDocumentCount(collectionDocumentCount);
        mergedList.add(pl);
      }

      context.getCounter(ReduceTime.Merging).increment(System.currentTimeMillis() - startTime);
      return true;
    }
  }

  public static final String[] RequiredParameters = { Constants.NumReduceTasks, Constants.IndexPath };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public BuildLPInvertedIndexDocSorted(Configuration conf) {
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
    int collectionDocCount = env.readCollectionDocumentCount();

    String postingsType = conf.get(Constants.PostingsListsType,
        PostingsListDocSortedPositional.class.getCanonicalName());
    @SuppressWarnings("unchecked")
    Class<? extends PostingsList> postingsClass =
        (Class<? extends PostingsList>) Class.forName(postingsType);

    // These are the default values for the LP algorithm.
    float mapMemoryThreshold = conf.getFloat(Constants.IndexingMapMemoryThreshold, 0.9f);
    float reduceMemoryThreshold = conf.getFloat(Constants.IndexingReduceMemoryThreshold, 0.9f);
    int maxHeap = conf.getInt(Constants.MaxHeap, 2048);
    int maxNDocsBeforeFlush = conf.getInt(Constants.MaxNDocsBeforeFlush, 50000);

    LOG.info("PowerTool: " + BuildLPInvertedIndexDocSorted.class.getSimpleName());
    LOG.info(String.format(" - %s: %s", Constants.IndexPath, indexPath));
    LOG.info(String.format(" - %s: %s", Constants.CollectionName, collectionName));
    LOG.info(String.format(" - %s: %s", Constants.CollectionDocumentCount, collectionDocCount));
    LOG.info(String.format(" - %s: %s", Constants.PostingsListsType, postingsClass.getCanonicalName()));
    LOG.info(String.format(" - %s: %s", Constants.NumReduceTasks, reduceTasks));
    LOG.info(String.format(" - %s: %s", Constants.MinSplitSize, minSplitSize));
    LOG.info(String.format(" - %s: %s", Constants.IndexingMapMemoryThreshold, mapMemoryThreshold));
    LOG.info(String.format(" - %s: %s", Constants.IndexingReduceMemoryThreshold, reduceMemoryThreshold));
    LOG.info(String.format(" - %s: %s", Constants.MaxHeap, maxHeap));
    LOG.info(String.format(" - %s: %s", Constants.MaxNDocsBeforeFlush, maxNDocsBeforeFlush));

    if (!fs.exists(new Path(indexPath))) {
      fs.mkdirs(new Path(indexPath));
    }

    Path inputPath = new Path(env.getIntDocVectorsDirectory());
    Path postingsPath = new Path(env.getPostingsDirectory());

    if (fs.exists(postingsPath)) {
      LOG.info("Postings already exist: no indexing will be performed.");
      return 0;
    }

    conf.setInt(Constants.CollectionDocumentCount, collectionDocCount);

    conf.setInt("mapred.min.split.size", minSplitSize);
    //conf.set("mapred.child.java.opts", "-Xmx" + maxHeap + "m");
    conf.set("mapreduce.map.memory.mb", "2048");
    conf.set("mapreduce.map.java.opts", "-Xmx2048m");
    conf.set("mapreduce.reduce.memory.mb", "2048");
    conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");

    Job job = Job.getInstance(conf,
        BuildLPInvertedIndexDocSorted.class.getSimpleName() + ":" + collectionName);
    job.setJarByClass(BuildLPInvertedIndexDocSorted.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, postingsPath);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PostingsListDocSortedPositional.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PostingsListDocSortedPositional.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    env.writePostingsType("ivory.data.PostingsListDocSortedPositional");

    return 0;
  }
}
