package ivory.core.index;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.document.IntDocVector;
import ivory.core.data.index.PartialPostings;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsListDocSortedPositional;
import ivory.core.data.index.TermPositions;
import ivory.core.util.QuickSort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

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

import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.HMapIV;
import edu.umd.cloud9.util.map.MapIV;

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

    // key
    private static IntWritable sTerm = new IntWritable();

    // current docno
    private int mDocno;

    // total number of docs in collection
    private int mCollectionDocumentCount;

    // memory usage threshold
    private static float mMapMemoryThreshold = 0.9f;

    // reusable posting list
    private PostingsListDocSortedPositional pl = new PostingsListDocSortedPositional();

    HMapIV<PartialPostings> sortedPartialPostings = new HMapIV<PartialPostings>();

    // run time object to get the used amount of memory
    private Runtime runtime = Runtime.getRuntime();

    // number of documents read so far
    int nDocs = 0;

    // max number of docs before flushing
    int maxNDocsBeforeFlush = 50000;

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      mMapMemoryThreshold = conf.getFloat("Ivory.IndexingMapMemoryThreshold", 0.9f);
      mCollectionDocumentCount = conf.getInt("Ivory.CollectionDocumentCount", 0);
      maxNDocsBeforeFlush = conf.getInt("Ivory.MaxNDocsBeforeFlush", 50000);
    }

    @Override
    public void map(IntWritable key, IntDocVector doc, Context context)
        throws IOException, InterruptedException {
      mDocno = key.get();

      // check if we should flush what we have so far
      flushPartialLists(false, nDocs, context);

      long startTime = System.currentTimeMillis();

      IntDocVector.Reader r = doc.getReader();
      int term;
      int[] tp;

      int dl = 0;
      PartialPostings pl;
      while (r.hasMoreTerms()) {
        term = r.nextTerm();
        tp = r.getPositions();
        pl = sortedPartialPostings.get(term);
        if (pl == null) {
          pl = new PartialPostings();
          sortedPartialPostings.put(term, pl);
        }
        pl.add(mDocno, tp);
        dl += tp.length;
      }
      context.getCounter(MapTime.Parsing).increment(System.currentTimeMillis() - startTime);

      // update number of indexed terms
      context.getCounter(IndexedTerms.Total).increment(dl);

      nDocs++;
      flushPartialLists(false, nDocs, context);
      context.getCounter(Docs.Total).increment(1);
    }

    // test flushing conditions and flush if test successful
    private boolean flushPartialLists(boolean forced, int nDocs, Context context)
        throws IOException, InterruptedException {
      if (!forced) {
        float memoryUsagePercent = 1 - (runtime.freeMemory() * 1.0f / runtime.totalMemory());
        context.setStatus("m" + memoryUsagePercent);
        if (memoryUsagePercent < mMapMemoryThreshold) {
          if (nDocs % maxNDocsBeforeFlush != 0)
            return false;
        }
        if (memoryUsagePercent >= mMapMemoryThreshold) {
          context.getCounter(MemoryFlushes.AfterMemoryFilled).increment(1);
        } else {
          context.getCounter(MemoryFlushes.AfterNDocs).increment(1);
        }
      }
      if (sortedPartialPostings.size() == 0)
        return true;

      TermPositions tp = new TermPositions();
      PartialPostings sortedPL;
      // start the timer
      long startTime = System.currentTimeMillis();
      for (MapIV.Entry<PartialPostings> e : sortedPartialPostings.entrySet()) {
        // emit a partial posting list for each term
        sTerm.set(e.getKey());
        context.setStatus("t" + sTerm.get());
        sortedPL = e.getValue();
        pl.clear();
        pl.setCollectionDocumentCount(mCollectionDocumentCount);
        pl.setNumberOfPostings(sortedPL.size());

        int[] docnos = sortedPL.getDocnos();
        int[][] positions = sortedPL.getPositions();
        QuickSort.quicksortWithStack(positions, docnos, 0, sortedPL.size() - 1);
        for (int i = 0; i < sortedPL.size(); i++) {
          tp.set(positions[i], (short) positions[i].length);
          pl.add(docnos[i], tp.getTf(), tp);
        }
        context.write(sTerm, pl);
      }
      context.getCounter(MapTime.Spilling).increment(System.currentTimeMillis() - startTime);
      sortedPartialPostings.clear();
      return true;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      if (sortedPartialPostings.size() > 0) {
        // force flushing
        flushPartialLists(true, nDocs, context);
        context.getCounter(MemoryFlushes.AtClose).increment(1);
      }
    }
  }

  public static class MyReducer extends 
      Reducer<IntWritable, PostingsListDocSortedPositional, IntWritable, PostingsListDocSortedPositional> {
    int docCnt = 0;
    private Runtime runtime = Runtime.getRuntime();
    private static float mReduceMemoryThreshold = 0.9f;

    // a list of merged partial lists
    ArrayList<PostingsList> mergedList = new ArrayList<PostingsList>();

    // a list of incoming partial lists since last merging
    ArrayList<PostingsList> incomingPLs = new ArrayList<PostingsList>();

    // final merged list
    PostingsListDocSortedPositional finalPostingsList = new PostingsListDocSortedPositional();

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      docCnt = conf.getInt("Ivory.CollectionDocumentCount", 0);
      mReduceMemoryThreshold = conf.getFloat("Ivory.IndexingReduceMemoryThreshold", 0.9f);
    }

    @Override
    public void reduce(IntWritable term, Iterable<PostingsListDocSortedPositional> values, Context context)
        throws IOException, InterruptedException {
      // sLogger.setLevel(Level.INFO);
      context.setStatus("t" + term);
      long start = System.currentTimeMillis();

      Iterator<PostingsListDocSortedPositional> iter = values.iterator();
      PostingsListDocSortedPositional pl = iter.next();
      if (!iter.hasNext()) { // it's just one partial list
        context.write(term, pl);
        context.getCounter(Reduce.OnePL).increment(1);
      } else {// it has at least 2 partial lists
        mergedList.clear();
        incomingPLs.clear();

        // add the first
        incomingPLs.add(PostingsListDocSortedPositional.create(pl.serialize()));

        // add the rest (at least another one)
        do {
          incomingPLs.add(PostingsListDocSortedPositional.create(iter.next().serialize()));
          mergeLists(false, incomingPLs, mergedList, context);
        } while (iter.hasNext());

        // force merging lists at the end
        mergeLists(true, incomingPLs, mergedList, context);

        if (mergedList.size() == 1) {
          context.write(term, (PostingsListDocSortedPositional) mergedList.get(0));
        } else {
          LOG.info("Merging the master list");
          finalPostingsList.clear();
          PostingsListDocSortedPositional.mergeList(finalPostingsList, mergedList, docCnt);
          context.write(term, finalPostingsList);
        }
      }
      long duration = System.currentTimeMillis() - start;
      context.getCounter(ReduceTime.Total).increment(duration);
    }

    // test merging condition and merge if test successful + add the new
    // list to mergedList
    private boolean mergeLists(boolean forced, ArrayList<PostingsList> plList,
        ArrayList<PostingsList> mergedList, Context context) throws IOException {
      if (plList.size() == 0)
        return false;

      float memoryUsagePercent = 1 - (runtime.freeMemory() * 1.0f / runtime.totalMemory());
      context.setStatus("m" + memoryUsagePercent);
      if (!forced && (memoryUsagePercent < mReduceMemoryThreshold)) {
        return false;
      }
      // start the timer
      long startTime = System.currentTimeMillis();
      LOG.info(">> merging a list of " + plList.size() + " partial lists");
      if (plList.size() > 1) {
        PostingsListDocSortedPositional merged = new PostingsListDocSortedPositional();
        PostingsListDocSortedPositional.mergeList(merged, plList, docCnt);
        plList.clear();
        mergedList.add(PostingsListDocSortedPositional.create(merged.serialize()));
        // runtime.gc();
        context.getCounter(Reduce.Merges).increment(1);
      } else {
        PostingsList pl = plList.remove(0);
        pl.setCollectionDocumentCount(docCnt);
        mergedList.add(pl);
      }
      context.getCounter(ReduceTime.Merging).increment(System.currentTimeMillis() - startTime);
      return true;
    }
  }

  public static final String[] RequiredParameters = { "Ivory.NumReduceTasks",
      "Ivory.IndexPath", "Ivory.IndexingMapMemoryThreshold", "Ivory.IndexingReduceMemoryThreshold" };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public BuildLPInvertedIndexDocSorted(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    String indexPath = conf.get("Ivory.IndexPath");
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

    String collectionName = env.readCollectionName();

    int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
    int reduceTasks = conf.getInt("Ivory.NumReduceTasks", 0);
    int minSplitSize = conf.getInt("Ivory.MinSplitSize", 0);
    int collectionDocCount = env.readCollectionDocumentCount();
    float mapMemoryThreshold = conf.getFloat("Ivory.IndexingMapMemoryThreshold", 0.9f);
    float reduceMemoryThreshold = conf.getFloat("Ivory.IndexingReduceMemoryThreshold", 0.9f);
    int maxHeap = conf.getInt("Ivory.MaxHeap", 2048);
    int maxNDocsBeforeFlush = conf.getInt("Ivory.MaxNDocsBeforeFlush", 50000);

    LOG.info("PowerTool: BuildLPInvertedIndexDocSorted");
    LOG.info(" - IndexPath: " + indexPath);
    LOG.info(" - CollectionName: " + collectionName);
    LOG.info(" - CollectionDocumentCount: " + collectionDocCount);
    LOG.info(" - IndexingMapMemoryThreshold: " + mapMemoryThreshold);
    LOG.info(" - IndexingReduceMemoryThreshold: " + reduceMemoryThreshold);
    LOG.info(" - NumMapTasks: " + mapTasks);
    LOG.info(" - NumReduceTasks: " + reduceTasks);
    LOG.info(" - MinSplitSize: " + minSplitSize);
    LOG.info(" - MaxHeap: " + maxHeap);
    LOG.info(" - MaxNDocsBeforeFlush: " + maxNDocsBeforeFlush);

    if (!fs.exists(new Path(indexPath))) {
      fs.mkdirs(new Path(indexPath));
    }

    Path inputPath = new Path(env.getIntDocVectorsDirectory());
    Path postingsPath = new Path(env.getPostingsDirectory());

    if (fs.exists(postingsPath)) {
      LOG.info("Postings already exist: no indexing will be performed.");
      return 0;
    }


    conf.setInt("Ivory.CollectionDocumentCount", collectionDocCount);

    conf.setInt("mapred.min.split.size", minSplitSize);
    // conf.set("mapred.child.java.opts", "-Xmx2048m");

    conf.set("mapred.child.java.opts", "-Xmx" + maxHeap + "m");

    Job job = new Job(conf, BuildLPInvertedIndexDocSorted.class.getSimpleName() + ":" + collectionName);
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

    System.out.println("MaxHeap: " + maxHeap);
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    env.writePostingsType("ivory.data.PostingsListDocSortedPositional");

    return 0;
  }
}
