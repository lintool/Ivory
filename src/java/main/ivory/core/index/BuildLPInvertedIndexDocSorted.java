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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.HMapIV;
import edu.umd.cloud9.util.map.MapIV;

/**
 * <p>
 * Indexer for building document-sorted inverted indexes.
 * </p>
 * 
 * @author Tamer Elsayed
 * @author Jimmy Lin
 * 
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

  private static class MyMapper extends MapReduceBase implements
      Mapper<IntWritable, IntDocVector, IntWritable, PostingsListDocSortedPositional> {

    // key
    private static IntWritable sTerm = new IntWritable();

    // keep reference to OutputCollector and Reporter to use in close()
    private OutputCollector<IntWritable, PostingsListDocSortedPositional> mOutput;
    private Reporter mReporter;

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

    public void configure(JobConf job) {
      mMapMemoryThreshold = job.getFloat("Ivory.IndexingMapMemoryThreshold", 0.9f);
      mCollectionDocumentCount = job.getInt("Ivory.CollectionDocumentCount", 0);
      maxNDocsBeforeFlush = job.getInt("Ivory.MaxNDocsBeforeFlush", 50000);
    }

    public void map(IntWritable key, IntDocVector doc,
        OutputCollector<IntWritable, PostingsListDocSortedPositional> output, Reporter reporter)
        throws IOException {
      mDocno = key.get();

      mOutput = output;
      mReporter = reporter;

      // check if we should flush what we have so far
      flushPartialLists(false, nDocs);

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
      reporter.incrCounter(MapTime.Parsing, System.currentTimeMillis() - startTime);

      // update number of indexed terms
      reporter.incrCounter(IndexedTerms.Total, dl);

      nDocs++;
      flushPartialLists(false, nDocs);
      reporter.incrCounter(Docs.Total, 1);
    }

    // test flushing conditions and flush if test successful
    private boolean flushPartialLists(boolean forced, int nDocs) throws IOException {
      if (!forced) {
        float memoryUsagePercent = 1 - (runtime.freeMemory() * 1.0f / runtime.totalMemory());
        mReporter.setStatus("m" + memoryUsagePercent);
        if (memoryUsagePercent < mMapMemoryThreshold) {
          if (nDocs % maxNDocsBeforeFlush != 0)
            return false;
        }
        if (memoryUsagePercent >= mMapMemoryThreshold)
          mReporter.incrCounter(MemoryFlushes.AfterMemoryFilled, 1);
        else
          mReporter.incrCounter(MemoryFlushes.AfterNDocs, 1);
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
        mReporter.setStatus("t" + sTerm.get());
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
        mOutput.collect(sTerm, pl);
      }
      mReporter.incrCounter(MapTime.Spilling, System.currentTimeMillis() - startTime);
      sortedPartialPostings.clear();
      return true;
    }

    public void close() throws IOException {
      if (sortedPartialPostings.size() > 0) {
        // force flushing
        flushPartialLists(true, nDocs);
        mReporter.incrCounter(MemoryFlushes.AtClose, 1);
      }
    }
  }

  public static class MyReducer extends MapReduceBase
      implements
      Reducer<IntWritable, PostingsListDocSortedPositional, IntWritable, PostingsListDocSortedPositional> {
    int docCnt = 0;
    private Runtime runtime = Runtime.getRuntime();
    private static float mReduceMemoryThreshold = 0.9f;

    // keep reference to reporter
    private Reporter mReporter;

    // a list of merged partial lists
    ArrayList<PostingsList> mergedList = new ArrayList<PostingsList>();

    // a list of incoming partial lists since last merging
    ArrayList<PostingsList> incomingPLs = new ArrayList<PostingsList>();

    // final merged list
    PostingsListDocSortedPositional finalPostingsList = new PostingsListDocSortedPositional();

    public void configure(JobConf job) {
      docCnt = job.getInt("Ivory.CollectionDocumentCount", 0);
      mReduceMemoryThreshold = job.getFloat("Ivory.IndexingReduceMemoryThreshold", 0.9f);
    }

    public void reduce(IntWritable term, Iterator<PostingsListDocSortedPositional> values,
        OutputCollector<IntWritable, PostingsListDocSortedPositional> output, Reporter reporter)
        throws IOException {
      // sLogger.setLevel(Level.INFO);
      mReporter = reporter;
      mReporter.setStatus("t" + term);
      long start = System.currentTimeMillis();

      PostingsListDocSortedPositional pl = values.next();
      if (!values.hasNext()) { // it's just one partial list
        output.collect(term, pl);
        mReporter.incrCounter(Reduce.OnePL, 1);
      } else {// it has at least 2 partial lists
        mergedList.clear();
        incomingPLs.clear();

        // add the first
        incomingPLs.add(PostingsListDocSortedPositional.create(pl.serialize()));

        // add the rest (at least another one)
        do {
          incomingPLs.add(PostingsListDocSortedPositional.create(values.next().serialize()));
          mergeLists(false, incomingPLs, mergedList);
        } while (values.hasNext());

        // force merging lists at the end
        mergeLists(true, incomingPLs, mergedList);

        if (mergedList.size() == 1) {
          output.collect(term, (PostingsListDocSortedPositional) mergedList.get(0));
        } else {
          LOG.info("Merging the master list");
          finalPostingsList.clear();
          PostingsListDocSortedPositional.mergeList(finalPostingsList, mergedList, docCnt);
          output.collect(term, finalPostingsList);
        }
      }
      long duration = System.currentTimeMillis() - start;
      reporter.incrCounter(ReduceTime.Total, duration);
    }

    // test merging condition and merge if test successful + add the new
    // list to mergedList
    private boolean mergeLists(boolean forced, ArrayList<PostingsList> plList,
        ArrayList<PostingsList> mergedList) throws IOException {
      if (plList.size() == 0)
        return false;

      float memoryUsagePercent = 1 - (runtime.freeMemory() * 1.0f / runtime.totalMemory());
      mReporter.setStatus("m" + memoryUsagePercent);
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
        mReporter.incrCounter(Reduce.Merges, 1);
      } else {
        PostingsList pl = plList.remove(0);
        pl.setCollectionDocumentCount(docCnt);
        mergedList.add(pl);
      }
      mReporter.incrCounter(ReduceTime.Merging, System.currentTimeMillis() - startTime);
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
    // create a new JobConf, inheriting from the configuration of this
    // PowerTool
    JobConf conf = new JobConf(getConf(), BuildLPInvertedIndexDocSorted.class);
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

    conf.setJobName("BuildLPInvertedIndex:" + collectionName);

    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(reduceTasks);

    conf.setInt("Ivory.CollectionDocumentCount", collectionDocCount);

    conf.setInt("mapred.min.split.size", minSplitSize);
    // conf.set("mapred.child.java.opts", "-Xmx2048m");

    conf.set("mapred.child.java.opts", "-Xmx" + maxHeap + "m");
    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, postingsPath);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);

    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(PostingsListDocSortedPositional.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(PostingsListDocSortedPositional.class);

    conf.setMapperClass(MyMapper.class);
    // conf.setCombinerClass(MyReducer.class);
    conf.setReducerClass(MyReducer.class);

    System.out.println("MaxHeap: " + maxHeap);
    long startTime = System.currentTimeMillis();
    RunningJob job = JobClient.runJob(conf);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    env.writePostingsType("ivory.data.PostingsListDocSortedPositional");

    return 0;
  }

}
