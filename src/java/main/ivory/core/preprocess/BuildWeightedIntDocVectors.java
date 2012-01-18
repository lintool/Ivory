/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
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
import ivory.core.data.document.WeightedIntDocVector;
import ivory.core.data.stat.DfTableArray;
import ivory.core.data.stat.DocLengthTable;
import ivory.core.data.stat.DocLengthTable2B;
import ivory.core.data.stat.DocLengthTable4B;
import ivory.pwsim.score.ScoringModel;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.MapIF;

public class BuildWeightedIntDocVectors extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildWeightedIntDocVectors.class);
  static {
    LOG.setLevel(Level.INFO);
  }

  protected static enum Docs {
    Total
  }

  private static class MyMapper extends MapReduceBase implements
      Mapper<IntWritable, IntDocVector, IntWritable, WeightedIntDocVector> {

    static IntWritable mDocno = new IntWritable();
    private static DocLengthTable mDLTable;
    private static ScoringModel mScoreFn;

    private static DfTableArray mDFTable;
    private boolean normalize = false;
    private boolean shortDocLengths = false;

    public void configure(JobConf conf) {
      normalize = conf.getBoolean("Ivory.Normalize", true);
      shortDocLengths = conf.getBoolean("Ivory.ShortDocLengths", false);

      Path[] localFiles;
      String dfByIntFile, cfByIntFile, docLengthsFile;
      try {
        String indexPath = conf.get(Constants.IndexPath);
        FileSystem fs = FileSystem.get(conf);
        RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
        dfByIntFile = env.getDfByIntData();
        cfByIntFile = env.getCfByIntData();
        docLengthsFile = env.getDoclengthsData().getName();

        // Detect if we're in standalone mode; if so, we can't us the
        // DistributedCache because it does not (currently) work in
        // standalone mode...
        if (conf.get("mapred.job.tracker").equals("local")) {
          // use values from above
        } else {
          localFiles = DistributedCache.getLocalCacheFiles(conf);
          for (Path p : localFiles) {
            LOG.info("In DistributedCache: " + p);
            if (p.toString().contains(dfByIntFile)) {
              dfByIntFile = p.toString();
            } else if (p.toString().contains(cfByIntFile)) {
              cfByIntFile = p.toString();
            } else if (p.toString().contains(docLengthsFile)) {
              docLengthsFile = p.toString();
            }
          }
          LOG.info("Df-by-int file: " + dfByIntFile);
          LOG.info("Cf-by-int file: " + cfByIntFile);
          LOG.info("Doc lengths file: " + docLengthsFile);
        }

      } catch (IOException e2) {
        throw new RuntimeException("Local cache files not read properly.");
      }

      try {
        mDFTable = new DfTableArray(new Path(dfByIntFile), FileSystem.getLocal(conf));
      } catch (IOException e1) {
        throw new RuntimeException("Error loading df table from " + dfByIntFile);
      }

      try {
        if (shortDocLengths)
          mDLTable = new DocLengthTable2B(new Path(docLengthsFile), FileSystem.getLocal(conf));
        else
          mDLTable = new DocLengthTable4B(new Path(docLengthsFile), FileSystem.getLocal(conf));
      } catch (IOException e1) {
        throw new RuntimeException("Error loading dl table from " + docLengthsFile);
      }
      try {
        mScoreFn = (ScoringModel) Class.forName(conf.get("Ivory.ScoringModel")).newInstance();

        // this only needs to be set once for the entire collection
        mScoreFn.setDocCount(mDLTable.getDocCount());
        mScoreFn.setAvgDocLength(mDLTable.getAvgDocLength());
      } catch (Exception e) {
        throw new RuntimeException("Error initializing Ivory.ScoringModel from "
            + conf.get("Ivory.ScoringModel"));
      }
    }

    HMapIFW vectorWeights = new HMapIFW();

    int term;
    float wt, sum2;

    public void map(IntWritable docno, IntDocVector doc,
        OutputCollector<IntWritable, WeightedIntDocVector> output, Reporter reporter)
        throws IOException {
      mDocno.set(docno.get());
      int docLen = mDLTable.getDocLength(mDocno.get());

      vectorWeights.clear();
      IntDocVector.Reader r = doc.getReader();
      LOG.debug("===================================BEGIN READ DOC");
      sum2 = 0;
      while (r.hasMoreTerms()) {
        term = r.nextTerm();
        mScoreFn.setDF(mDFTable.getDf(term));
        wt = mScoreFn.computeDocumentWeight(r.getTf(), docLen);
        vectorWeights.put(term, wt);
        sum2 += wt * wt;
      }
      LOG.debug("===================================END READ DOC");
      if (normalize) {
        /* length-normalize doc vectors */
        sum2 = (float) Math.sqrt(sum2);
        for (MapIF.Entry e : vectorWeights.entrySet()) {
          float score = vectorWeights.get(e.getKey());
          vectorWeights.put(e.getKey(), score / sum2);
        }
      }
      WeightedIntDocVector weightedVector = new WeightedIntDocVector(docLen, vectorWeights);
      output.collect(mDocno, weightedVector);
      reporter.incrCounter(Docs.Total, 1);
    }
  }

  public static final String[] RequiredParameters = { "Ivory.NumMapTasks", "Ivory.IndexPath",
  // "Ivory.OutputPath",
      "Ivory.ScoringModel", "Ivory.Normalize", };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public BuildWeightedIntDocVectors(Configuration conf) {
    super(conf);
  }

  @SuppressWarnings("deprecation")
  public int runTool() throws Exception {
    LOG.info("PowerTool: GetWeightedIntDocVectors");

    // create a new JobConf, inheriting from the configuration of this
    // PowerTool
    JobConf conf = new JobConf(getConf(), BuildWeightedIntDocVectors.class);
    FileSystem fs = FileSystem.get(conf);

    String indexPath = conf.get("Ivory.IndexPath");
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    String outputPath = env.getWeightedIntDocVectorsDirectory();
    int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
    int minSplitSize = conf.getInt("Ivory.MinSplitSize", 0);
    String collectionName = conf.get("Ivory.CollectionName");

    LOG.info("Characteristics of the collection:");
    LOG.info(" - CollectionName: " + collectionName);
    LOG.info("Characteristics of the job:");
    LOG.info(" - NumMapTasks: " + mapTasks);
    LOG.info(" - MinSplitSize: " + minSplitSize);

    String dfByIntFilePath = env.getDfByIntData();
    String cfByIntFilePath = env.getCfByIntData();

    /* add df table to cache */
    if (!fs.exists(new Path(dfByIntFilePath))) {
      throw new RuntimeException("Error, df data file " + dfByIntFilePath + "doesn't exist!");
    }
    DistributedCache.addCacheFile(new URI(dfByIntFilePath), conf);

    /* add cf table to cache */
    if (!fs.exists(new Path(cfByIntFilePath))) {
      throw new RuntimeException("Error, cf data file " + cfByIntFilePath + "doesn't exist!");
    }
    DistributedCache.addCacheFile(new URI(cfByIntFilePath), conf);

    /* add dl table to cache */
    Path docLengthFile = env.getDoclengthsData();
    if (!fs.exists(docLengthFile)) {
      throw new RuntimeException("Error, doc-length data file " + docLengthFile + "doesn't exist!");
    }
    DistributedCache.addCacheFile(docLengthFile.toUri(), conf);

    Path inputPath = new Path(env.getIntDocVectorsDirectory());
    Path weightedVectorsPath = new Path(outputPath);

    if (fs.exists(weightedVectorsPath)) {
      LOG.info("Output path already exists!");
      return 0;
    }

    // fs.delete(weightedVectirsPath, true);

    conf.setJobName("GetWeightedIntDocVectors:" + collectionName);
    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(0);
    conf.setInt("mapred.min.split.size", minSplitSize);
    conf.set("mapred.child.java.opts", "-Xmx2048m");

    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, weightedVectorsPath);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(WeightedIntDocVector.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(WeightedIntDocVector.class);

    conf.setMapperClass(MyMapper.class);
    // conf.setInt("mapred.task.timeout",3600000);

    long startTime = System.currentTimeMillis();

    RunningJob job = JobClient.runJob(conf);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }
}
