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

import ivory.core.RetrievalEnvironment;
import ivory.core.data.document.WeightedIntDocVector;
import ivory.lsh.driver.PwsimEnvironment;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters;
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

import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.MapKF;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;

/**
 * Map term doc vectors into int doc vectors using the term-to-id mapping. 
 * This task is the same in either cross-lingual or mono-lingual case. That is, this task works for the case where doc vectors are translated into English and the case where doc vectors are originally in English.
 * Also, weights in doc vector are normalized.
 * 
 * @author ferhanture
 *
 */
public class BuildTargetLangWeightedIntDocVectors extends PowerTool {
  private static final Logger sLogger = Logger.getLogger(BuildWeightedIntDocVectors.class);

  static{
    sLogger.setLevel(Level.INFO);
  }
  protected static enum Docs{
    Total
  }
  protected static enum Terms{
    OOV, NEG
  }

  private static class MyMapper extends MapReduceBase implements
      Mapper<IntWritable, HMapSFW, IntWritable, WeightedIntDocVector> {

    static IntWritable mDocno = new IntWritable();
    private boolean normalize = false;
    private Vocab engVocabH;

    public void configure(JobConf conf){
      normalize = conf.getBoolean("Ivory.Normalize", false);

      try {
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        String vocabFile = conf.get("Ivory.FinalVocab");
        vocabFile = vocabFile.substring(vocabFile.lastIndexOf("/") + 1);

        for (Path p : localFiles) {
          if (p.toString().contains(vocabFile)) {
            engVocabH = HadoopAlign.loadVocab(p, FileSystem.getLocal(conf));
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing vocab data!");
      }
    }

    WeightedIntDocVector weightedVectorOut = new WeightedIntDocVector();
    HMapIFW weightedVector = new HMapIFW();

    float sum2;
    public void map(IntWritable docno, HMapSFW doc,
        OutputCollector<IntWritable, WeightedIntDocVector> output, Reporter reporter)
    throws IOException {	
      mDocno.set(docno.get());
      weightedVector.clear();

      sLogger.debug("===================================BEGIN READ DOC");
      sum2 = 0;

      for (MapKF.Entry<String> entry : doc.entrySet()) {
        String eTerm = entry.getKey();
        int e = engVocabH.get(eTerm);
        if (e < 0) {
          sLogger.debug(eTerm+ " term in doc not found in aligner vocab");
          continue;
        }
        float score = entry.getValue(); 
        if (normalize) {
          sum2+=score*score;
        }
        weightedVector.put(e, score);
      }
      sLogger.debug("===================================END READ DOC");

      weightedVectorOut.setWeightedTerms(weightedVector);
      if (normalize) {
        /*length-normalize doc vectors*/
        sum2 = (float) Math.sqrt(sum2);
        weightedVectorOut.normalizeWith(sum2);
      }
      output.collect(mDocno, weightedVectorOut);
      reporter.incrCounter(Docs.Total, 1);
    }
  }

  public static final String[] RequiredParameters = { "Ivory.NumMapTasks",
    "Ivory.IndexPath", 
    "Ivory.Normalize",
  };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public BuildTargetLangWeightedIntDocVectors(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {
    sLogger.info("PowerTool: " + BuildTargetLangWeightedIntDocVectors.class.getName());

    JobConf conf = new JobConf(getConf(), BuildTargetLangWeightedIntDocVectors.class);
    FileSystem fs = FileSystem.get(conf);

    String indexPath = getConf().get("Ivory.IndexPath");

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

    String outputPath = env.getWeightedIntDocVectorsDirectory();
    int mapTasks = getConf().getInt("Ivory.NumMapTasks", 0);
    int minSplitSize = getConf().getInt("Ivory.MinSplitSize", 0);
    String collectionName = getConf().get("Ivory.CollectionName");

    sLogger.info("Characteristics of the collection:");
    sLogger.info(" - CollectionName: " + collectionName);
    sLogger.info("Characteristics of the job:");
    sLogger.info(" - NumMapTasks: " + mapTasks);
    sLogger.info(" - MinSplitSize: " + minSplitSize);

    String vocabFile = getConf().get("Ivory.FinalVocab");
    DistributedCache.addCacheFile(new URI(vocabFile), conf);

    Path inputPath = new Path(PwsimEnvironment.getFileNameWithPars(indexPath, "TermDocs", fs));
    Path weightedVectorsPath = new Path(outputPath);

    if (fs.exists(weightedVectorsPath)) {
      sLogger.info("Output path already exists!");
      return -1;
    }
    conf.setJobName(BuildTargetLangWeightedIntDocVectors.class.getSimpleName() + ":" + collectionName);
    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(0);
    conf.setInt("mapred.min.split.size", minSplitSize);
    conf.set("mapred.child.java.opts", "-Xmx2048m");
    conf.setBoolean("Ivory.Normalize", getConf().getBoolean("Ivory.Normalize", true));
    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, weightedVectorsPath);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(WeightedIntDocVector.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(WeightedIntDocVector.class);

    conf.setMapperClass(MyMapper.class);

    long startTime = System.currentTimeMillis();

    RunningJob rj = JobClient.runJob(conf);
    sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");
    Counters counters = rj.getCounters();

    long numOfDocs= (long) counters.findCounter(Docs.Total).getCounter();

    return (int) numOfDocs;
  }
}
