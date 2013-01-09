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
import ivory.core.data.dictionary.DefaultFrequencySortedDictionary;
import ivory.core.data.document.LazyTermDocVector;
import ivory.core.data.document.TermDocVector;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.MapKF;

public class BuildWeightedTermDocVectors extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildWeightedTermDocVectors.class);

  protected static enum Docs { Total, ZERO, SHORT }

  private static class MyMapper extends MapReduceBase implements
      Mapper<IntWritable, LazyTermDocVector, IntWritable, HMapSFW> {

    static IntWritable mDocno = new IntWritable();
    private static DocLengthTable mDLTable;
    private static ScoringModel mScoreFn;
    int MIN_SIZE = 0;

    boolean shortDocLengths = false; 
    private boolean normalize = false;
    DefaultFrequencySortedDictionary dict;
    DfTableArray dfTable; 
    HMapSFW weightedVector = new HMapSFW();
    String term;
    float wt, sum2;

    public void configure(JobConf conf){
      normalize = conf.getBoolean("Ivory.Normalize", false);
      shortDocLengths = conf.getBoolean("Ivory.ShortDocLengths", true);
      MIN_SIZE = conf.getInt("Ivory.MinNumTerms", 0);

      Path[] localFiles;
      Map<String, Path> pathMapping = Maps.newHashMap();
      String termsFile;
      String termidsFile;
      String idToTermFile;
      String dfFile;
      String dlFile;

      try {
        if (conf.get ("mapred.job.tracker").equals ("local")) {
          // Explicitly not support local mode.
          throw new RuntimeException("Local mode not supported!");
        }

        FileSystem fs = FileSystem.get(conf);
        RetrievalEnvironment env = new RetrievalEnvironment(conf.get(Constants.IndexPath), fs);
        termsFile = env.getIndexTermsData();
        termidsFile = env.getIndexTermIdsData();
        idToTermFile = env.getIndexTermIdMappingData();
        dfFile = env.getDfByIntData();
        dlFile = env.getDoclengthsData().toString();

        termsFile = termsFile.substring(termsFile.lastIndexOf("/") + 1);
        termidsFile = termidsFile.substring(termidsFile.lastIndexOf("/") + 1);
        idToTermFile = idToTermFile.substring(idToTermFile.lastIndexOf("/") + 1);
        dfFile = dfFile.substring(dfFile.lastIndexOf("/") + 1);
        dlFile = dlFile.substring(dlFile.lastIndexOf("/") + 1);

        // We need to figure out which file in the DistributeCache is which...
        localFiles = DistributedCache.getLocalCacheFiles(conf);
        for (Path p : localFiles) {
          LOG.info("In DistributedCache: " + p);
          if (p.toString().contains(termsFile)) {
            pathMapping.put(termsFile, p);
          } else if (p.toString().contains(termidsFile)) {
            pathMapping.put(termidsFile, p);
          } else if (p.toString().contains(idToTermFile)) {
            pathMapping.put(idToTermFile, p);
          } else if (p.toString().contains(dfFile)) {
            pathMapping.put(dfFile, p);
          } else if (p.toString().contains(dlFile)) {
            pathMapping.put(dlFile, p);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException ("Local cache files not read properly.");
      }

      LOG.info(" - terms: " + pathMapping.get(termsFile));
      LOG.info(" - id: " + pathMapping.get(termidsFile));
      LOG.info(" - idToTerms: " + pathMapping.get(idToTermFile));
      LOG.info(" - df data: " + pathMapping.get(dfFile));
      LOG.info(" - dl data: " + pathMapping.get(dlFile));

      try{
        dict = new DefaultFrequencySortedDictionary(pathMapping.get(termsFile),
            pathMapping.get(termidsFile), pathMapping.get(idToTermFile), FileSystem.getLocal(conf));
        dfTable = new DfTableArray(pathMapping.get(dfFile), FileSystem.getLocal(conf));
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error loading Terms File for dictionary from "+localFiles[0]);
      }

      LOG.info("Global Stats table loaded successfully.");

      try {
        if(shortDocLengths)
          mDLTable = new DocLengthTable2B(pathMapping.get(dlFile), FileSystem.getLocal(conf));
        else 
          mDLTable = new DocLengthTable4B(pathMapping.get(dlFile), FileSystem.getLocal(conf));
      } catch (IOException e1) {
        throw new RuntimeException("Error loading dl table from "+localFiles[4]);
      }

      try {
        mScoreFn = (ScoringModel) Class.forName(conf.get("Ivory.ScoringModel")).newInstance();

        // this only needs to be set once for the entire collection
        mScoreFn.setDocCount(mDLTable.getDocCount());
        mScoreFn.setAvgDocLength(mDLTable.getAvgDocLength());
      } catch (Exception e) {
        throw new RuntimeException("Error initializing Ivory.ScoringModel from "+conf.get("Ivory.ScoringModel"));
      }
    }
    
    public void map(IntWritable docno, LazyTermDocVector doc,
        OutputCollector<IntWritable, HMapSFW> output, Reporter reporter)
    throws IOException {      
      mDocno.set(docno.get());
      int docLen = mDLTable.getDocLength(mDocno.get());
      
      weightedVector.clear();
      TermDocVector.Reader r = doc.getReader();			

      //LOG.debug("===================================BEGIN READ DOC");
      sum2 = 0;
      while(r.hasMoreTerms()){
        term = r.nextTerm();
        int id = dict.getId(term); 
        if(id != -1){
          int df = dfTable.getDf(id);
          mScoreFn.setDF(df);
          wt = mScoreFn.computeDocumentWeight(r.getTf(), docLen);
          //LOG.debug(term+","+id+"==>"+r.getTf()+","+df+","+docLen+"="+wt);
          weightedVector.put(term, wt);
          sum2 += wt * wt;
        }else{
          //LOG.debug("skipping term "+term+" (not in dictionary)");
        }
      }
      //LOG.debug("===================================END READ DOC");
      if(normalize){
        /*length-normalize doc vectors*/
        sum2 = (float) Math.sqrt(sum2);
        for(MapKF.Entry<String> e : weightedVector.entrySet()){
          float score = weightedVector.get(e.getKey());
          weightedVector.put(e.getKey(), score/sum2);
        }
      }
      //LOG.debug("docvector size="+weightedVector.size());
      if(weightedVector.size()==0){
        reporter.incrCounter(Docs.ZERO, 1);
      }else if(weightedVector.size()<MIN_SIZE){
        reporter.incrCounter(Docs.SHORT, 1);
      }
      else{
        output.collect(mDocno, weightedVector);
        reporter.incrCounter(Docs.Total, 1);
      }
    }
  }

  public static final String[] RequiredParameters = { "Ivory.NumMapTasks",
    "Ivory.IndexPath", 
    "Ivory.ScoringModel",
    "Ivory.Normalize",
  };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public BuildWeightedTermDocVectors(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {
    LOG.info("PowerTool: " + BuildWeightedTermDocVectors.class.getName());

    JobConf conf = new JobConf(getConf(), BuildWeightedTermDocVectors.class);
    FileSystem fs = FileSystem.get(conf);

    String indexPath = getConf().get("Ivory.IndexPath");
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    String outputPath = env.getWeightedTermDocVectorsDirectory();
    int mapTasks = getConf().getInt("Ivory.NumMapTasks", 0);
    int minSplitSize = getConf().getInt("Ivory.MinSplitSize", 0);
    String collectionName = getConf().get("Ivory.CollectionName");

    String termsFilePath = env.getIndexTermsData();
    String termsIdsFilePath = env.getIndexTermIdsData();
    String termIdMappingFilePath = env.getIndexTermIdMappingData();
    String dfByIntFilePath = env.getDfByIntData();

    Path inputPath = new Path(env.getTermDocVectorsDirectory());
    Path weightedVectorsPath = new Path(outputPath);

    if (fs.exists(weightedVectorsPath)) {
      LOG.info("Output path already exists!");
      return 0;
    }

    /* add terms file to cache */
    if (!fs.exists(new Path(termsFilePath)) || !fs.exists(new Path(termsIdsFilePath)) || !fs.exists(new Path(termIdMappingFilePath))) {
      throw new RuntimeException("Error, terms file " + termsFilePath + "/" + termsIdsFilePath + "/" + termIdMappingFilePath + "doesn't exist!");
    }
    DistributedCache.addCacheFile(new URI(termsFilePath), conf);
    DistributedCache.addCacheFile(new URI(termsIdsFilePath), conf);
    DistributedCache.addCacheFile(new URI(termIdMappingFilePath), conf);

    /* add df table to cache */
    if (!fs.exists(new Path(dfByIntFilePath))) {
      throw new RuntimeException("Error, df data file " + dfByIntFilePath + "doesn't exist!");
    }
    DistributedCache.addCacheFile(new URI(dfByIntFilePath), conf);

    /* add dl table to cache */
    Path docLengthFile = env.getDoclengthsData();
    if (!fs.exists(docLengthFile)) {
      throw new RuntimeException("Error, doc-length data file " + docLengthFile + "doesn't exist!");
    }
    DistributedCache.addCacheFile(docLengthFile.toUri(), conf);

    conf.setJobName(BuildWeightedTermDocVectors.class.getSimpleName() + ":" + collectionName);
    conf.setMapperClass(MyMapper.class);

    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(0);

    conf.setInt("mapred.min.split.size", minSplitSize);
    conf.set("mapred.child.java.opts", "-Xmx2048m");
    conf.setInt("Ivory.MinNumTerms", getConf().getInt("Ivory.MinNumTerms", Integer.MAX_VALUE));		
    conf.setBoolean("Ivory.Normalize", getConf().getBoolean("Ivory.Normalize", false));
    if(getConf().get("Ivory.ShortDocLengths")!=null){
      conf.set("Ivory.ShortDocLengths", getConf().get("Ivory.ShortDocLengths"));
    }
    conf.set("Ivory.ScoringModel", getConf().get("Ivory.ScoringModel"));

    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, weightedVectorsPath);
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(HMapSFW.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(HMapSFW.class);

    LOG.info("Running job: "+conf.getJobName());

    long startTime = System.currentTimeMillis();
    JobClient.runJob(conf);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    return 0;
  }
}
