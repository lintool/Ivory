package ivory.lsh.driver;

import ivory.core.RetrievalEnvironment;
import ivory.lsh.eval.BruteForcePwsim;
import ivory.lsh.eval.FilterResults;
import ivory.lsh.eval.SampleIntDocVectors;
import ivory.lsh.pwsim.GenerateChunkedPermutedTables;
import ivory.lsh.pwsim.cl.CLSlidingWindowPwsim;

import java.util.SortedMap;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


import edu.umd.cloud9.io.SequenceFileUtils;

/**
 * Runner class for cross-lingual pairwise similarity algorithms.
 * 
 * @author ferhanture
 *
 */
public class RunEvalCrossLingPwsim extends PwsimEnvironment implements Tool {
  private static final Logger sLogger = Logger.getLogger(RunEvalCrossLingPwsim.class);
  private Options options;
  private String mode, srcLangDir, targetLangDir;
  
  @SuppressWarnings("unchecked")
  public int run(String[] args) throws Exception {
    if ( parseArgs(args) < 0 ) {
      printUsage();
      return -1;
    }
    /////////Configuration//////////////////
    long startTime = System.currentTimeMillis();
    
    PwsimEnvironment.isCrossLingual = true;		

    Configuration config = new Configuration();
    FileSystem hdfs;
//    if(!PwsimEnvironment.cluster){
//      config.set("mapred.job.tracker", "local");
//      config.set("fs.default.name", "file:///");
//      hdfs = FileSystem.getLocal(config);
//    }else{
      hdfs = FileSystem.get(config);
//    }

//    targetLangDir = args[0];
//    srcLangDir = args[1];
    config.setInt("Ivory.NumMapTasks", 100);

    RetrievalEnvironment targetEnv = new RetrievalEnvironment(targetLangDir, hdfs);
    RetrievalEnvironment srcEnv = new RetrievalEnvironment(srcLangDir, hdfs);

    config.set("Ivory.CollectionName", targetEnv.readCollectionName()+"_"+srcEnv.readCollectionName());
    config.set("Ivory.IndexPath", targetLangDir);

    // collection size is the sum of the two collections' sizes
    int srcCollSize = srcEnv.readCollectionDocumentCount();
    int collSize = targetEnv.readCollectionDocumentCount()+srcCollSize;
    config.setInt("Ivory.CollectionDocumentCount", collSize);

    ///////Parameters/////////////
//    numOfBits = Integer.parseInt(args[2]);
//    signatureType = args[3].toLowerCase();
//    numOfPermutations = Integer.parseInt(args[4]);
//    chunkOverlapSize = Integer.parseInt(args[5]);
//    slidingWindowSize = Integer.parseInt(args[6]);
//    maxHammingDistance = Integer.parseInt(args[7]);

//    sampleSize = Integer.parseInt(args[8]);
//    mode = args[9];

    PwsimEnvironment.setClassTypes(config);
    config.setInt("Ivory.NumOfBits", numOfBits);
    config.setInt("Ivory.NumOfPermutations", numOfPermutations);
    config.setInt("Ivory.SlidingWindowSize", slidingWindowSize);
    config.setInt("Ivory.MaxHammingDistance", maxHammingDistance);
    int numReducers = numOfPermutations;
    config.setInt("Ivory.NumReduceTasks", numReducers);
    config.setInt("Ivory.OverlapSize", chunkOverlapSize);
    config.set("SrcLangDir", srcLangDir);

    // determine size of each chunk
    int minChunkSize = 100000;
    int maxChunkSize = 2000000;

    int chunkSize = collSize/numChunksPerPermTable;
    if(chunkSize < minChunkSize) chunkSize = minChunkSize;
    else if(chunkSize > maxChunkSize) chunkSize = maxChunkSize;
    config.setInt("Ivory.ChunckSize", chunkSize);

    /**************************************************************************************
     * A. CREATE A SAMPLE OF DOCUMENTS FROM SOURCE SIDE
     *************************************************************************************/

    String wtdIntDocVectorsPath = getFileNameWithPars(srcLangDir,"IntDocs");
    String sampleWtdIntDocVectorsPath = getFileNameWithPars(srcLangDir,"SampleIntDocs");
    Path sampleDocnosPath = new Path(getFileNameWithPars(srcLangDir,"SampleDocnos"));

    // sample from non-English weighted int doc vectors: these are needed by BruteForcePwsim, which finds the ground truth pairs for the sample using dot product of doc vectors
    if(!hdfs.exists(new Path(sampleWtdIntDocVectorsPath))){
      if (hdfs.exists(sampleDocnosPath)) {
        sLogger.info("Sample docnos exist, creating doc vectors w.r.t this docno list...");
        String[] sampleArgs = {wtdIntDocVectorsPath, sampleWtdIntDocVectorsPath, "100", "-1", sampleDocnosPath.toString()};
        SampleIntDocVectors.main(sampleArgs);
      }else {
        int frequency = (srcCollSize/sampleSize);
        sLogger.info("Sample docnos don't exist, creating a sample file with frequency " + frequency);
        String[] sampleArgs = {wtdIntDocVectorsPath, sampleWtdIntDocVectorsPath, "100", frequency+""};
        SampleIntDocVectors.main(sampleArgs);
      }
    }

    // extract sample docnos into a separate file: needed for filtering pwsim pairs
    if(!hdfs.exists(sampleDocnosPath)){
      sLogger.info("Extracting sample docnos from sampled vectors...");
      SortedMap<WritableComparable, Writable> docno2DocVectors;
      try{
        docno2DocVectors = SequenceFileUtils.readFileIntoMap(new Path(sampleWtdIntDocVectorsPath+"/part-00000"));
        FSDataOutputStream out = hdfs.create(sampleDocnosPath);
        for(Entry<WritableComparable, Writable> entry : docno2DocVectors.entrySet()){
          int docno = ((IntWritable) entry.getKey()).get();
          out.writeBytes(docno+"\n");
        }
        out.close();
      } catch (Exception e) {
        throw new RuntimeException(e.toString());
      }
    }

    /**************************************************************************************
     * B. SLIDING WINDOW ALGORITHM FOR PAIRWISE SIMILARITY
     *************************************************************************************/
    //create tables
    GenerateChunkedPermutedTables createTableTool = new GenerateChunkedPermutedTables(config);
    createTableTool.run();

    // pwsim on entire collection
    if(mode.equals("all")){
      String[] clSlidingArgs = {getFileNameWithPars(targetLangDir, "Tables"), getFileNameWithPars(targetLangDir, "PWSimCollection"), slidingWindowSize+"", maxHammingDistance+""};
      CLSlidingWindowPwsim.main(clSlidingArgs);
    }else{	// pwsim on sample documents only
      String[] clSlidingArgs = {getFileNameWithPars(targetLangDir, "Tables"), getFileNameWithPars(targetLangDir, "PWSimCollectionFiltered"), slidingWindowSize+"", maxHammingDistance+"",sampleDocnosPath.toString()};
      CLSlidingWindowPwsim.main(clSlidingArgs);
    }


    /**************************************************************************************
     * C. EVALUATION 
     **************************************************************************************/

    if(mode.equals("all")){
      // FROM PWSIM OUTPUT, FOR EACH SAMPLE DOC, EXTRACT SIMILAR PAIRS 

      config.set("SampleDocnosFile", sampleDocnosPath.toString());
      config.set("Ivory.PWSimOutputPath",	getFileNameWithPars(targetLangDir, "PWSimCollection"));
      config.setInt("Ivory.NumResults", numResults);

      String filterPWSimResultsPath = getFileNameWithPars(targetLangDir, "PWSimCollectionFiltered");
      config.set("FilteredPWSimFile", filterPWSimResultsPath);

      //filter pwsim results w.r.t sample
      String[] filterArgs = {getFileNameWithPars(targetLangDir, "PWSimCollection"), filterPWSimResultsPath, sampleDocnosPath.toString(), maxHammingDistance+"", numResults+""};
      FilterResults.main(filterArgs);
    }

    /**************************************************************************************
     * D. GROUND TRUTH GENERATION
     **************************************************************************************/

    //find all cosine pairs w.r.t sample = needs to be done once for every cosine threshold.
    float T = (float) Math.cos(Math.PI*maxHammingDistance/numOfBits);
    int x = (int) (T*100);
    T = x/100.0f;

    String groundTruthOutput = (numResults > 0) ? (targetLangDir+"/groundtruth_T="+T+"_topN="+numResults) : (targetLangDir+"/groundtruth_T="+T+"_topN=all");
    if(!hdfs.exists(new Path(groundTruthOutput))){
      String[] evalArgs = {"docvectors", getFileNameWithPars(targetLangDir,"IntDocs"), groundTruthOutput, sampleWtdIntDocVectorsPath+"/part-00000", T+"", numResults+""};
      BruteForcePwsim.main(evalArgs);
    }

    sLogger.info("Job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // Following is not part of regular evaluation. This is for determining upperbound recall values

    //		int dist = 400;
    //		String signatureOutput = targetLangDir+"/pwsim_dist="+dist;
    //		String[] evalArgs = {"signatures", getFileNameWithPars(targetLangDir,"SignaturesRandom"), signatureOutput, srcLangDir+"/sample-signatures-random_D=1000/part-00000", "400", "-1"};
    //		BruteForcePwsim.main(evalArgs);
    //
    //		
    //		dist = 436;
    //		signatureOutput = targetLangDir+"/pwsim_dist="+dist;
    //		String[] evalArgs2 = {"signatures", getFileNameWithPars(targetLangDir,"SignaturesRandom"), signatureOutput, srcLangDir+"/sample-signatures-random_D=1000/part-00000", "436", "-1"};
    //		BruteForcePwsim.main(evalArgs2);

    return 0;
  }
  
  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( this.getClass().getCanonicalName(), options );
    //    System.out.println("usage: [targetlang-dir] [srclang-dir] [num-bits] [type-of-signature] [num-perms] [overlap-size] [window-size] [max-dist] [sample-size] [mode]\nIf you want to run full pwsim on all document pairs, mode=all, otherwise mode=sample\n");
    //    return -1;
  }

  private static final String INDEX_PATH_OPTION = "sourceindex";
  private static final String TARGET_INDEX_PATH_OPTION = "targetindex";
  private static final String SIGNLENG_OPTION = "length";
  private static final String SIGNTYPE_OPTION = "type";
  private static final String NUMPERMS_OPTION = "Q";
  private static final String OVERLAPSIZE_OPTION = "overlap";
  private static final String WINDOWSIZE_OPTION = "B";
  private static final String THRESHOLD_OPTION = "T";
  private static final String SAMPLESIZE_OPTION = "sample";
  private static final String MODE_OPTION = "mode";

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("Find all pairs or only pairs matching sample").withArgName("all|sample").hasArg().isRequired().create(MODE_OPTION));
    options.addOption(OptionBuilder.withDescription("path to source-language index directory").withArgName("path").hasArg().isRequired().create(INDEX_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to target-language index directory").withArgName("path").hasArg().isRequired().create(TARGET_INDEX_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("length of signature").withArgName("number of bits").hasArg().isRequired().create(SIGNLENG_OPTION));
    options.addOption(OptionBuilder.withDescription("type of signature").withArgName("random|minhash|simhash").hasArg().isRequired().create(SIGNTYPE_OPTION));
    options.addOption(OptionBuilder.withDescription("number of permutations (tables)").withArgName("permutations").hasArg().isRequired().create(NUMPERMS_OPTION));
    options.addOption(OptionBuilder.withDescription("sliding window size").withArgName("window").hasArg().create(WINDOWSIZE_OPTION));
    options.addOption(OptionBuilder.withDescription("hamming distance threshold for similar pairs").withArgName("threshold").hasArg().isRequired().create(THRESHOLD_OPTION));
    options.addOption(OptionBuilder.withDescription("number of sampled pairs from source collection").withArgName("sample size").hasArg().create(SAMPLESIZE_OPTION));
    // not required (default=window size)
    options.addOption(OptionBuilder.withDescription("size of overlap between chunks (default: window size)").withArgName("overlap size").hasArg().create(OVERLAPSIZE_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    mode = cmdline.getOptionValue(MODE_OPTION);
    srcLangDir = cmdline.getOptionValue(INDEX_PATH_OPTION);
    targetLangDir = cmdline.getOptionValue(TARGET_INDEX_PATH_OPTION);
    numOfBits = Integer.parseInt(cmdline.getOptionValue(SIGNLENG_OPTION));
    signatureType = cmdline.getOptionValue(SIGNTYPE_OPTION);
    numOfPermutations = Integer.parseInt(cmdline.getOptionValue(NUMPERMS_OPTION));
    slidingWindowSize = Integer.parseInt(cmdline.getOptionValue(WINDOWSIZE_OPTION));
    maxHammingDistance = Integer.parseInt(cmdline.getOptionValue(THRESHOLD_OPTION));
    sampleSize = Integer.parseInt(cmdline.getOptionValue(SAMPLESIZE_OPTION));
    
    if (cmdline.hasOption(OVERLAPSIZE_OPTION)){
      chunkOverlapSize = Integer.parseInt(cmdline.getOptionValue(OVERLAPSIZE_OPTION));      
    }else {
      chunkOverlapSize = slidingWindowSize;
    }
    if(chunkOverlapSize<slidingWindowSize){
      System.out.println("Error: [overlap-size] cannot be less than [window-size] for algorithm's correctness!");
      return -1;
    }

    return 1;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new RunEvalCrossLingPwsim(), args);
  }

}
