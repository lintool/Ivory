package ivory.lsh.driver;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import ivory.core.RetrievalEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;


/**
 * Runner class for pairwise similarity algorithms.
 * 
 * @author ferhanture
 *
 */
public abstract class PwsimEnvironment extends Configured {
  public static final boolean cluster = true;			//set this parameter true if working on cluster
  public static boolean isCrossLingual;

  //Signatures
  public static String permutationType; // how to permute signatures: by bits ("bit") or by blocks ("block")
  public static String signatureType; // the type of signature: "simhash", "minhash", or "random"
  public static int numOfPermutations; // number of permutation tables (i.e., Q : number of permutations to use in randomized pwsim algorithm)
  public static int numOfBits;	// number of bits on the signature

  //Similarity
  public static int maxHammingDistance; // maximum allowable distance for similarity

  //Eval
  public static int sampleSize = -1;
  public static int numResults = -1;
  public static String mode;

  //Sliding Window
  public static int slidingWindowSize; // window size of similarity comparisons (i.e., B: beam size parameter for randomized pwsim algorithm)
  public static int chunkOverlapSize;	// size of overlap between chunks for sliding window algorithm 
  public static int numChunksPerPermTable = 10; // chunks per permutation table
  public static boolean pairwiseWithinChunk = false; // do pairwise comparisons within each chunk

  //Batch
  public static int batchIndexKeyLength;	// length of indexing key in bits for batch and hybrid algorithms
  public static int numBatchFiles; // number of batch files (i.e., MapReduce jobs) for batch algorithm

  //Partitioned Duplicate Detection
  public static boolean withBoundaries = false; // boundaries will be considered or not

  public static int dfCut = 100000;
  public static float scoreThreshold = 0.5f;

  public static void setClassTypes(String signatureType, Configuration config){
    if(signatureType.toLowerCase().equals("random")){
      config.set("Ivory.SignatureClass", "ivory.lsh.data.NBitSignature");		
      config.set("Ivory.PairClass", "ivory.lsh.data.PairOfIntNBitSignature");
      config.set("Type", "Random");
    }else if(signatureType.toLowerCase().equals("simhash")){
      config.set("Ivory.SignatureClass", "ivory.lsh.data.SixtyFourBitSignature");		
      config.set("Ivory.PairClass", "ivory.lsh.data.PairOfInt64BitSignature");
      config.set("Type", "Simhash");
    }else if(signatureType.toLowerCase().equals("minhash")){
      config.set("Ivory.SignatureClass", "ivory.lsh.data.MinhashSignature");		
      config.set("Ivory.PairClass", "ivory.lsh.data.PairOfIntMinhashSignature");
      config.set("Type", "Minhash");
    }else{
      throw new RuntimeException("Error: Unknown signature type.");
    }
  }

  public static String getTermDocvectorsFile (String dir, FileSystem fs) throws IOException {
    RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
    return env.getWeightedTermDocVectorsDirectory();
  }

  public static String getIntDocvectorsFile (String dir, FileSystem fs) throws IOException {
    RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
    return env.getWeightedIntDocVectorsDirectory();
  }

  public static String getIntDocvectorsFile (String dir, FileSystem fs, int sampleSize) throws IOException {
    RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
    String s = env.getWeightedIntDocVectorsDirectory();
    return s.substring(0, s.length()-1)+"_sample="+sampleSize;
  }
  
  public static String getTermDocvectorsFile (String dir, FileSystem fs, int sampleSize) throws IOException {
    RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
    String s = env.getWeightedTermDocVectorsDirectory();
    return s.substring(0, s.length()-1)+"_sample="+sampleSize;
  }

  public static String getPermutationsFile (String dir, FileSystem fs, int numOfBits, int numOfPermutations) throws IOException {
    return dir + "/random-perms-bit_D=" + numOfBits + "_Q=" + numOfPermutations;
  }

  public static String getTablesDir (String dir, FileSystem fs, String signatureType, int numOfBits, int chunkOverlapSize, int numOfPermutations) throws IOException {
    return dir + "/tables_" + signatureType + "_D=" + numOfBits + "_V=" + chunkOverlapSize + "_Q=" + numOfPermutations; 
  }

  public static String getPwsimDir (String dir, String signatureType, int maxHammingDistance, int numOfBits, int numOfPermutations, int slidingWindowSize) throws IOException {
    return dir + "/similardocs_" + signatureType + "_maxdst=" + maxHammingDistance + "_D=" + numOfBits + "_Q=" + numOfPermutations + "_B=" + slidingWindowSize;
  }

  public static String getFilteredPwsimDir (String dir, String signatureType, int maxHammingDistance, int numOfBits, int numOfPermutations, int slidingWindowSize, String docnos, int numResults) throws IOException {
    return dir + "/similardocs_" + signatureType + "_maxdst=" + maxHammingDistance + "_D=" + numOfBits + "_Q=" + numOfPermutations + "_B=" + slidingWindowSize + "-filtered_sample=" + docnos.substring(docnos.lastIndexOf("/") + 1) + "_top" + numResults;
  }

  public static String getRandomVectorsDir (String dir, int numOfBits) {
    return dir + "/randomvectors_D="+numOfBits;
  }
  
  public static String getSignaturesDir (String dir, int numOfBits, String type) {
    return dir + "/signatures-" + type + "_D=" + numOfBits;
  }

  public static String getSignaturesDir (String dir, int numOfBits, String type, int numBatch) {
    return dir + "/signatures-" + type + "_D=" + numOfBits + "_batch=" + numBatch;
  }

  public static JobConf setBitextPaths(JobConf conf, String dataDir, String eLang, String fLang, String bitextName, String eDir, String fDir, String classifierType) throws IOException, URISyntaxException {
    String eSentDetect = dataDir+"/sent/"+eLang+"-sent.bin";
    String eTokenizer = dataDir+"/token/"+eLang+"-token.bin";
    String eVocabSrc = dataDir+"/"+bitextName+"/vocab."+eLang+"-"+fLang+"."+eLang;
    String eStopwords = dataDir+"/token/"+eLang+".stop";
    String eVocabTrg = dataDir+"/"+bitextName+"/vocab."+fLang+"-"+eLang+"."+eLang;
    String eStemmedStopwords = dataDir+"/token/"+eLang+".stop.stemmed";

    String fSentDetect = dataDir+"/sent/"+fLang+"-sent.bin";
    String fTokenizer = dataDir+"/token/"+fLang+"-token.bin";
    String fVocabSrc = dataDir+"/"+bitextName+"/vocab."+fLang+"-"+eLang+"."+fLang;
    String fStopwords = dataDir+"/token/"+fLang+".stop";
    String fVocabTrg = dataDir+"/"+bitextName+"/vocab."+eLang+"-"+fLang+"."+fLang;
    String fStemmedStopwords = dataDir+"/token/"+fLang+".stop.stemmed";

    String f2e_ttableFile = dataDir+"/"+bitextName+"/ttable."+fLang+"-"+eLang;
    String e2f_ttableFile = dataDir+"/"+bitextName+"/ttable."+eLang+"-"+fLang;

    String classifierFile = dataDir+"/"+bitextName+"/classifier-" + classifierType + "." + fLang + "-" + eLang;

    conf.set("eDir", eDir);
    conf.set("fDir", fDir);
    conf.set("eLang", eLang);
    conf.set("fLang", fLang);
    conf.set("fTokenizer", fTokenizer);
    conf.set("eTokenizer", eTokenizer);
    conf.set("eStopword", eStopwords);
    conf.set("fStopword", fStopwords);
    conf.set("eStemmedStopword", eStemmedStopwords);
    conf.set("fStemmedStopword", fStemmedStopwords);
    
    //e-files

    RetrievalEnvironment eEnv = new RetrievalEnvironment(eDir, FileSystem.get(conf));
    DistributedCache.addCacheFile(new URI(eEnv.getDfByTermData()), conf);
    DistributedCache.addCacheFile(new URI(eSentDetect), conf);
    DistributedCache.addCacheFile(new URI(eTokenizer), conf);
    DistributedCache.addCacheFile(new URI(eVocabSrc), conf);
    DistributedCache.addCacheFile(new URI(eVocabTrg), conf);

    //f-files

    //    DistributedCache.addCacheFile(new URI(fDir+"/transDf.dat"), conf);
    DistributedCache.addCacheFile(new URI(fSentDetect), conf);
    DistributedCache.addCacheFile(new URI(fTokenizer), conf);
    DistributedCache.addCacheFile(new URI(fVocabSrc), conf);
    DistributedCache.addCacheFile(new URI(fVocabTrg), conf);

    /////cross-lang files

    DistributedCache.addCacheFile(new URI(f2e_ttableFile), conf);
    DistributedCache.addCacheFile(new URI(e2f_ttableFile), conf);
    DistributedCache.addCacheFile(new URI(eEnv.getIndexTermsData()), conf);
    DistributedCache.addCacheFile(new URI(classifierFile), conf);
    
    return conf;    
  }
  
  public static JobConf setBitextPaths(JobConf conf, String dataDir, String eLang, String fLang, String bitextName, String eDir, String fDir, float classifierThreshold, int classifierId, String pwsimPairsPath, String classifierType) throws IOException, URISyntaxException {
    conf = setBitextPaths(conf, dataDir, eLang, fLang, bitextName, eDir, fDir, classifierType);
    conf.setFloat("ClassifierThreshold", classifierThreshold);
    conf.setInt("ClassifierId", classifierId);
    if (pwsimPairsPath != null) {
      conf.set("PwsimPairs", pwsimPairsPath);
      DistributedCache.addCacheFile(new URI(pwsimPairsPath), conf);
    }
    return conf;    
  }
  
  //  public static String getFileNameWithPars(String dir, String fileName) throws Exception{
  //    return getFileNameWithPars(dir, fileName, FileSystem.get(new Configuration()));
  //  }

  //  public static String getFileNameWithPars(String dir, String fileName, FileSystem fs) throws Exception{
  //    RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
  //    }else if(fileName.equals("SampleDocnos")){
  //      return dir + "/sample-docnos_"+sampleSize;
  //    }else if(fileName.equals("SignaturesSimhash")){
  //      if(numBatchFiles>0){
  //        return dir + "/signatures-simhash_D="+numOfBits+"_batch="+numBatchFiles;
  //      }else{
  //        return dir + "/signatures-simhash_D="+numOfBits;
  //      }
  //    }else if(fileName.equals("SignaturesMinhash")){
  //      if(numBatchFiles>0){
  //        return dir + "/signatures-minhash_D="+numOfBits+"_batch="+numBatchFiles;
  //      }else{
  //        return dir + "/signatures-minhash_D="+numOfBits;
  //      }
  //    }else if(fileName.equals("SignaturesRandom")){
  //      if(numBatchFiles>0){
  //        return dir + "/signatures-random_D="+numOfBits+"_batch="+numBatchFiles;
  //      }else{
  //        return dir + "/signatures-random_D="+numOfBits;
  //      }
  //    }else if(fileName.equals("Signatures")){
  //      if(numBatchFiles>0){
  //        return dir + "/signatures-"+ signatureType +"_D="+numOfBits+"_batch="+numBatchFiles;
  //      }else{
  //        return dir + "/signatures-"+ signatureType +"_D="+numOfBits;
  //      }
  //    }else if(fileName.equals("SignaturesIndexable")){
  //      return dir + "/signatures-random-indx_D="+numOfBits;
  //    }else if(fileName.equals("P-SignaturesSimhash")){
  //      if(numBatchFiles>0){
  //        return dir + (withBoundaries? "/p-b":"/p")+"-signatures-simhash_D="+numOfBits+"_batch="+numBatchFiles;
  //      }else{
  //        return dir + (withBoundaries? "/p-b":"/p")+"-signatures-simhash_D="+numOfBits;
  //      }
  //    }else if(fileName.equals("P-SignaturesMinhash")){
  //      if(numBatchFiles>0){
  //        return dir + (withBoundaries? "/p-b":"/p")+"-signatures-minhash_D="+numOfBits+"_batch="+numBatchFiles;
  //      }else{
  //        return dir + (withBoundaries? "/p-b":"/p")+"-signatures-minhash_D="+numOfBits;
  //      }
  //    }else if(fileName.equals("P-SignaturesRandom")){
  //      if(numBatchFiles>0){
  //        return dir + (withBoundaries? "/p-b":"/p")+"-signatures-random_D="+numOfBits+"_batch="+numBatchFiles;
  //      }else{
  //        return dir + (withBoundaries? "/p-b":"/p")+"-signatures-random_D="+numOfBits;
  //      }
  //    }else if(fileName.equals("BatchSignaturesMap")){
  //      if(numBatchFiles>0){
  //        return dir + "/map-signatures-"+signatureType+"_L="+batchIndexKeyLength+"_Q="+numOfPermutations+"_D="+numOfBits+"_batch="+numBatchFiles;
  //      }else{
  //        return dir + "/map-signatures-"+signatureType+"_L="+batchIndexKeyLength+"_Q="+numOfPermutations+"_D="+numOfBits;
  //      }		
  //      /*}else if(fileName.equals("P-Bnd-BatchSignaturesMap")){
  //			return dir + "/p-bnd-map-signatures-"+signatureType+"_L="+batchIndexKeyLength+"_Q="+numOfPermutations+"_D="+numOfBits;*/
  //    }else if(fileName.equals("PWSim")){
  //      return dir + "/similardocs_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize;	//for backward compatibility. may remove later.
  //
  //    }else if(fileName.equals("PWSimCollectionFiltered")){
  //      if(batchIndexKeyLength>0)
  //        return dir + "/similardocs_hybrid_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize+"_L="+batchIndexKeyLength+(pairwiseWithinChunk?"_pw":"")+"-filtered_top"+numResults;
  //      else
  //        return dir + "/similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize+"-filtered_top"+numResults;
  //
  //    }else if(fileName.equals("P-PWSimCollection")){
  //      if(batchIndexKeyLength>0)
  //        return dir + (withBoundaries? "/p-b":"/p")+ "-similardocs_hybrid_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize+"_L="+batchIndexKeyLength+(pairwiseWithinChunk?"_pw":"");
  //      else
  //        return dir + (withBoundaries? "/p-b":"/p")+"-similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize;
  //
  //    }else if(fileName.equals("PCP")){
  //      return dir + "/pcp-dfcut=" + dfCut+"_Th="+scoreThreshold;
  //
  //    }else if(fileName.equals("EvaluateGolden")){
  //      if(chunkOverlapSize > 0)
  //        return dir + "/eval_similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_L="+batchIndexKeyLength):("_B="+slidingWindowSize));
  //      else 
  //        return dir + "/eval_similardocs_"+((numBatchFiles>0)?"batch":"hybrid")+"_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_B="+slidingWindowSize+"_L="+batchIndexKeyLength):"")+((numBatchFiles==0 && pairwiseWithinChunk)?"_pw":"");
  //
  //    }else if(fileName.equals("PCP-EvaluateGolden")){
  //      if(chunkOverlapSize > 0)
  //        return dir + "/eval-pcp-dfcut=" + dfCut+"_Th="+scoreThreshold;
  //      else 
  //        return dir + "/eval_similardocs_"+((numBatchFiles>0)?"batch":"hybrid")+"_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_B="+slidingWindowSize+"_L="+batchIndexKeyLength):"")+((numBatchFiles==0 && pairwiseWithinChunk)?"_pw":"");
  //
  //    }else if(fileName.equals("U-EvaluateGolden")){
  //      if(chunkOverlapSize > 0)
  //        return dir + (withBoundaries? "/u-p-b":"/u-p")+"-eval_similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_L="+batchIndexKeyLength):("_B="+slidingWindowSize));
  //      else 
  //        return dir + (withBoundaries? "/u-p-b":"/u-p")+"-eval_similardocs_"+((numBatchFiles>0)?"batch":"hybrid")+"_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_B="+slidingWindowSize+"_L="+batchIndexKeyLength):"")+((numBatchFiles==0 && pairwiseWithinChunk)?"_pw":"");
  //
  //    }else if(fileName.equals("P-EvaluateGolden")){
  //      if(chunkOverlapSize > 0)
  //        return dir + (withBoundaries? "/p-b":"/p")+"-eval_similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_L="+batchIndexKeyLength):("_B="+slidingWindowSize));
  //      else 
  //        return dir + (withBoundaries? "/p-b":"/p")+"-eval_similardocs_"+((numBatchFiles>0)?"batch":"hybrid")+"_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_B="+slidingWindowSize+"_L="+batchIndexKeyLength):"")+((numBatchFiles==0 && pairwiseWithinChunk)?"_pw":"");
  //
  //    }else if(fileName.equals("PWSimBatch")){
  //      return dir + "/similardocs_batch_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_L="+batchIndexKeyLength;
  //
  //    }else if(fileName.equals("P-Bnd-PWSimBatch")){
  //      return dir + "/p-bnd-similardocs_batch_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_L="+batchIndexKeyLength;
  //
  //    }else if(fileName.equals("Tables")){
  //      if(batchIndexKeyLength>0){
  //        return dir + "/tablesIndexed_"+signatureType+"_D="+numOfBits+"_keylen="+batchIndexKeyLength+"_Q="+numOfPermutations;
  //      }else{
  //        return dir + "/tables_"+signatureType+"_D="+numOfBits+"_V="+chunkOverlapSize+"_Q="+numOfPermutations;	
  //      }
  //
  //    }else if(fileName.equals("P-Tables")){
  //      if(batchIndexKeyLength>0){
  //        return dir + (withBoundaries? "/p-b":"/p")+ "-tablesIndexed_"+signatureType+"_D="+numOfBits+"_keylen="+batchIndexKeyLength+"_Q="+numOfPermutations;
  //      }else{
  //        return dir + (withBoundaries? "/p-b":"/p")+ "-tables_"+signatureType+"_D="+numOfBits+"_V="+chunkOverlapSize+"_Q="+numOfPermutations;	
  //      }
  //
  //    }else if(fileName.equals("Permsbit")){
  //      return dir + "/random-perms-bit"+"_D="+numOfBits+"_Q="+numOfPermutations;
  //
  //    }else if(fileName.equals("Permsblk")){
  //      return dir + "/random-perms-blk"+"_D="+numOfBits+"_Q="+numOfPermutations;
  //
  //    }else if(fileName.equals("EvalSeeAlso")){
  //      return dir + "/eval-seealso_coll_perm=bit_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize;
  //
  //    }else if(fileName.equals("DuplicateSets")){
  //      return dir + "/sets_similardocs_coll_perm=bit_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize;
  //    }
  //
  //    else{
  //      throw new RuntimeException("Could not process file name: "+fileName);
  //    }
  //  }
}
