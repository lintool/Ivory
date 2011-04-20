package ivory.lsh.driver;
import ivory.util.RetrievalEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
	
	public static void setClassTypes(Configuration config){
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
	
	public static String getFileNameWithPars(String dir, String fileName) throws Exception{
		RetrievalEnvironment env = new RetrievalEnvironment(dir, FileSystem.get(new Configuration()));
		if(fileName.equals("TermDocs")){
			return env.getWeightedTermDocVectorsDirectory();
		}else if(fileName.equals("IntDocs")){
			return env.getWeightedIntDocVectorsDirectory();
		}else if(fileName.equals("SampleIntDocs")){
			String s = env.getWeightedIntDocVectorsDirectory();
			return s.substring(0, s.length()-1)+"_sample="+sampleSize;
		}else if(fileName.equals("SampleDocnos")){
			return dir + "/sample-docnos_"+sampleSize;
		}else if(fileName.equals("RandomVectors")){
			return dir + "/randomvectors_D="+numOfBits;
		/*}else if(fileName.equals("PartitionedSignatures")){
			return dir + "/partitioned-sigs";*/
		}else if(fileName.equals("SignaturesSimhash")){
			if(numBatchFiles>0){
				return dir + "/signatures-simhash_D="+numOfBits+"_batch="+numBatchFiles;
			}else{
				return dir + "/signatures-simhash_D="+numOfBits;
			}
		}else if(fileName.equals("SignaturesMinhash")){
			if(numBatchFiles>0){
				return dir + "/signatures-minhash_D="+numOfBits+"_batch="+numBatchFiles;
			}else{
				return dir + "/signatures-minhash_D="+numOfBits;
			}
		}else if(fileName.equals("SignaturesRandom")){
			if(numBatchFiles>0){
				return dir + "/signatures-random_D="+numOfBits+"_batch="+numBatchFiles;
			}else{
				return dir + "/signatures-random_D="+numOfBits;
			}
		}else if(fileName.equals("Signatures")){
			if(numBatchFiles>0){
				return dir + "/signatures-"+ signatureType +"_D="+numOfBits+"_batch="+numBatchFiles;
			}else{
				return dir + "/signatures-"+ signatureType +"_D="+numOfBits;
			}
		}else if(fileName.equals("SignaturesIndexable")){
			return dir + "/signatures-random-indx_D="+numOfBits;
		}else if(fileName.equals("P-SignaturesSimhash")){
			if(numBatchFiles>0){
				return dir + (withBoundaries? "/p-b":"/p")+"-signatures-simhash_D="+numOfBits+"_batch="+numBatchFiles;
			}else{
				return dir + (withBoundaries? "/p-b":"/p")+"-signatures-simhash_D="+numOfBits;
			}
		}else if(fileName.equals("P-SignaturesMinhash")){
			if(numBatchFiles>0){
				return dir + (withBoundaries? "/p-b":"/p")+"-signatures-minhash_D="+numOfBits+"_batch="+numBatchFiles;
			}else{
				return dir + (withBoundaries? "/p-b":"/p")+"-signatures-minhash_D="+numOfBits;
			}
		}else if(fileName.equals("P-SignaturesRandom")){
			if(numBatchFiles>0){
				return dir + (withBoundaries? "/p-b":"/p")+"-signatures-random_D="+numOfBits+"_batch="+numBatchFiles;
			}else{
				return dir + (withBoundaries? "/p-b":"/p")+"-signatures-random_D="+numOfBits;
			}
		}else if(fileName.equals("BatchSignaturesMap")){
			if(numBatchFiles>0){
				return dir + "/map-signatures-"+signatureType+"_L="+batchIndexKeyLength+"_Q="+numOfPermutations+"_D="+numOfBits+"_batch="+numBatchFiles;
			}else{
				return dir + "/map-signatures-"+signatureType+"_L="+batchIndexKeyLength+"_Q="+numOfPermutations+"_D="+numOfBits;
			}		
		/*}else if(fileName.equals("P-Bnd-BatchSignaturesMap")){
			return dir + "/p-bnd-map-signatures-"+signatureType+"_L="+batchIndexKeyLength+"_Q="+numOfPermutations+"_D="+numOfBits;*/
		}else if(fileName.equals("PWSim")){
			return dir + "/similardocs_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize;	//for backward compatibility. may remove later.
	
		}else if(fileName.equals("PWSimCollection")){
			if(batchIndexKeyLength>0)
				return dir + "/similardocs_hybrid_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize+"_L="+batchIndexKeyLength+(pairwiseWithinChunk?"_pw":"");
			else
				return dir + "/similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize;
	
		}else if(fileName.equals("PWSimCollectionFiltered")){
			if(batchIndexKeyLength>0)
				return dir + "/similardocs_hybrid_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize+"_L="+batchIndexKeyLength+(pairwiseWithinChunk?"_pw":"")+"-filtered_top"+numResults;
			else
				return dir + "/similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize+"-filtered_top"+numResults;
	
		}else if(fileName.equals("P-PWSimCollection")){
			if(batchIndexKeyLength>0)
				return dir + (withBoundaries? "/p-b":"/p")+ "-similardocs_hybrid_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize+"_L="+batchIndexKeyLength+(pairwiseWithinChunk?"_pw":"");
			else
				return dir + (withBoundaries? "/p-b":"/p")+"-similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize;
	
		}else if(fileName.equals("PCP")){
				return dir + "/pcp-dfcut=" + dfCut+"_Th="+scoreThreshold;

		}else if(fileName.equals("EvaluateGolden")){
			if(chunkOverlapSize > 0)
				return dir + "/eval_similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_L="+batchIndexKeyLength):("_B="+slidingWindowSize));
			else 
				return dir + "/eval_similardocs_"+((numBatchFiles>0)?"batch":"hybrid")+"_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_B="+slidingWindowSize+"_L="+batchIndexKeyLength):"")+((numBatchFiles==0 && pairwiseWithinChunk)?"_pw":"");
	
		}else if(fileName.equals("PCP-EvaluateGolden")){
			if(chunkOverlapSize > 0)
				return dir + "/eval-pcp-dfcut=" + dfCut+"_Th="+scoreThreshold;
			else 
				return dir + "/eval_similardocs_"+((numBatchFiles>0)?"batch":"hybrid")+"_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_B="+slidingWindowSize+"_L="+batchIndexKeyLength):"")+((numBatchFiles==0 && pairwiseWithinChunk)?"_pw":"");
		
		}else if(fileName.equals("U-EvaluateGolden")){
			if(chunkOverlapSize > 0)
				return dir + (withBoundaries? "/u-p-b":"/u-p")+"-eval_similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_L="+batchIndexKeyLength):("_B="+slidingWindowSize));
			else 
				return dir + (withBoundaries? "/u-p-b":"/u-p")+"-eval_similardocs_"+((numBatchFiles>0)?"batch":"hybrid")+"_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_B="+slidingWindowSize+"_L="+batchIndexKeyLength):"")+((numBatchFiles==0 && pairwiseWithinChunk)?"_pw":"");
		
		}else if(fileName.equals("P-EvaluateGolden")){
			if(chunkOverlapSize > 0)
				return dir + (withBoundaries? "/p-b":"/p")+"-eval_similardocs_coll_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_L="+batchIndexKeyLength):("_B="+slidingWindowSize));
			else 
				return dir + (withBoundaries? "/p-b":"/p")+"-eval_similardocs_"+((numBatchFiles>0)?"batch":"hybrid")+"_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+((batchIndexKeyLength>0)?("_B="+slidingWindowSize+"_L="+batchIndexKeyLength):"")+((numBatchFiles==0 && pairwiseWithinChunk)?"_pw":"");
		
		}else if(fileName.equals("PWSimBatch")){
			return dir + "/similardocs_batch_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_L="+batchIndexKeyLength;
		
		}else if(fileName.equals("P-Bnd-PWSimBatch")){
			return dir + "/p-bnd-similardocs_batch_"+signatureType+"_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_L="+batchIndexKeyLength;
		
		}else if(fileName.equals("Tables")){
			if(batchIndexKeyLength>0){
				return dir + "/tablesIndexed_"+signatureType+"_D="+numOfBits+"_keylen="+batchIndexKeyLength+"_Q="+numOfPermutations;
			}else{
				return dir + "/tables_"+signatureType+"_D="+numOfBits+"_V="+chunkOverlapSize+"_Q="+numOfPermutations;	
			}
		
		}else if(fileName.equals("P-Tables")){
			if(batchIndexKeyLength>0){
				return dir + (withBoundaries? "/p-b":"/p")+ "-tablesIndexed_"+signatureType+"_D="+numOfBits+"_keylen="+batchIndexKeyLength+"_Q="+numOfPermutations;
			}else{
				return dir + (withBoundaries? "/p-b":"/p")+ "-tables_"+signatureType+"_D="+numOfBits+"_V="+chunkOverlapSize+"_Q="+numOfPermutations;	
			}
		
		}else if(fileName.equals("Permsbit")){
			return dir + "/random-perms-bit"+"_D="+numOfBits+"_Q="+numOfPermutations;
		
		}else if(fileName.equals("Permsblk")){
			return dir + "/random-perms-blk"+"_D="+numOfBits+"_Q="+numOfPermutations;
		
		}else if(fileName.equals("EvalSeeAlso")){
			return dir + "/eval-seealso_coll_perm=bit_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize;
		
		}else if(fileName.equals("DuplicateSets")){
			return dir + "/sets_similardocs_coll_perm=bit_maxdst="+maxHammingDistance+"_D="+numOfBits+"_Q="+numOfPermutations+"_B="+slidingWindowSize;
		}
		
		else{
			throw new RuntimeException("Could not process file name: "+fileName);
		}
	}
}
