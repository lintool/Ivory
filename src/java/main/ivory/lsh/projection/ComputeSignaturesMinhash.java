package ivory.lsh.projection;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.document.WeightedIntDocVector;
import ivory.lsh.data.MinhashSignature;
import ivory.lsh.data.Permutation;
import ivory.lsh.data.PermutationByBit;
import ivory.lsh.driver.PwsimEnvironment;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.util.PowerTool;


/**
 *	A Hadoop task to compute signatures from document vectors.
 * 
 * @author ferhanture
 * 
 *
 */
@SuppressWarnings("deprecation")
public class ComputeSignaturesMinhash extends PowerTool {

	public ComputeSignaturesMinhash(Configuration conf) {
		super(conf);
	}

	public static final String[] RequiredParameters = {};
	private static final Logger sLogger = Logger.getLogger(ComputeSignaturesMinhash.class);

	static {
		sLogger.setLevel(Level.INFO);
	}

	@SuppressWarnings("unused")
	private static int printUsage() {
		System.out.println("usage: [index-path] [num-of-bits] [type-of-computation] ([batch-size])");
		return -1;
	}

	protected static enum Maps {
		ALL,ONES,ZEROS,EMPTY
	};

	//	public ComputeSignatures(Configuration conf) {
	//		super(conf);
	//	}

	/** 
	 * Signatures are created in a sequence of MyMapper calls. Each call appends SIZE_OF_FILE bits to each signature,
	 *	until the size reaches D.
	 * 
	 * @author ferhanture
	 *
	 */
	public static class MyMapper extends MapReduceBase implements
	Mapper<IntWritable, WeightedIntDocVector, IntWritable, MinhashSignature> {

		static Path[] localFiles;
		static int D;
		static MinhashSignature signature;
		static List<Writable> randomOrderings;

		public void configure(JobConf job){
//			sLogger.setLevel(Level.DEBUG);
			D = job.getInt("Ivory.NumOfBits", -1);
			if(D==-1){
				throw new RuntimeException("Could not read parameters!");
			}

			if(PwsimEnvironment.cluster){
				try {
					localFiles = DistributedCache.getLocalCacheFiles(job);
					randomOrderings = SequenceFileUtils.readValues(localFiles[0], FileSystem.getLocal(job));
				} catch (Exception e) {
					throw new RuntimeException("Error reading random vectors!");
				}
			}else{
				try {
					randomOrderings = SequenceFileUtils.readValues(new Path(job.get("Ivory.IndexPath")+"/random-perms-bit_D=39869_Q=2"), FileSystem.getLocal(job));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(randomOrderings.size()!=D){
				throw new RuntimeException("No of random orderings not correct. Something is wrong!");		//CODE SHOULD NOT COME HERE
			}
			signature = new MinhashSignature(D);

		}

		public void map(IntWritable docno, WeightedIntDocVector docvectorIn,
				OutputCollector<IntWritable, MinhashSignature> output,
				Reporter reporter) throws IOException {
			HMapIFW docvector = docvectorIn.getWeightedTerms();
			signature.clear();

			for(int i=0;i<randomOrderings.size();i++){
				int minTerm = getMinHashTerm(docvector, (ArrayListOfIntsWritable) randomOrderings.get(i));
				signature.add(minTerm);
			}
			sLogger.debug("Doc vector " + docvector + " mapped to \nBitsSignature: "+docno+"\n"+signature);
			output.collect(docno, signature);
		}

		private int getMinHashTerm(HMapIFW docvector,
				ArrayListOfIntsWritable ordering) {
			for(int i=0;i<ordering.size();i++){
				int term = ordering.get(i);
				if(docvector.containsKey(term)){
					return term;
				}
			}
			throw new RuntimeException("No terms in doc vector. Something is wrong!");		//CODE SHOULD NOT COME HERE
		}
	}

	public int runTool() throws Exception {
		Configuration conf = getConf();
		int numInts = conf.getInt("Ivory.NumOfBits", -1);
		int numBatchFiles = conf.getInt("NumBatch", 0);
		boolean isBatch = (numBatchFiles!=0);
		String dir = conf.get("Ivory.IndexPath");	
		if(numInts<0 || numBatchFiles<0){
			throw new RuntimeException("Parameters not read properly! Quitting...");
		}
		JobConf job = new JobConf(conf, ComputeSignaturesMinhash.class);
//		job.set("mapred.job.tracker", "local");
//		job.set("fs.default.name", "file:///");
		FileSystem fs = FileSystem.get(job);
		RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
		int vocabSize = (int) env.readCollectionTermCount();

		job.setJobName("ComputeSignatures_minhash");//+"_D="+D+"_"+RetrievalEnvironment.readCollectionName(fs, dir));

		String inputPath = PwsimEnvironment.getFileNameWithPars(dir, "IntDocs");
		String outputPath = PwsimEnvironment.getFileNameWithPars(dir, "SignaturesMinhash");
		int numMappers = 300;
		if(fs.exists(new Path(outputPath))){
			sLogger.info("Signatures output path already exists! Quitting...");
			return 0;
		}
		
		//if doesn't exist, create Q permutations of bit positions [1..D]
		
		//to make the file path correct:
		PwsimEnvironment.numOfBits = vocabSize;
		PwsimEnvironment.numOfPermutations = numInts;
		String randomPermFile = PwsimEnvironment.getFileNameWithPars(dir, "Permsbit");
		//fix them back
		PwsimEnvironment.numOfBits = numInts;
		PwsimEnvironment.numOfPermutations = 0;	//doesn't matter. not used to compute signatures
		
		if(fs.exists(new Path(randomPermFile))){
			sLogger.info("Random permutations output path already exists!");
		}else{
			Permutation p = new PermutationByBit(vocabSize);
			Permutation.writeToFile(p, numInts, fs, job, randomPermFile);	
		}
		DistributedCache.addCacheFile(new URI(randomPermFile), job);

		sLogger.info("Computing signatures...");
		sLogger.info("Type of computation: Minhash");
		sLogger.info("Total number of ints: "+numInts);
		sLogger.info("random perms file: "+randomPermFile);
		sLogger.info("InputPath: "+inputPath);
		sLogger.info("outputPath: "+outputPath);
		sLogger.info("Batch?: "+isBatch);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		job.setInt("mapred.map.max.attempts", 10);
		job.setInt("mapred.reduce.max.attempts", 10);
		job.setInt("mapred.task.timeout", 6000000);

		job.setNumMapTasks(numMappers);
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MinhashSignature.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MinhashSignature.class);
		job.setMapperClass(MyMapper.class);

		job.setOutputFormat(SequenceFileOutputFormat.class);			

		if(isBatch){
			job.setNumReduceTasks(numBatchFiles);
			job.setReducerClass(IdentityReducer.class);
		}else{
			job.setNumReduceTasks(0);
		}
		long startTime = System.currentTimeMillis();
		JobClient.runJob(job);
		System.out.println("Job finished in "+(System.currentTimeMillis()-startTime)+" milliseconds");

		return 0;
	}

	@Override
	public String[] getRequiredParameters() {
		return new String[]{};
	}

}
