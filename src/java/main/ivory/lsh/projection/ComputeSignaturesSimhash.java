package ivory.lsh.projection;

import ivory.core.RetrievalEnvironment;
import ivory.lsh.data.SixtyFourBitSignature;
import ivory.lsh.driver.PwsimEnvironment;

import java.io.IOException;

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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.MapKF.Entry;


/**
 *	A Hadoop task to compute signatures from document vectors.
 * 
 * @author ferhanture
 * 
 *
 */		
@SuppressWarnings("deprecation")
public class ComputeSignaturesSimhash extends PowerTool {
	public static final String[] RequiredParameters = {"Ivory.NumOfBits"};
	private static final Logger sLogger = Logger.getLogger(ComputeSignaturesSimhash.class);

	static {
		sLogger.setLevel(Level.INFO);
	}
	
	public ComputeSignaturesSimhash(Configuration conf) {
		super(conf);
	}
	
	protected static enum Maps {
		ALL,ONES,ZEROS
	};

	/** 
	 * Simhash implementation, as explained in Manku et al's Detecting near-duplicates for web crawling (WWW07)
	 * 
	 * @author ferhanture
	 *
	 */
	public static class MyMapper extends MapReduceBase implements
	Mapper<IntWritable, HMapSFW, IntWritable, SixtyFourBitSignature> {

		static GeneralHashFunctionLibrary hashLib;
		static float[] V = new float[64];   
		static SixtyFourBitSignature s = new SixtyFourBitSignature();

		public void configure(JobConf job){
			hashLib = new GeneralHashFunctionLibrary();
		}

		public void map(IntWritable docno, HMapSFW docvector,
				OutputCollector<IntWritable, SixtyFourBitSignature> output,
				Reporter reporter) throws IOException {			
			V = new float[64];
			for(Entry<String> entry : docvector.entrySet()){
				String term = entry.getKey();
				float weight = entry.getValue();
				
				long hashL = hashLib.APHash(term);
				for(int i=0;i<64;i++){
					int bit =  (int) ((hashL >>> i)&1);
					if(bit==1){
						V[i]+=weight;
					}else{
						V[i]-=weight;		
					}
					
				}
			}
			int i=0;
			for(float f: V){
				if(f>0){
					s.set(i, true);
				}else{
					s.set(i, false);
				}
				i++;
			}
			
//			sLogger.info(docno+","+s+" --> "+docvector);
			output.collect(docno, s);
		}
	}


	public int runTool() throws Exception {
		sLogger.setLevel(Level.INFO);
		
		Configuration conf = getConf();
//		int D = conf.getInt("Ivory.NumOfBits", -1);
		int numBatchFiles = conf.getInt("NumBatch", 0);
		boolean isBatch = (numBatchFiles!=0);
		String dir = conf.get("Ivory.IndexPath");	
		if(numBatchFiles<0){
			System.out.println("numBatchFiles: "+numBatchFiles);
			throw new RuntimeException("Parameters not read properly! Quitting...");
		}
		JobConf job = new JobConf(conf, ComputeSignaturesSimhash.class);
		FileSystem fs = FileSystem.get(job);
		RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
		job.setJobName("ComputeSignatures_simhash"+"_D=64_"+env.readCollectionName());

		String inputPath = PwsimEnvironment.getFileNameWithPars(dir, "TermDocs");
		String outputPath = PwsimEnvironment.getFileNameWithPars(dir, "SignaturesSimhash");
		
		int numMappers = 300;

		if(fs.exists(new Path(outputPath))){
			sLogger.info("Signatures output path already exists! Quitting...");
			return 0;
		}

		sLogger.info("Computing signatures...");
		sLogger.info("Type of computation: Simhash");
		sLogger.info("Total number of bits: 64");
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
		job.setMapOutputValueClass(SixtyFourBitSignature.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SixtyFourBitSignature.class);
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
