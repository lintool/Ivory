package ivory.spots.partition;
import ivory.data.DocLengthTable;
import ivory.data.DocLengthTable2B;
import ivory.data.DocLengthTable4B;
import ivory.lsh.data.Signature;

import ivory.lsh.data.PairOfIntSignature;
import ivory.lsh.driver.PwsimEnvironment;
import ivory.spots.Utils;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Iterator;

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
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.PowerTool;

public class PartitionSignatures extends PowerTool{
	private static final Logger sLogger = Logger.getLogger(PartitionSignatures.class);
	static {
		sLogger.setLevel(Level.WARN);
	}

	static enum Count{
		Partitions, PossibleSigsInPrevious, SigsInPrevious, PossibleSigsForNext, SigsForNext
	}


	public PartitionSignatures(Configuration conf) {
		super(conf);
	}

	/**
	 * @author ferhanture
	 *
	 *		Maps each signature to Q random permutations, usign the permuters stored in local cache file
	 *		<docno,signature> --> <(permno,signature),docno>
	 */

	public static class MyMapper extends MapReduceBase implements
	Mapper<IntWritable, Signature, IntWritable, PairOfIntSignature> {

		int c;
		boolean shortDocLengths = false;
		DocLengthTable mDLTable;

		int minAllowedSpotSigs;
		int spotSigsRange;
		float similarityThreshold;
		SpotSigsPartition[] partitions;

		static PairOfIntSignature pair;
		static Constructor pairConstructor;
		static Signature permutedSignature;
		//int boundaries = 3; // 0: in-partition only, 1: in previous partition only, 2: for next partition only, 3: ALL [Default]
		boolean withBoundaries = false; 
		int nPartitions = -1;
		public void configure(JobConf job){
			Class signatureClass = null;
			int numOfBits =  job.getInt("Ivory.NumOfBits", -1);
			try {
				Class pairClass = Class.forName(job.get("Ivory.PairClass"));
				pairConstructor = pairClass.getConstructor(int.class, Signature.class);
				signatureClass = Class.forName(job.get("Ivory.SignatureClass"));
				Constructor intConstructor = signatureClass.getConstructor(int.class);
				permutedSignature = (Signature) intConstructor.newInstance(numOfBits);
				pair = (PairOfIntSignature) pairConstructor.newInstance(0, permutedSignature);
			} catch (Exception e){
				throw new RuntimeException("config exception: \n"+e.toString());
			}
			
			shortDocLengths = job.getBoolean("Ivory.ShortDocLengths", false);
			Path[] localFiles;
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e2) {
				throw new RuntimeException("Local cache files not read properly.");
			}
			try {
				if(shortDocLengths)
					mDLTable = new DocLengthTable2B(localFiles[0], FileSystem.getLocal(job));
				else 
					mDLTable = new DocLengthTable4B(localFiles[0], FileSystem.getLocal(job));
			} catch (IOException e1) {
				throw new RuntimeException("Error loading dl table from "+localFiles[0]);
			}	

			minAllowedSpotSigs = job.getInt("Ivory.MinAllowedSpotSigs", 4);
			spotSigsRange = job.getInt("Ivory.SpotSigsRange", 0);
			similarityThreshold = job.getFloat("Ivory.SpotSigsSimilarityThreshold", 0.44f);

			sLogger.info("minAllowedSpotSigs: "+minAllowedSpotSigs);
			sLogger.info("spotSigsRange: "+spotSigsRange);
			sLogger.info("similarityThreshold: "+similarityThreshold);

			partitions = Utils.computePartitions(minAllowedSpotSigs, spotSigsRange, similarityThreshold);
			String s = ""; for(int k = 0; k< partitions.length; k++) s+=partitions[k].toString();sLogger.info("Partitions: "+s);
			nPartitions = partitions.length;
			withBoundaries = job.getBoolean("Ivory.WithBoundaries", false);

		}

		int doc, length;
		IntWritable outKey = new IntWritable();
		
		public void map(IntWritable docno, Signature signature, OutputCollector<IntWritable, PairOfIntSignature> output,
				Reporter reporter) throws IOException {
			doc = docno.get();
			length = mDLTable.getDocLength(doc);
			SpotSigsPartition p = Utils.getPartition(length, partitions);
			outKey.set(p.id);
			//sLogger.info(doc+"\t"+length+"\t"+p.id);
			pair.setInt(doc);
			pair.setSignature(signature);
			
			output.collect(outKey, pair);
			
			if(!withBoundaries) // w/o boundaries 
				return;
			if(p.id!=0){ // not the first partition
				reporter.incrCounter(Count.PossibleSigsInPrevious, 1);
				int maxLengthOfPrevPart = partitions[p.id -1].end;
				if((float)maxLengthOfPrevPart/length >= similarityThreshold){ // potential duplicate to docs in previous partition?
					reporter.incrCounter(Count.SigsInPrevious, 1);
					outKey.set(p.id - 1);
					//sLogger.info(doc+"\t"+length+"\t"+p.id);
					output.collect(outKey, pair);
				}
			}
			
			/*
			if(p.id!=0){
				reporter.incrCounter(Count.PossibleSigsInPrevious, 1);
				int maxLengthOfPrevPart = partitions[p.id -1].end;
				if((float)maxLengthOfPrevPart/length >= similarityThreshold){
					reporter.incrCounter(Count.SigsInPrevious, 1);
					outKey.set(p.id - 1 + nPartitions);
					//sLogger.info(doc+"\t"+length+"\t"+p.id);
					output.collect(outKey, pair);
				}
			}
			
			if(p.id!=nPartitions-1){
				reporter.incrCounter(Count.PossibleSigsForNext, 1);
				int maxLengthOfNextPart = partitions[p.id +1].begin;
				if((float)length/maxLengthOfNextPart >= similarityThreshold){
					reporter.incrCounter(Count.SigsForNext, 1);
					outKey.set(p.id + 2 * nPartitions);
					//sLogger.info(doc+"\t"+length+"\t"+p.id);
					output.collect(outKey, pair);
				}
			}*/
			/*
			if(boundaries == 0 || boundaries == 2)
				output.collect(outKey, pair);
			
			if((boundaries == 1 || boundaries == 2) && (p.id!=0)){
				reporter.incrCounter(Count.PossibleSigsInPrevious, 1);
				int maxLengthOfPrevPart = partitions[p.id -1].end;
				if((float)maxLengthOfPrevPart/length >= similarityThreshold){
					reporter.incrCounter(Count.SigsInPrevious, 1);
					outKey.set(p.id - 1);
					//sLogger.info(doc+"\t"+length+"\t"+p.id);
					output.collect(outKey, pair);
				}
			}*/
		}
	}

	public static class MyReducer extends MapReduceBase implements
	Reducer<IntWritable, PairOfIntSignature, IntWritable, Signature> {
		
		IntWritable outKey = new IntWritable();
		PairOfIntSignature pair;
		public void reduce(IntWritable key, Iterator<PairOfIntSignature> val,
				OutputCollector<IntWritable, Signature> output, Reporter reporter)
		throws IOException {
			reporter.incrCounter(Count.Partitions, 1);
			while(val.hasNext()){
				pair = val.next();
				outKey.set(pair.getInt());
				output.collect(outKey, pair.getSignature());
			}
		}
	}

	@Override
	public String[] getRequiredParameters() {
		return RequiredParameters ;
	}

	public static final String[] RequiredParameters = {
		//"Ivory.NumMapTasks",
		//"Ivory.NumReduceTasks",
		"Ivory.CollectionName",
		"Ivory.IndexPath",
		"Ivory.NumOfBits",
		"Ivory.PairClass",
		"Ivory.SignatureClass",
		//"Ivory.MinAllowedSpotSigs",
		//"Ivory.SpotSigsRange",
		//"Ivory.SpotSigsSimilarityThreshold", 
	};

	@Override
	public int runTool() throws Exception {

		String rootPath = getConf().get("Ivory.IndexPath");	

		JobConf job = new JobConf(getConf(), PartitionSignatures.class);

		FileSystem fs = FileSystem.get(job);

		RetrievalEnvironment env = new RetrievalEnvironment(rootPath, fs);
		
		String inputPath = PwsimEnvironment.getFileNameWithPars(rootPath, "Signatures"+job.get("Type"));
		//boolean withBoundaries = job.getBoolean("Ivory.WithBoundaries", false);
		String outputPath = PwsimEnvironment.getFileNameWithPars(rootPath, "P-Signatures"+job.get("Type"));
		/*
		if(boundaries == 0 || boundaries == 2)
			outputPath = PwsimEnvironment.getFileNameWithPars(rootPath, "PSignatures"+job.get("Type"));
		else if(boundaries == 1){
			outputPath = PwsimEnvironment.getFileNameWithPars(rootPath, "PSignatures"+job.get("Type"))+"-b";
		}
		else throw new Exception("Invalid boundaries parameter: "+boundaries + ", should be 0, 1, or 2");
		*/
		int numMappers = job.getInt("Ivory.NumMapTasks", 100);
		
		int minAllowedSpotSigs = job.getInt("Ivory.MinAllowedSpotSigs", 4);
		int spotSigsRange = job.getInt("Ivory.SpotSigsRange", 0);
		float similarityThreshold = job.getFloat("Ivory.SpotSigsSimilarityThreshold", 0.44f);
		SpotSigsPartition[] partitions = Utils.computePartitions(minAllowedSpotSigs, spotSigsRange, similarityThreshold);

		int numReducers;
		boolean withBoundaries = job.getBoolean("Ivory.WithBoundaries", false);
		/*if(withBoundaries)
			numReducers = partitions.length * 3;
		else*/ 
			numReducers = partitions.length;

		Path docLengthFile = env.getDoclengthsData();
		if (!fs.exists(docLengthFile)) {
			throw new RuntimeException("Error, doc-length data file " + docLengthFile + "doesn't exist!");
		}
		DistributedCache.addCacheFile(docLengthFile.toUri(), job);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));

		
		if(fs.exists(new Path(outputPath))){
			System.out.println("signatures are partitioned already! Quitting...");
			return 0;
			//fs.delete(new Path(outputPath), true);	
		}	
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		String collectionName = job.get("Ivory.CollectionName");
		job.setJobName("PartitionSignatures" + (withBoundaries? "(w/bnd): " : "(w/o bnd): ") + collectionName);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		//job.setInt("mapred.map.max.attempts", 10);
		//job.setInt("mapred.reduce.max.attempts", 10);
		job.setInt("mapred.task.timeout", 600000000);

		//job.setInt("Ivory.CollectionDocumentCount", env.readCollectionDocumentCount());
		
		job.setBoolean("Ivory.ShortDocLengths", true);
		job.setInt("Ivory.SpotSigsRange", 100);
		job.set("Ivory.SpotSigsSimilarityThreshold", 0.44+"");
		job.setInt("Ivory.MinAllowedSpotSigs", 4);
		
		job.setNumMapTasks(numMappers);
		job.setNumReduceTasks(numReducers);
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Class.forName(job.get("Ivory.PairClass")));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Class.forName(job.get("Ivory.SignatureClass")));
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		
		sLogger.info("Running job "+job.getJobName()+"...");

		JobClient.runJob(job);

		return 0;
	}
}
