package ivory.lsh.pwsim;

import ivory.lsh.data.BitsSignatureTable;
import ivory.lsh.data.PairOfIntSignature;
import ivory.lsh.data.Permutation;
import ivory.lsh.data.PermutationByBit;
import ivory.lsh.data.Signature;
import ivory.lsh.driver.PwsimEnvironment;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
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


import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.util.PowerTool;

@SuppressWarnings("deprecation")
public class GenerateChunkedPermutedTables extends PowerTool{
	private static final Logger sLogger = Logger.getLogger(GenerateChunkedPermutedTables.class);
	static {
		sLogger.setLevel(Level.WARN);
	}

	static enum Count{
		Signatures, Chunks, SignaturesInChunks
	}


	public GenerateChunkedPermutedTables(Configuration conf) {
		super(conf);
	}

	/**
	 * @author ferhanture
	 *
	 *		Maps each signature to Q random permutations, usign the permuters stored in local cache file
	 *		<docno,signature> --> <(permno,signature),docno>
	 */

	@SuppressWarnings("unchecked")
	public static class MyMapper extends MapReduceBase implements
	Mapper<IntWritable, Signature, PairOfIntSignature, IntWritable> {

		static Path[] localFiles;
		static List<Writable> randomPermutations;
		static int numOfPermutations;
		static Signature permutedSignature;
		static Constructor pairConstructor;
		static PairOfIntSignature pair;

		public void configure(JobConf job){
			numOfPermutations = job.getInt("Ivory.NumOfPermutations", -1);
			int numOfBits =  job.getInt("Ivory.NumOfBits", -1);
			Class signatureClass = null;

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

			sLogger.debug ("Reading permutations file....");
			sLogger.debug ("PwsimEnvironment.cluster: " + PwsimEnvironment.cluster);
			sLogger.debug ("PwsimEnvironment.cluster: " + PwsimEnvironment.cluster);
			//sLogger.debug ("job.get(\"mapred.job.tracker\"): " + job.get ("mapred.job.tracker"));
			//sLogger.debug ("job.get(\"mapred.job.tracker\").equals (\"local\"): " + job.get ("mapred.job.tracker").equals ("local"));
			try {
				if (job.get ("mapred.job.tracker").equals ("local")) {
					String rootPath = job.get ("Ivory.IndexPath");
					String randomPermFile = PwsimEnvironment.getFileNameWithPars (rootPath, "Permsbit");
					randomPermutations = SequenceFileUtils.readValues(new Path (randomPermFile), FileSystem.getLocal(job));
					//					randomPermutations = edu.umd.cloud9.util.SequenceFileUtils.readFile("index/random.perms", new Text(""), -1);
				}else{
					localFiles = DistributedCache.getLocalCacheFiles (job);
					//sLogger.debug ("localFiles [0]: " + localFiles [0]);
					randomPermutations = SequenceFileUtils.readValues(localFiles[0], FileSystem.getLocal(job));
				}
			} catch (Exception e) {
				throw new RuntimeException("Error reading random vectors!");
			}
			sLogger.debug("Done reading file.");
		}

		public void map(IntWritable docno, Signature signature, OutputCollector<PairOfIntSignature, IntWritable> output,
				Reporter reporter) throws IOException {
			sLogger.debug("Mapping signature "+docno);
			sLogger.debug("Permuting "+ signature +"...");

			//Map each signature to Q random permutations
			for(int i=0;i<numOfPermutations;i++){
				signature.perm((ArrayListOfIntsWritable) randomPermutations.get(i), permutedSignature);
				//				sLogger.debug("Permutation "+i+" : "+permutedSign);
				try {
					pair.setInt(i);
					pair.setSignature(permutedSignature);
					output.collect(pair, docno);
				} catch (Exception e){
					throw new RuntimeException("output.collect exception: \n"+e.toString());
				}
				sLogger.debug("emitted");
			}
			reporter.incrCounter(Count.Signatures, 1);
		}
	}

	public static class MyPartitioner implements Partitioner<PairOfIntSignature,IntWritable>{
		// partition with permutation number only
		public int getPartition(PairOfIntSignature key,IntWritable value,int numReducers){
			return key.getInt()%numReducers;
		}
		public void configure(JobConf conf) {			
		}
	}

	public static class MyReducer extends MapReduceBase implements
	Reducer<PairOfIntSignature, IntWritable, IntWritable, BitsSignatureTable> {
		static int permNo = -1;
		Signature[] signatures = null;
		int[] docNos = null;
		int curTableSize = 0;
		int overlapSize = -1;
		int chunckSize = -1;
		public void configure(JobConf conf){
//			sLogger.setLevel(Level.DEBUG);
			overlapSize = conf.getInt("Ivory.OverlapSize", -1);
			chunckSize = conf.getInt("Ivory.ChunckSize", -1);
			if(overlapSize >= chunckSize) throw new RuntimeException(
					"Invalid Ivory.OverlapSize("+overlapSize+") or Ivory.ChunkSize("+chunckSize+")");
			signatures = new Signature[chunckSize];
			docNos = new int[chunckSize];
		}

		BitsSignatureTable table = new BitsSignatureTable();
		IntWritable outKey = new IntWritable();
		OutputCollector<IntWritable, BitsSignatureTable> mOutput = null;
		PairOfIntSignature lastKey = null;
		Reporter mReporter = null;
		public void reduce(PairOfIntSignature key, Iterator<IntWritable> val,
				OutputCollector<IntWritable, BitsSignatureTable> output, Reporter reporter)
		throws IOException {
			mReporter = reporter;
			// all signatures: 
			// (1) belong to the same permutation table, and
			// (2) sorted
			mOutput = output;
			lastKey = key;
			//sLogger.debug(key.toString());
			while(val.hasNext()){
				docNos[curTableSize] = val.next().get();
				signatures[curTableSize] = key.getSignature();
				curTableSize++;
				if(curTableSize == chunckSize){
					table.set(signatures, docNos, curTableSize);
					outKey.set(key.getInt());
					output.collect(outKey, table);
					reporter.incrCounter(Count.SignaturesInChunks, table.getNumOfSignatures());
					reporter.incrCounter(Count.Chunks, 1);
					shiftOverlap();
				}
			}
		}
		   
		private void shiftOverlap(){
			if(overlapSize >= curTableSize) return;
			
			// overlapSize < curTableSize ==> shift up
			int j = 0;
			for(int i = curTableSize-overlapSize; i<curTableSize; i++){
				signatures[j] = signatures[i];
				docNos[j] = docNos[i];
				j++;
			}
			curTableSize = j;
		}

		@Override
		public void close() throws IOException {
			if(curTableSize == 0 || mOutput == null) return;
			table.set(signatures, docNos, curTableSize);
			outKey.set(lastKey.getInt());
			mOutput.collect(outKey, table);
			mReporter.incrCounter(Count.SignaturesInChunks, table.getNumOfSignatures());
			mReporter.incrCounter(Count.Chunks, 1);
			
//			sLogger.debug(nSignaturesRead);
		}

	}

	@Override
	public String[] getRequiredParameters() {
		return RequiredParameters ;
	}

	//create Q permutation functions and write them to file
	public static String createPermutations(FileSystem fs, JobConf job, String rootPath, int numBits, int numOfPermutations) throws Exception{
		String randomPermFile = PwsimEnvironment.getFileNameWithPars(rootPath, "Permsbit");
		if(fs.exists(new Path(randomPermFile))){
			sLogger.info("Random permutations output path already exists!");
			return randomPermFile;
		}
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, job, new Path(randomPermFile), IntWritable.class, ArrayListOfIntsWritable.class);
		Permutation p = new PermutationByBit(numBits);
		for(int i=0;i<numOfPermutations;i++){
			ArrayListOfIntsWritable perm = p.nextPermutation();
			writer.append(new IntWritable(i), perm);
			sLogger.debug(i +":"+perm);
		}
		writer.close();
		sLogger.info("Random permutations written.");
		return randomPermFile;
	}
	public static final String[] RequiredParameters = {
		"Ivory.NumMapTasks",
		"Ivory.NumReduceTasks",
		"Ivory.CollectionName",
		"Ivory.IndexPath",
		"Ivory.NumOfPermutations",
		"Ivory.ChunckSize",
		"Ivory.OverlapSize"
	};

	@Override
	public int runTool() throws Exception {
		sLogger.setLevel(Level.INFO);
		int numOfPermutations = getConf().getInt("Ivory.NumOfPermutations", -1);
		int numBits = getConf().getInt("Ivory.NumOfBits", -1);
		if(numOfPermutations<0)
			throw new RuntimeException("parameters not read properly");

		String rootPath = getConf().get("Ivory.IndexPath");	

		JobConf job = new JobConf(getConf(), GenerateChunkedPermutedTables.class);

		FileSystem fs = FileSystem.get(job);
		String inputPath, outputPath, fileno;
		String collectionName = job.get("Ivory.CollectionName");
		
		if(job.getBoolean("Ivory.SignaturesPartitioned", false)){
			inputPath = job.get("Ivory.PartitionFile");
			fileno = inputPath.substring(inputPath.lastIndexOf('-')+1);
			outputPath = PwsimEnvironment.getFileNameWithPars(rootPath, "P-Tables")+"/"+fileno;
			job.setJobName("GenerateChunkedPermutedTables: " + collectionName+"-p#"+Integer.parseInt(fileno));
		}
		else{
			inputPath = PwsimEnvironment.getFileNameWithPars(rootPath, "Signatures"+job.get("Type"));
			outputPath = PwsimEnvironment.getFileNameWithPars(rootPath, "Tables");
			job.setJobName("GenerateChunkedPermutedTables: " + numOfPermutations + "_" + collectionName);
		}
		int numMappers = job.getInt("Ivory.NumMapTasks", 100);
		int numReducers = job.getInt("Ivory.NumReduceTasks", 100);

		if(fs.exists(new Path(outputPath))){
			sLogger.info("Permuted tables already exist! Quitting...");
			return 0;
		}	

		//create Q permutation functions and write them to file
		String randomPermFile = createPermutations(fs, job, rootPath, numBits, numOfPermutations);
		DistributedCache.addCacheFile(new URI(randomPermFile), job);

		FileInputFormat.addInputPath(job, new Path(inputPath));

		//ONLY FOR CROSS-LINGUAL CASE
		if(PwsimEnvironment.isCrossLingual){
			String srcLangInputPath = PwsimEnvironment.getFileNameWithPars(job.get("SrcLangDir"), "Signatures");
			FileInputFormat.addInputPath(job, new Path(srcLangInputPath));
		}
		Path[] paths = FileInputFormat.getInputPaths(job);

		for(Path path : paths){
			sLogger.info("Added "+path);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		//job.setInt("mapred.map.max.attempts", 10);
		//job.setInt("mapred.reduce.max.attempts", 10);
		job.setInt("mapred.task.timeout", 600000000);

		job.setNumMapTasks(numMappers);
		job.setNumReduceTasks(numReducers);
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Class.forName(job.get("Ivory.PairClass")));
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BitsSignatureTable.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		sLogger.info("Running job "+job.getJobName()+"...");
		sLogger.info("Collection: "+collectionName);
		sLogger.info("Number of bits/signature(D): "+numBits);
		sLogger.info("Number of permutations(Q): "+numOfPermutations);
		sLogger.info("Overlap size: "+getConf().getInt("Ivory.OverlapSize", -1));
		sLogger.info("Chunk size: "+getConf().getInt("Ivory.ChunckSize", -1));

		JobClient.runJob(job);

		return 0;
	}
}
