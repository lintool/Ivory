package ivory.lsh.eval;

import ivory.lsh.data.MinhashSignature;
import ivory.lsh.data.NBitSignature;
import ivory.lsh.data.Signature;
import ivory.lsh.data.SixtyFourBitSignature;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umd.cloud9.io.FSLineReader;
import edu.umd.cloud9.io.map.HMapIIW;


@SuppressWarnings("deprecation")
public class SampleSignatures  extends Configured implements Tool{
	public static final String[] RequiredParameters = {};
	private static final Logger sLogger = Logger.getLogger(SampleSignatures.class);

	static enum mapoutput{
		count
	};

	private static int printUsage() {
		System.out.println("usage: [signatures-path] [sample-signatures-path] [signature-type] [sample-frequency] ([sample-docnos-path])\nSignature type is either random, simhash or minhash.");
		return -1;
	}

	public SampleSignatures() {
		super();
	}

	/**
	 * 	Filter signatures that are not from sample.
	 *  
	 * @author ferhanture
	 *
	 */
	public static class MyMapper extends MapReduceBase implements
	Mapper<IntWritable, Signature, IntWritable, Signature> {

		static Path[] localFiles;
		HMapIIW samplesMap = null;
		static int sampleFreq;

		public void configure(JobConf job){
			sLogger.setLevel(Level.INFO);
			
			sampleFreq = job.getInt("SampleFrequency", -1);

			//read doc ids of sample into vectors
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (Exception e) {
				throw new RuntimeException("Error reading doc vectors!");
			}

			if(localFiles!=null && localFiles.length > 0){
				samplesMap = new HMapIIW();
				try {
					FSLineReader reader = new FSLineReader(localFiles[0], FileSystem.getLocal(job));
					Text t = new Text();
					while(reader.readLine(t)!=0){
						int docno = Integer.parseInt(t.toString());
						samplesMap.put(docno, 1);
					}
					reader.close();
				} catch (IOException e1) {
				}
				sLogger.info(samplesMap);
			}
		}

		public void map(IntWritable key, Signature value,
				OutputCollector<IntWritable, Signature> output,
				Reporter reporter) throws IOException {
			sLogger.debug(key);
			if(samplesMap != null){
				if(samplesMap.containsKey(key.get())){
					reporter.incrCounter(mapoutput.count, 1);
					output.collect(key, value);
				}
			}else{
				int randInt = (int) (Math.random()*sampleFreq); 	//integer in [0,sampleFrq)
				if(randInt==0){	
					reporter.incrCounter(mapoutput.count, 1);
					output.collect(key, value);
				}
			}
		}
	}


	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public int run(String[] args) throws Exception {
		if (args.length != 4 && args.length != 5) {
			printUsage();
			return -1;
		}
		JobConf job2 = new JobConf(getConf(), SampleSignatures.class);
		job2.setJobName(this.getClass().getName());
		FileSystem fs2 = FileSystem.get(job2);

		String inputPath2 = args[0];//PwsimEnvironment.getFileNameWithPars(dir, "SignaturesRandom");
		String outputPath2 = args[1];//PwsimEnvironment.getFileNameWithPars(dir, "SignaturesRandom")+"-sample";
		int sampleFreq = Integer.parseInt(args[3]);

		int numMappers2 = 100;
		int numReducers2 = 1;

		if(fs2.exists(new Path(outputPath2))){
			sLogger.info("Sample signatures output already exists! Quitting...");
			return 0;
		}	
		FileInputFormat.setInputPaths(job2, new Path(inputPath2));
		FileOutputFormat.setOutputPath(job2, new Path(outputPath2));
		FileOutputFormat.setCompressOutput(job2, false);

		// if sample docnos path provided,
		if(args.length == 5){
			sampleFreq = -1;	//ignore sample frequency
			DistributedCache.addCacheFile(new URI(args[4]), job2);	//sample doc vectors in file
		}

		job2.set("mapred.child.java.opts", "-Xmx2048m");
		job2.setInt("mapred.map.max.attempts", 10);
		job2.setInt("mapred.reduce.max.attempts", 10);
		job2.setInt("mapred.task.timeout", 6000000);
		job2.setInt("SampleFrequency", sampleFreq);

		sLogger.info("Running job "+job2.getJobName());
		sLogger.info("Input directory: "+inputPath2);
		sLogger.info("Output directory: "+outputPath2);
		sLogger.info("Sample frequency: "+sampleFreq);

		job2.setNumMapTasks(numMappers2);
		job2.setNumReduceTasks(numReducers2);
		job2.setInputFormat(SequenceFileInputFormat.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		if(args[2].equals("simhash")){
			job2.setMapOutputValueClass(SixtyFourBitSignature.class);
			job2.setOutputValueClass(SixtyFourBitSignature.class);
		}else if(args[2].equals("random")){
			job2.setMapOutputValueClass(NBitSignature.class);		
			job2.setOutputValueClass(NBitSignature.class);
		}else if(args[2].equals("minhash")){
			job2.setMapOutputValueClass(MinhashSignature.class);
			job2.setOutputValueClass(MinhashSignature.class);
		}else{
			throw new RuntimeException("Unknown signature type "+args[2]);
		}
		job2.setOutputKeyClass(IntWritable.class);
		job2.setMapperClass(MyMapper.class);
		job2.setReducerClass(IdentityReducer.class);
		job2.setOutputFormat(SequenceFileOutputFormat.class);			

		JobClient.runJob(job2);

		return 0;
	}

	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(), new SampleSignatures(), args);
		return;
	}
}
