package ivory.lsh.eval;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.TreeSet;

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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.FSLineReader;
import edu.umd.cloud9.io.map.HMapIIW;
import edu.umd.cloud9.io.pair.PairOfInts;

@SuppressWarnings("deprecation")
public class FilterResults extends Configured implements Tool {
	public static final String[] RequiredParameters = {};
	private static final Logger sLogger = Logger.getLogger(FilterResults.class);

	static enum mapoutput{
		count
	};
	
	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [sample-docnos] [threshold] [num-results]");
		return -1;
	}
	
	public FilterResults() {
		super();
	}

	/**
	 * 	Filter results that are not from sample and/or have distance more than specified in option Ivory.MaxHammingDistance. 
	 *  Reducer selects closest N pairs for each sample foreign-language document.
	 *  
	 * @author ferhanture
	 *
	 */
	public static class MyMapperTopN extends MapReduceBase implements
	Mapper<PairOfInts, IntWritable, IntWritable, PairOfInts> {

		static Path[] localFiles;
		HMapIIW samplesMap = null;
		int maxDist;

		public void configure(JobConf job){
			sLogger.setLevel(Level.INFO);
			maxDist = job.getInt("Ivory.MaxHammingDistance", -1);

			//read doc ids of sample into vectors
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (Exception e) {
				throw new RuntimeException("Error reading doc vectors!");
			}

			if(localFiles != null && localFiles.length > 0){
				samplesMap = new HMapIIW();
				try {
					FSLineReader reader = new FSLineReader(localFiles[0], FileSystem.getLocal(job));
					Text t = new Text();
					while(reader.readLine(t)!=0){
						int docno = Integer.parseInt(t.toString());
						sLogger.info(docno + " --> sample");
						samplesMap.put(docno, 1);
					}
					reader.close();
				} catch (IOException e1) {
				}
				sLogger.info(samplesMap.size()+" sampled");
			}else{
				sLogger.info("samples file not specified in local cache");
			}
		}

		public void map(PairOfInts key, IntWritable value,
				OutputCollector<IntWritable, PairOfInts> output,
				Reporter reporter) throws IOException {

			int leftKey = key.getLeftElement();			//english docno
			int rightKey = key.getRightElement();		//german docno

			sLogger.debug(rightKey);
			if(samplesMap==null || samplesMap.containsKey(rightKey)){
				if(maxDist==-1 || value.get()<=maxDist){
					output.collect(new IntWritable(rightKey), new PairOfInts(value.get(),leftKey));
					
					//symmetric implementation. change when not desired.
//					output.collect(new IntWritable(leftKey), new PairOfInts(value.get(),rightKey));
				}
			}

		}
	}

	public static class MyReducerTopN extends MapReduceBase implements
	Reducer<IntWritable, PairOfInts, IntWritable, PairOfInts> {
		int numResults;
		TreeSet<PairOfInts> list = new TreeSet<PairOfInts>();

		public void configure(JobConf conf){
			numResults = conf.getInt("Ivory.NumResults", -1);
			sLogger.info("numResults");
		}

		public void reduce(IntWritable key, Iterator<PairOfInts> values,
				OutputCollector<IntWritable, PairOfInts> output, Reporter reporter)
		throws IOException {
			list.clear();
			while(values.hasNext()){
				PairOfInts p = values.next();
				list.add(new PairOfInts(p.getLeftElement(),p.getRightElement()));
				reporter.incrCounter(mapoutput.count, 1);
			}
			int cntr = 0;
			while(!list.isEmpty() && cntr<numResults){
				output.collect(key, list.pollFirst());
				cntr++;
			}
		}

	}

	
	public static class MyMapper extends MapReduceBase implements
	Mapper<PairOfInts, IntWritable, PairOfInts, IntWritable> {

		static Path[] localFiles;
		HMapIIW samplesMap = null;
		int maxDist;
		IntWritable outValue = new IntWritable();
		PairOfInts outKey = new PairOfInts();
		
		public void configure(JobConf job){
			sLogger.setLevel(Level.INFO);
			maxDist = job.getInt("Ivory.MaxHammingDistance", -1);

			//read doc ids of sample into vectors
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (Exception e) {
				throw new RuntimeException("Error reading doc vectors!");
			}

			if(localFiles != null && localFiles.length > 0){
				samplesMap = new HMapIIW();
				try {
					FSLineReader reader = new FSLineReader(localFiles[0], FileSystem.getLocal(job));
					Text t = new Text();
					while(reader.readLine(t)!=0){
						int docno = Integer.parseInt(t.toString());
						sLogger.info(docno + " --> sample");
						samplesMap.put(docno, 1);
					}
					reader.close();
				} catch (IOException e1) {
				}
				sLogger.info(samplesMap.size()+" sampled");
			}else{
				sLogger.info("samples file not specified in option SampleDocnosFile");
			}
		}

		public void map(PairOfInts key, IntWritable value,
				OutputCollector<PairOfInts, IntWritable> output,
				Reporter reporter) throws IOException {

			int leftKey = key.getLeftElement();			//english docno
			int rightKey = key.getRightElement();		//german docno

			sLogger.debug(rightKey);
			if(samplesMap==null || samplesMap.containsKey(rightKey)){
				if(maxDist==-1 || value.get()<=maxDist){
					outKey.set(leftKey, rightKey);
					outValue.set(value.get());
					output.collect(outKey, outValue);
				}
			}
		}
	}
	
	// @author: ferhanture
	// I wrote this to be used on a text dataset. needs some fixing.
//	public static class MyReducerAltOutput extends MapReduceBase implements
//	Reducer<IntWritable, PairOfInts, Text, Text> {
//		int numResults;
//		TreeSet<PairOfInts> list = new TreeSet<PairOfInts>();
//		private DocnoMapping mDocMapping;
//
//		public void configure(JobConf conf){
//			numResults = conf.getInt("Ivory.NumResults", -1);
//			sLogger.info("numResults");
//			mDocMapping = new TextDocnoMapping();
//			try {
//				mDocMapping.loadMapping(new Path("/user/fture/doug/docno-mapping.dat"), FileSystem.get(conf));
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//
//		public void reduce(IntWritable key, Iterator<PairOfInts> values,
//				OutputCollector<Text, Text> output, Reporter reporter)
//		throws IOException {
//			list.clear();
//			while(values.hasNext()){
//				PairOfInts p = values.next();
//				list.add(new PairOfInts(p.getLeftElement(),p.getRightElement()));
//				reporter.incrCounter(mapoutput.count, 1);
//			}
//			int cntr = 0;
//			while(!list.isEmpty() && cntr<numResults){
//				PairOfInts nextClosest = list.pollFirst();
//				String keyDocid = mDocMapping.getDocid(key.get());
//				String valueDocid = mDocMapping.getDocid(nextClosest.getRightElement());
//				int dist= nextClosest.getLeftElement();
//				output.collect(new Text(keyDocid), new Text(valueDocid+"\t"+dist));
//				cntr++;
//			}
//		}
//
//	}

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage();
			return -1;
		}
		JobConf job = new JobConf(getConf(), FilterResults.class);
		
		String samplesFile = args[2]; 
		int maxHammingDistance = Integer.parseInt(args[3]);
		job.setInt("Ivory.MaxHammingDistance",maxHammingDistance);
		int numResults = Integer.parseInt(args[4]);
		job.setInt("Ivory.NumResults",numResults);

		job.setJobName("FilterResults_"+maxHammingDistance+"_"+numResults);
		FileSystem fs2 = FileSystem.get(job);

		String inputPath2 = args[0];//job2.get("Ivory.PWSimOutputPath");
		String outputPath2 = args[1];//job2.get("FilteredPWSimFile");

		int numMappers2 = 300;
		int numReducers2 = 1;

		if(fs2.exists(new Path(outputPath2))){
			sLogger.info("FilteredPwsim output already exists! Quitting...");
			return 0;
		}	
		FileInputFormat.setInputPaths(job, new Path(inputPath2));
		FileOutputFormat.setOutputPath(job, new Path(outputPath2));
		FileOutputFormat.setCompressOutput(job, false);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		job.setInt("mapred.map.max.attempts", 10);
		job.setInt("mapred.reduce.max.attempts", 10);
		job.setInt("mapred.task.timeout", 6000000);

		sLogger.info("Running job "+job.getJobName());
		sLogger.info("Input directory: "+inputPath2);
		sLogger.info("Output directory: "+outputPath2);

		sLogger.info("Samples file: "+samplesFile);

		if(!samplesFile.equals("none")){
			DistributedCache.addCacheFile(new URI(samplesFile), job);	//sample doc vectors in file
		}

		if(numResults > 0){
			sLogger.info("Number of results = "+numResults);
			job.setMapperClass(MyMapperTopN.class);
			job.setReducerClass(MyReducerTopN.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(PairOfInts.class);
		}else{
			sLogger.info("Number of results = all");
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(IdentityReducer.class);
			job.setMapOutputKeyClass(PairOfInts.class);
			job.setMapOutputValueClass(IntWritable.class);
		}
		
		job.setNumMapTasks(numMappers2);
		job.setNumReduceTasks(numReducers2);
		job.setInputFormat(SequenceFileInputFormat.class);
		//		job2.setOutputFormat(SequenceFileOutputFormat.class);			

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(), new FilterResults(), args);
		return;
	}

}
