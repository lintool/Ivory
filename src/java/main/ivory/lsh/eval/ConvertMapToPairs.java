package ivory.lsh.eval;

import ivory.core.RetrievalEnvironment;

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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.MapIF.Entry;

/**
 *
 * Convert the format of the PCP algorithm's output. 
 * 
 * @author ferhanture
 *
 */
@SuppressWarnings("deprecation")
public class ConvertMapToPairs extends PowerTool {
	public static final String[] RequiredParameters = {};
	private static final Logger sLogger = Logger.getLogger(ConvertMapToPairs.class);

	static enum mapoutput{
		count
	};

	public ConvertMapToPairs(Configuration conf) {
		super(conf);
	}
	
	/**
	 * 	Input is keyed by german docno, and the value is a map from similar english docnos to similarity weights.
	 *  Output is keyed by the pair of docnos: (english docno, german docno), and the value is the sim weight
	 *   
	 *  <docno,[(docno,score),...]> --> <(docno,docno),similarity-weight> 
	 *  
	 * @author ferhanture
	 *
	 */
	public static class MyMapper extends MapReduceBase implements
	Mapper<IntWritable, HMapIFW, PairOfInts, IntWritable> {

		public void configure(JobConf job){
			sLogger.setLevel(Level.INFO);
		}
		PairOfInts outKey = new PairOfInts();
		IntWritable outVal = new IntWritable();

		public void map(IntWritable docno, HMapIFW map, OutputCollector<PairOfInts, IntWritable> output,
				Reporter reporter) throws IOException {
			reporter.incrCounter(mapoutput.count, map.size());
			for(Entry e : map.entrySet()){
				outKey.set(e.getKey(), docno.get());
				outVal.set((int) e.getValue());			// the similarity weight is casted to an integer
				output.collect(outKey, outVal);
			}
		}
	}

	@Override
	public String[] getRequiredParameters() {
		return RequiredParameters ;
	}

	@Override
	public int runTool() throws Exception {
		Configuration conf = getConf();

		JobConf job2 = new JobConf(conf,ConvertMapToPairs.class);
		job2.setJobName("ConvertMap2Pairs");
		
		FileSystem fs = FileSystem.get(job2);
		
		String indexPath = getConf().get("Ivory.IndexPath");
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
		int blockSize = getConf().getInt("Ivory.BlockSize", 0);		
		int numDocs = env.readCollectionDocumentCount();
		int numBlocks = numDocs / blockSize + 1;

		String inputPath = null;
		for (int i = 0; i < numBlocks; i++) {
			inputPath = conf.get("Ivory.PCPOutputPath")+"/block"+i;			//one block of output of PCP algorithm
			FileInputFormat.addInputPath(job2, new Path(inputPath));
		}
		
		String outputPath = conf.get("Ivory.PWSimOutputPath");	//pairs from pcp -- use other parameters like term length
		if(fs.exists(new Path(outputPath))){
			sLogger.info("PCP-pairs output already exists! Quitting...");
			return 0;
		}
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job2, false);

		sLogger.info("Input path="+inputPath);
		sLogger.info("Output path="+outputPath);
		
		int numMappers2 = 100;
		int numReducers2 = 0;

		job2.set("mapred.child.java.opts", "-Xmx2048m");
		job2.setInt("mapred.map.max.attempts", 10);
		job2.setInt("mapred.reduce.max.attempts", 10);
		job2.setInt("mapred.task.timeout", 6000000);

		job2.setNumMapTasks(numMappers2);
		job2.setNumReduceTasks(numReducers2);
		job2.setInputFormat(SequenceFileInputFormat.class);
		job2.setMapOutputKeyClass(PairOfInts.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(PairOfInts.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setMapperClass(MyMapper.class);
		job2.setOutputFormat(SequenceFileOutputFormat.class);			

		sLogger.info("Running job "+job2.getJobName());

		JobClient.runJob(job2);
		
		return 0;
	}

}
