package ivory.util;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.PairOfStringInt;
import edu.umd.cloud9.util.SequenceFileUtils;

@SuppressWarnings("deprecation")
public class CombineRetrievalResults  extends Configured implements Tool{
	private static final Logger sLogger = Logger.getLogger(CombineRetrievalResults.class);

	public CombineRetrievalResults(){
		
	}
	
	private static class MyMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
	
		public void map(LongWritable key, Text val,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			Text t = new Text("");
			
			output.collect(val, t);
		}
	}
	
	private static int printUsage() {
		System.out.println("usage: [input] [output-dir] [number-of-mappers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 3 && args.length!=4) {
			printUsage();
			return -1;
		}
		String inputPath = args[0];
		String outputPath = args[1];
		int N = Integer.parseInt(args[2]);
		
		JobConf job = new JobConf(CombineRetrievalResults.class);
		job.setJobName("CombineRetrievalResults");
			
		int numMappers = N;
		int numReducers = 1;

		if (!FileSystem.get(job).exists(new Path(inputPath))) {
			throw new RuntimeException("Error, input path does not exist!");
		}
		
		if (FileSystem.get(job).exists(new Path(outputPath))) {
			sLogger.info("Output path already exists!");
			return 0;
		}
		
		FileSystem.get(job).delete(new Path(outputPath), true);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		job.setInt("mapred.map.max.attempts", 100);
		job.setInt("mapred.reduce.max.attempts", 100);
		job.setInt("mapred.task.timeout", 600000000);
		if(args.length==4){
			job.set("mapred.job.tracker", "local");
			job.set("fs.default.name", "file:///");
		}
		sLogger.setLevel(Level.INFO);
		
		sLogger.info("Running job "+job.getJobName());
		sLogger.info("Input directory: "+inputPath);
		sLogger.info("Output directory: "+outputPath);
		sLogger.info("Number of mappers: "+N);
			
		job.setNumMapTasks(numMappers);
		job.setNumReduceTasks(numReducers);
//		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(IdentityReducer.class);
//		job.setOutputFormat(SequenceFileOutputFormat.class);

		JobClient.runJob(job); 		

		return 0;
	}

	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(), new CombineRetrievalResults(), args);
		return;
	}
}
