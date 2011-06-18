package ivory.lsh.eval;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfInts;

/**
 * Read in sequence file format and output as text format.
 * 
 * @author ferhanture
 *
 */
@SuppressWarnings("deprecation")
public class OutputResultsAsText extends Configured implements Tool {
	public static final String[] RequiredParameters = {};
	private static final Logger sLogger = Logger.getLogger(OutputResultsAsText.class);

	static enum mapoutput{
		count
	};

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path]");
		return -1;
	}

	public OutputResultsAsText() {
		super();
	}

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
			return -1;
		}
		JobConf job = new JobConf(getConf(), OutputResultsAsText.class);

		job.setJobName("OutputAsText");
		FileSystem fs = FileSystem.get(job);

		String inputPath = args[0];
		String outputPath = args[1];

		int numMappers = 300;
		int numReducers = 1;

		if(fs.exists(new Path(outputPath))){
			sLogger.info("Output already exists! Quitting...");
			return 0;
		}	
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		job.setInt("mapred.map.max.attempts", 10);
		job.setInt("mapred.reduce.max.attempts", 10);
		job.setInt("mapred.task.timeout", 6000000);

		sLogger.info("Running job "+job.getJobName());
		sLogger.info("Input directory: "+inputPath);
		sLogger.info("Output directory: "+outputPath);

		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(IdentityReducer.class);

		job.setMapOutputKeyClass(PairOfInts.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(PairOfInts.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumMapTasks(numMappers);
		job.setNumReduceTasks(numReducers);
		job.setInputFormat(SequenceFileInputFormat.class);

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(), new OutputResultsAsText(), args);
		return;
	}

}
