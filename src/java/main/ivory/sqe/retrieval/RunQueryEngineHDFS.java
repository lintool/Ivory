package ivory.sqe.retrieval;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;


@SuppressWarnings({ "deprecation" })
public class RunQueryEngineHDFS extends Configured implements Tool  {

	private static final Logger LOG = Logger.getLogger(RunQueryEngineHDFS.class);
	private static enum Time { Query };

	private static class QueryRunner extends NullMapper {
		public void run(JobConf conf, Reporter reporter) throws IOException {
			FileSystem fs = FileSystem.get(conf);
			QueryEngine qe;
			try {
				LOG.info("Initializing QueryEngine...");
				qe = new QueryEngine(conf, fs);
				LOG.info("Running the queries ...");
				long start = System.currentTimeMillis();
				qe.runQueries(conf);
				long end = System.currentTimeMillis();

				reporter.incrCounter(Time.Query, (end - start));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
      String setting = conf.getInt(Constants.KBest, 0) + 
              "-" + (int)(100*conf.getFloat(Constants.MTWeight, 0)) + 
              "-" + (int) (100*conf.getFloat(Constants.BitextWeight, 0)) + 
              "-" + (int) (100*conf.getFloat(Constants.TokenWeight, 0));
			RunQueryEngine.eval(qe, conf, setting);
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = RunQueryEngine.parseArgs(args);
		
		JobConf job = new JobConf(conf, RunQueryEngineHDFS.class);
		job.setJobName(getClass().getSimpleName());

		job.setNumMapTasks(1);
		job.setNumReduceTasks(0);

		job.setInputFormat(NullInputFormat.class);
		job.setOutputFormat(NullOutputFormat.class);
		job.setMapperClass(QueryRunner.class);

		job.set("mapred.child.java.opts", "-Xmx16g");

		JobClient client = new JobClient(job);
		client.submitJob(job);

		LOG.info("runner started!");
		return 0;
	}

	public RunQueryEngineHDFS() {}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new RunQueryEngineHDFS(), args);
	}

}
