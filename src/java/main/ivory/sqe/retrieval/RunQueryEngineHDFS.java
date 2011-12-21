package ivory.sqe.retrieval;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;


public class RunQueryEngineHDFS extends Configured implements Tool  {

	  private static final Logger LOG = Logger.getLogger(RunQueryEngineHDFS.class);
	  private static enum Time { Query };

	  private static class QueryRunner extends NullMapper {
	    public void run(JobConf conf, Reporter reporter) throws IOException {
	      String[] args = conf.get("args").split(";");
	      FileSystem fs = FileSystem.get(conf);
	      QueryEngine qe;
	      try {
	        LOG.info("Initializing QueryEngine...");
	        qe = new QueryEngine(args, fs);
	        LOG.info("Running the queries ...");
	        long start = System.currentTimeMillis();
	        qe.runQueries();
	        long end = System.currentTimeMillis();

	        reporter.incrCounter(Time.Query, (end - start));
	      } catch (Exception e) {
	        throw new RuntimeException(e);
	      }
	    }
	  }

	  public int run(String[] args) throws Exception {
	    if (args.length != 2 && args.length != 6) {
	      System.out.println("usage 1: [index-path] [queries-file] [vocab-f-file] [vocab-e-file] [ttable-f2e-file] [tokenizer-model-file]");
	      System.out.println("usage 2: [index-path] [queries-file]");
	      ToolRunner.printGenericCommandUsage(System.out);
	      return -1;
	    }

	    String argsStr = Joiner.on(";").join(args);

	    JobConf conf = new JobConf(RunQueryEngineHDFS.class);
	    conf.setJobName("RunQueryEngineHDFS");

	    conf.setNumMapTasks(1);
	    conf.setNumReduceTasks(0);

	    conf.setInputFormat(NullInputFormat.class);
	    conf.setOutputFormat(NullOutputFormat.class);
	    conf.setMapperClass(QueryRunner.class);

	    conf.set("args", argsStr);
	    conf.set("mapred.child.java.opts", "-Xmx16g");
	    
//	    if (args.length == 6) {
//	    	conf.set("Ivory.F_Vocab_F2E", args[2]);
//	    	conf.set("Ivory.E_Vocab_F2E", args[3]);
//	    	conf.set("Ivory.TTable_F2E", args[4]);
//	    	conf.set("Ivory.TokenizerModel", args[5]);
//	    }
	    LOG.info("argsStr: " + argsStr);

	    JobClient client = new JobClient(conf);
	    client.submitJob(conf);

	    LOG.info("runner started!");

	    return 0;
	  }

	  public RunQueryEngineHDFS() {}

	  public static void main(String[] args) throws Exception {
	    ToolRunner.run(new RunQueryEngineHDFS(), args);
	  }

}
