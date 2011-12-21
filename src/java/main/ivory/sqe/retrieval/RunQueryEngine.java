package ivory.sqe.retrieval;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class RunQueryEngine {

	private static final Logger LOG = Logger.getLogger(RunQueryEngine.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (!(args.length == 2 || args.length >= 6)) {
			System.out.println("usage (clir): [index-path] [queries-file] [vocab-f-file] [vocab-e-file] [ttable-f2e-file] [tokenizer-model-file] (prob-threshold)");
			System.out.println("usage (monolingual): [index-path] [queries-file]");
			ToolRunner.printGenericCommandUsage(System.out);
			return;
		}

		FileSystem fs = FileSystem.getLocal(conf);
		QueryEngine qe;
		try {
			LOG.info("Initializing QueryEngine...");
			qe = new QueryEngine(args, fs);
			LOG.info("Running the queries ...");
			long start = System.currentTimeMillis();
			qe.runQueries();
			long end = System.currentTimeMillis();

			LOG.info("Completed in "+(end - start)+" ms");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
