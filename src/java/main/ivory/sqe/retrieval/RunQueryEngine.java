package ivory.sqe.retrieval;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class RunQueryEngine {

	private static final Logger LOG = Logger.getLogger(RunQueryEngine.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (!parseArgs(args, conf)) {
			System.out.println("usage: ivory.sqe.retrieval.RunQueryEngine bow [index-path] [queries-file]");
			System.out.println("usage: ivory.sqe.retrieval.RunQueryEngine clir [index-path] [queries-file] [vocab-f-file] [vocab-e-file] [ttable-f2e-file] [tokenizer-model-file] [src-lang] (prob-threshold)");
			System.out.println("usage: ivory.sqe.retrieval.RunQueryEngine phrase [index-path] [queries-file] [grammar-file] [tokenizer-model-file] [src-lang]");
			ToolRunner.printGenericCommandUsage(System.out);
			return;
		}

		FileSystem fs = FileSystem.getLocal(conf);
		QueryEngine qe;
		try {
			LOG.info("Initializing QueryEngine...");
			qe = new QueryEngine(conf, fs);
			LOG.info("Running the queries ...");
			long start = System.currentTimeMillis();
			qe.runQueries();
			long end = System.currentTimeMillis();

			LOG.info("Completed in "+(end - start)+" ms");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static boolean parseArgs(String[] args, Configuration conf) {
		try {
			String mode = args[0];
			conf.set(Constants.QueryType, args[0]);
			conf.set(Constants.IndexPath, args[1]);
			conf.set(Constants.QueriesPath, args[2]);
			if(mode.equals("clir")){
				conf.set(Constants.fVocabPath, args[3]);
				conf.set(Constants.eVocabPath, args[4]);
				conf.set(Constants.f2eProbsPath, args[5]);
				conf.set(Constants.TokenizerModelPath, args[6]);
				conf.set(Constants.SourceLanguageCode, args[7]);
				if(args.length == 9){
					conf.setFloat(Constants.ProbThreshold, Float.parseFloat(args[8]));
				}
			}else if(mode.equals("phrase")){
				conf.set(Constants.SCFGPath, args[3]);
				conf.set(Constants.TokenizerModelPath, args[4]);
				conf.set(Constants.SourceLanguageCode, args[5]);
			}
			LOG.info("Running job in mode = "+mode);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
		
	}

}
