package ivory.sqe.retrieval;

import ivory.core.eval.Qrels;
import ivory.core.eval.RankedListEvaluator;
import ivory.smrf.retrieval.Accumulator;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;


public class RunQueryEngine {

	private static final Logger LOG = Logger.getLogger(RunQueryEngine.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = parseArgs(args);
			
		FileSystem fs = FileSystem.getLocal(conf);
		QueryEngine qe;
		try {
			LOG.info(conf.get(Constants.Language));
			LOG.info(conf.get(Constants.IndexPath));
			LOG.info(conf.get(Constants.Heuristic3));
			LOG.info(conf.get(Constants.LexicalProbThreshold));
			LOG.info(conf.get(Constants.CumulativeProbThreshold));

			LOG.info("Initializing QueryEngine...");
			qe = new QueryEngine(conf, fs);
			LOG.info("Running the queries ...");
			long start = System.currentTimeMillis();
			qe.runQueries(conf);
			long end = System.currentTimeMillis();

			LOG.info("Completed in "+(end - start)+" ms");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		eval(qe, conf);
	}

	static Configuration parseArgs(String[] args) {
		Configuration conf = new Configuration();
		try {
			String mode = args[0];
			conf.set(Constants.QueryType, mode);
			conf.set(Constants.IndexPath, args[1]);
			conf.set(Constants.QueriesPath, args[2]);
			conf.set(Constants.QrelsPath, args[3]);
			conf.set(Constants.Language, args[4]);
			conf.set(Constants.TokenizerData, args[5]);

			if(mode.equals("clir")){
				conf.set(Constants.fVocabPath, args[6]);
				conf.set(Constants.eVocabPath, args[7]);
				conf.set(Constants.f2eProbsPath, args[8]);
				if (args.length >= 10) {
					conf.setFloat(Constants.LexicalProbThreshold, Float.parseFloat(args[9]));
				}
				if (args.length >= 11) {
					conf.setFloat(Constants.CumulativeProbThreshold, Float.parseFloat(args[10]));
				}
			}else if(mode.equals("phrase")){
				conf.set(Constants.SCFGPath, args[6]);
				if (args.length >= 8) {
					conf.set(Constants.Heuristic1, args[7]);
				}
				if (args.length >= 9) {
					conf.setBoolean(Constants.Heuristic2, Boolean.parseBoolean(args[8]));
				}
				if (args.length >= 10) {
					conf.set(Constants.Heuristic3, args[9]);
				}
			}else if(mode.equals("phrase2")){
				conf.set(Constants.fVocabPath, args[6]);
				conf.set(Constants.eVocabPath, args[7]);
				conf.set(Constants.f2eProbsPath, args[8]);
				conf.set(Constants.SCFGPath, args[9]);
				
				if (args.length >= 11) {
					conf.set(Constants.Heuristic1, args[10]);
				}
				if (args.length >= 12) {
					conf.setBoolean(Constants.Heuristic2, Boolean.parseBoolean(args[11]));
				}
				if (args.length >= 13) {
					conf.set(Constants.Heuristic3, args[12]);
				}			
				if (args.length >= 14) {
					conf.setFloat(Constants.LexicalProbThreshold, Float.parseFloat(args[13]));
				}
				if (args.length >= 15) {
					conf.setFloat(Constants.CumulativeProbThreshold, Float.parseFloat(args[14]));
				}
			}
			LOG.info("Running job in mode = "+mode);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("usage: ivory.sqe.retrieval.RunQueryEngine bow [index-path] [queries-file]  [qrels] [src-lang] [tokenizer-model-file]");
			System.out.println("usage: ivory.sqe.retrieval.RunQueryEngine clir [index-path] [queries-file]  [qrels] [src-lang] [tokenizer-model-file] [vocab-f-file] [vocab-e-file] [ttable-f2e-file] (lex-prob-threshold) (cum-prob-threshold)");
			System.out.println("usage: ivory.sqe.retrieval.RunQueryEngine phrase [index-path] [queries-file] [qrels] [src-lang] [tokenizer-model-file] [grammar-file] (H1=yes|no) (H2=yes|no) (H3=sum|max|avg)");
			System.out.println("usage: ivory.sqe.retrieval.RunQueryEngine phrase2 [index-path] [queries-file] [qrels] [src-lang] [tokenizer-model-file] [vocab-f-file] [vocab-e-file] [ttable-f2e-file] [grammar-file] (H3=sum|max|avg) (lex-prob-threshold) (cum-prob-threshold)");
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);	
		}
		return conf;
		
	}

	static void eval(QueryEngine qe, Configuration conf){
		Qrels qrels = new Qrels(conf.get(Constants.QrelsPath));
		DocnoMapping mapping = qe.getDocnoMapping();
		float apSum = 0, p10Sum = 0;
		Map<String, Accumulator[]> results = qe.getResults();
	    for (String qid : results.keySet()) {
	        float ap = (float) RankedListEvaluator.computeAP(results.get(qid), mapping,
	            qrels.getReldocsForQid(qid));

	        float p10 = (float) RankedListEvaluator.computePN(10, results.get(qid), mapping,
	            qrels.getReldocsForQid(qid));
	        LOG.info(qid+":"+results.get(qid).length+","+qrels.getReldocsForQid(qid).size()+" => "+ap+","+p10);
	        apSum += ap;
	        p10Sum += p10;
	    }
	    conf.setFloat("AP", apSum);
	    conf.setFloat("P10", p10Sum);
	    float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / results.size());
	    float P10Avg = (float) RankedListEvaluator.roundTo4SigFigs(p10Sum / results.size());
	    LOG.info("Eval = "+MAP+","+P10Avg+"\nNumber of queries = "+results.size());
	}
}
