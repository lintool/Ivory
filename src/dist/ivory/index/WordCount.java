package ivory.index;

import ivory.util.Tokenizer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.util.MapKI;
import edu.umd.cloud9.util.OHMapKI;

public class WordCount extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(WordCount.class);

	protected static enum Docs {
		TOTAL
	};

	protected static enum Size {
		PROCESSED_BYTES, PROCESSED_TOKENS
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<Writable, Indexable, Text, IntWritable> {

		private static Text sTerm = new Text();
		private static IntWritable sCount = new IntWritable();

		private Tokenizer mTokenizer;

		public void configure(JobConf job) {
			try {
				mTokenizer = (Tokenizer) Class.forName(job.get("Ivory.Tokenizer")).newInstance();
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing tokenizer!");
			}
		}

		public void map(Writable key, Indexable doc, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			reporter.incrCounter(Docs.TOTAL, 1);

			String text = doc.getContent();
			String[] terms = mTokenizer.processContent(text);

			reporter.incrCounter(Size.PROCESSED_BYTES, text.length());
			reporter.incrCounter(Size.PROCESSED_TOKENS, terms.length);

			OHMapKI<String> h = new OHMapKI<String>();
			for (String term : terms) {
				h.increment(term);
			}

			for (MapKI.Entry<String> e : h.entrySet()) {
				sTerm.set(e.getKey());
				sCount.set(e.getValue());
				output.collect(sTerm, sCount);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		private final static IntWritable sTotal = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			sTotal.set(sum);
			output.collect(key, sTotal);
		}
	}

	private WordCount() {
	}

	private static int printUsage() {
		System.out
				.println("usage: [base-path] [output-path] [tokenizer-class] [num-mapper] [num-reducer]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage();
			return -1;
		}

		String collectionPath = args[0];
		String outputPath = args[1];
		String tokenizer = args[2];
		int numMappers = Integer.parseInt(args[3]);
		int numReducers = Integer.parseInt(args[4]);

		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("WordCount");

		sLogger.info("Tool name: WordCount");
		sLogger.info(" - Base path: " + collectionPath);
		sLogger.info(" - Output path: " + outputPath);
		sLogger.info(" - Tokenizer class: " + tokenizer);
		sLogger.info(" - Number of mappers: " + numMappers);
		sLogger.info(" - Number of reducers: " + numReducers);

		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputPath), true);

		FileInputFormat.setInputPaths(conf, new Path(collectionPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.set("Ivory.Tokenizer", tokenizer);
		conf.setNumMapTasks(numMappers);
		conf.setNumReduceTasks(numReducers);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);

		JobClient.runJob(conf);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}
}
