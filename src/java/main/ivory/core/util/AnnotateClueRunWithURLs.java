package ivory.core.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.clue.ClueWarcForwardIndex;
import edu.umd.cloud9.io.FSLineReader;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;

public class AnnotateClueRunWithURLs extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(AnnotateClueRunWithURLs.class);

	static {
		Logger.getLogger(edu.umd.cloud9.collection.clue.ClueWarcForwardIndex.class).setLevel(
				Level.WARN);
	}

	private static enum MyCounter {
		Count, Time
	};

	private static class MyMapper extends NullMapper {
		public void run(JobConf conf, Reporter reporter) throws IOException {
			String inputFile = conf.get("InputFile");
			String outputFile = conf.get("OutputFile");
			String findexFile = conf.get("ForwardIndexFile");
			String docnoMapping = conf.get("DocnoMappingFile");

			ClueWarcForwardIndex findex = new ClueWarcForwardIndex();
			findex.loadIndex(findexFile, docnoMapping);

			FileSystem fs = FileSystem.get(conf);

			sLogger.info("reading " + inputFile);

			FSLineReader reader = new FSLineReader(new Path(inputFile), fs);
			FSDataOutputStream writer = fs.create(new Path(outputFile), true);

			Text line = new Text();
			while (reader.readLine(line) > 0) {
				String[] arr = line.toString().split("\\s+");

				String docid = arr[2];
				int rank = Integer.parseInt(arr[3]);

				long start = System.currentTimeMillis();
				String url = findex.getDocument(docid).getHeaderMetadataItem("WARC-Target-URI");
				long duration = System.currentTimeMillis() - start;

				reporter.incrCounter(MyCounter.Count, 1);
				reporter.incrCounter(MyCounter.Time, duration);

				if (rank == 1 || rank % 100 == 0)
					sLogger.info(line + " " + url + " (" + duration + "ms)");
				writer.write(new String(line + " " + url + "\n").getBytes());
			}

			reader.close();
			writer.close();

		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public AnnotateClueRunWithURLs() {
	}

	private static int printUsage() {
		System.out.println("usage: [input-file] [output-file] [forward-index] [docno-mapping]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			printUsage();
			return -1;
		}

		String inputFile = args[0];
		String outputFile = args[1];
		String findexFile = args[2];
		String docnoMapping = args[3];

		sLogger.info("Tool name: AnnotateClueRunWithURLs");
		sLogger.info(" - input file: " + inputFile);
		sLogger.info(" - output file: " + outputFile);
		sLogger.info(" - forward index: " + findexFile);
		sLogger.info(" - docno mapping file: " + docnoMapping);

		long r = System.currentTimeMillis();
		String outputPath = "/tmp/" + r;

		JobConf conf = new JobConf(AnnotateClueRunWithURLs.class);
		conf.setJobName("AnnotateClueRunWithURLs");

		conf.setSpeculativeExecution(false);

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(MyMapper.class);

		conf.set("InputFile", inputFile);
		conf.set("OutputFile", outputFile);
		conf.set("ForwardIndexFile", findexFile);
		conf.set("DocnoMappingFile", docnoMapping);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		// clean up
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AnnotateClueRunWithURLs(), args);
		System.exit(res);
	}
}
