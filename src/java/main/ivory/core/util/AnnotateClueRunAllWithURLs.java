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
import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import edu.umd.cloud9.io.FSLineReader;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;

public class AnnotateClueRunAllWithURLs extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(AnnotateClueRunAllWithURLs.class);

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

			ClueWarcForwardIndex[] indexes = new ClueWarcForwardIndex[10];

			indexes[0] = new ClueWarcForwardIndex();
			indexes[0].loadIndex(new Path("/shared/ClueWeb09/collection.compressed.block/findex.en.01.dat"),
					     new Path("/shared/ClueWeb09/docno-mapping.dat"), FileSystem.get(conf));

			indexes[1] = new ClueWarcForwardIndex();
			indexes[1].loadIndex(new Path("/shared/ClueWeb09/collection.compressed.block/findex.en.02.dat"),
					     new Path("/shared/ClueWeb09/docno-mapping.dat"), FileSystem.get(conf));

			indexes[2] = new ClueWarcForwardIndex();
			indexes[2].loadIndex(new Path("/shared/ClueWeb09/collection.compressed.block/findex.en.03.dat"),
					     new Path("/shared/ClueWeb09/docno-mapping.dat"), FileSystem.get(conf));

			indexes[3] = new ClueWarcForwardIndex();
			indexes[3].loadIndex(new Path("/shared/ClueWeb09/collection.compressed.block/findex.en.04.dat"),
					     new Path("/shared/ClueWeb09/docno-mapping.dat"), FileSystem.get(conf));

			indexes[4] = new ClueWarcForwardIndex();
			indexes[4].loadIndex(new Path("/shared/ClueWeb09/collection.compressed.block/findex.en.05.dat"),
					     new Path("/shared/ClueWeb09/docno-mapping.dat"), FileSystem.get(conf));

			indexes[5] = new ClueWarcForwardIndex();
			indexes[5].loadIndex(new Path("/shared/ClueWeb09/collection.compressed.block/findex.en.06.dat"),
					     new Path("/shared/ClueWeb09/docno-mapping.dat"), FileSystem.get(conf));

			indexes[6] = new ClueWarcForwardIndex();
			indexes[6].loadIndex(new Path("/shared/ClueWeb09/collection.compressed.block/findex.en.07.dat"),
					     new Path("/shared/ClueWeb09/docno-mapping.dat"), FileSystem.get(conf));

			indexes[7] = new ClueWarcForwardIndex();
			indexes[7].loadIndex(new Path("/shared/ClueWeb09/collection.compressed.block/findex.en.08.dat"),
					     new Path("/shared/ClueWeb09/docno-mapping.dat"), FileSystem.get(conf));

			indexes[8] = new ClueWarcForwardIndex();
			indexes[8].loadIndex(new Path("/shared/ClueWeb09/collection.compressed.block/findex.en.09.dat"),
					     new Path("/shared/ClueWeb09/docno-mapping.dat"), FileSystem.get(conf));

			indexes[9] = new ClueWarcForwardIndex();
			indexes[9].loadIndex(new Path("/shared/ClueWeb09/collection.compressed.block/findex.en.10.dat"),
					     new Path("/shared/ClueWeb09/docno-mapping.dat"), FileSystem.get(conf));

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
				ClueWarcRecord doc = null;

				for (int i = 0; i < 10; i++) {
					doc = indexes[i].getDocument(docid);
					if (doc != null)
						break;
				}
				String url = doc.getHeaderMetadataItem("WARC-Target-URI");
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
	public AnnotateClueRunAllWithURLs() {
	}

	private static int printUsage() {
		System.out.println("usage: [input-file] [output-file]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
			return -1;
		}

		String inputFile = args[0];
		String outputFile = args[1];

		sLogger.info("Tool name: AnnotateClueRunWithURLs");
		sLogger.info(" - input file: " + inputFile);
		sLogger.info(" - output file: " + outputFile);

		JobConf conf = new JobConf(AnnotateClueRunAllWithURLs.class);
		conf.setJobName("AnnotateClueRunWithURLs");

		conf.setSpeculativeExecution(false);

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(MyMapper.class);

		conf.set("InputFile", inputFile);
		conf.set("OutputFile", outputFile);

		JobClient.runJob(conf);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AnnotateClueRunAllWithURLs(), args);
		System.exit(res);
	}
}
