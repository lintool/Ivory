/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.preprocess;

import ivory.data.LazyTermDocVector;
import ivory.data.TermDocVector;
import ivory.tokenize.DocumentProcessingUtils;
import ivory.tokenize.Tokenizer;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.io.FSLineReader;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.array.ArrayListOfInts;
import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.MapII;

@SuppressWarnings("deprecation")
public class BuildTermDocVectors extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(BuildTermDocVectors.class);
	static {
		sLogger.setLevel(Level.WARN);
	}

	protected static enum Docs {
		Skipped, Total, Empty, Exception, Started, Created, Read
	}

	protected static enum MapTime {
		Spilling, Parsing
	}

	protected static enum DocLengths {
		Count, SumOfDocLengths, Files
	}

	private static class MyMapper extends MapReduceBase implements
			Mapper<Writable, Indexable, IntWritable, TermDocVector> {

		// key and value
		private static IntWritable keyOut = new IntWritable();

		// the tokenizer
		private Tokenizer mTokenizer;

		// keep reference to OutputCollector and Reporter to use in close()
		private Reporter mReporter;

		// need configuration to get FileSystem handle in close()
		private JobConf mJobConf;

		// keep track of index path to know where to write doclengths
		private String indexPath;

		// for mapping docids to docnos
		private DocnoMapping mDocMapping;

		// current docno
		private int mDocno;

		// holds document lengths
		private HMapII mDocLengths;

		private String mTaskId;

		public void configure(JobConf job) {
			mDocLengths = new HMapII();

			mTaskId = job.get("mapred.task.id");
			indexPath = job.get("Ivory.IndexPath");
			mJobConf = job;

			Path[] localFiles;
			try {
				localFiles = DistributedCache.getLocalCacheFiles(mJobConf);
			} catch (IOException e) {
				throw new RuntimeException("Local cache files not read properly.");
			}

			// initialize the tokenizer
			try {
				mTokenizer = (Tokenizer) Class.forName(job.get("Ivory.Tokenizer")).newInstance();
				mTokenizer.configure(job);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing Ivory.Tokenizer!");
			}

			// load the docid to docno mappings
			try {
				mDocMapping = (DocnoMapping) Class.forName(job.get("Ivory.DocnoMappingClass"))
						.newInstance();

				// Detect if we're in standalone mode; if so, we can't us the
				// DistributedCache because it does not (currently) work in
				// standalone mode...
				if (job.get("mapred.job.tracker").equals("local")) {
					FileSystem fs = FileSystem.get(job);
					String indexPath = job.get("Ivory.IndexPath");
					RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
					mDocMapping.loadMapping(env.getDocnoMappingData(), fs);
				} else {
					mDocMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing DocnoMapping!");
			}
		}

		public void map(Writable key, Indexable doc,
				OutputCollector<IntWritable, TermDocVector> output, Reporter reporter)
				throws IOException {

			mReporter = reporter;
			mDocno = mDocMapping.getDocno(doc.getDocid());

			// if invalid docno, skip doc; remember docnos start at one
			if (mDocno <= 0) {
				reporter.incrCounter(Docs.Skipped, 1);
				return;
			}
			//if(mDocno > 50220423 + 51577077 + 50547493 /2) return;
			long startTime = System.currentTimeMillis();
			
			Map<String, ArrayListOfInts> termPositionsMap = DocumentProcessingUtils
					.getTermPositionsMap(doc, mTokenizer);
			reporter.incrCounter(MapTime.Parsing, System.currentTimeMillis() - startTime);
			if (termPositionsMap.size() == 0) {
				reporter.incrCounter(Docs.Empty, 1);
			}
			sLogger.info ("in BuildTermDocVectors map, created term positions map: " + termPositionsMap);
			TermDocVector docVector = new LazyTermDocVector(termPositionsMap);
			sLogger.info ("in BuildTermDocVectors map, created term doc vector:" + docVector);
			startTime = System.currentTimeMillis();
			keyOut.set(mDocno);
			reporter.incrCounter(Docs.Created, 1);
			output.collect(keyOut, docVector);
			reporter.incrCounter(MapTime.Spilling, System.currentTimeMillis() - startTime);
			reporter.incrCounter(Docs.Total, 1);
			int dl = DocumentProcessingUtils.getDocLengthFromPositionsMap(termPositionsMap);
			// record the document length
			sLogger.info ("in BuildTermDocVectors map, outputting mDocno: " + mDocno + ", dl: " + dl);
			mDocLengths.put(mDocno, dl);
		}

		public void close() throws IOException {
			// Now we want to write out the doclengths as "side data" onto HDFS.
			// Since speculative execution is on, we'll append the task id to
			// the filename to guarantee uniqueness. However, this means that
			// the may be multiple files with the same doclength information,
			// which we have the handle when we go to write out the binary
			// encoding of the data.

			if (mDocLengths.size() == 0) {
				throw new RuntimeException("Error: DocLength table empty!");
			}
			long bytesCnt = 0;
			FileSystem fs = FileSystem.get(mJobConf);
			// use the last processed docno as the file name + task id
			Path path = new Path(indexPath + "/doclengths/" + mDocno + "." + mTaskId);
			FSDataOutputStream out = fs.create(path, false);

			// iterate through the docs and write out doclengths
			long dlSum = 0;
			int cnt = 0;
			for (MapII.Entry e : mDocLengths.entrySet()) {
				String s = e.getKey() + "\t" + e.getValue() + "\n";
				out.write(s.getBytes());
				bytesCnt += s.getBytes().length;
				cnt++;
				dlSum += e.getValue();
			}
			out.close();

			// We want to check if the file has actually been written
			// successfully...
			sLogger.info("Expected length of doclengths file: " + bytesCnt);

			long bytesActual = fs.listStatus(path)[0].getLen();
			sLogger.info("Actual length of doclengths file: " + bytesActual);

			if (bytesCnt == 0)
				throw new RuntimeException("Error: zero bytesCnt at " + path);
			else if (bytesActual == 0)
				throw new RuntimeException("Error: zero bytesActual at " + path);
			else if (bytesCnt != bytesActual)
				throw new RuntimeException("Error writing Doclengths file: " + bytesCnt + " "
						+ bytesActual + " " + path);

			mReporter.incrCounter(DocLengths.Count, cnt);
			// sum of the document lengths, should match sum of tfs
			mReporter.incrCounter(DocLengths.SumOfDocLengths, dlSum);
		}
	}

	private static class DocLengthDataWriterMapper extends NullMapper {

		int collectionDocCount = -1;

		public void run(JobConf conf, Reporter reporter) throws IOException {
			collectionDocCount = conf.getInt("Ivory.CollectionDocumentCount", -1);
			String inputPath = conf.get("InputPath");
			String dataFile = conf.get("DocLengthDataFile");

			int docnoOffset = conf.getInt("Ivory.DocnoOffset", 0);

			Path p = new Path(inputPath);

			sLogger.info("InputPath: " + inputPath);
			sLogger.info("DocLengthDataFile: " + dataFile);
			sLogger.info("DocnoOffset: " + docnoOffset);

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fileStats = fs.listStatus(p);

			// initial array to hold the doc lengths
			int[] doclengths = new int[collectionDocCount + 1];

			// largest docno
			int maxDocno = 0;

			// smallest docno
			int minDocno = Integer.MAX_VALUE;

			int nFiles = fileStats.length;
			for (int i = 0; i < nFiles; i++) {
				// skip log files
				if (fileStats[i].getPath().getName().startsWith("_"))
					continue;

				FSLineReader reader = new FSLineReader(fileStats[i].getPath(), fs);

				Text line = new Text();
				while (reader.readLine(line) > 0) {
					String[] arr = line.toString().split("\\t+", 2);

					int docno = Integer.parseInt(arr[0]);
					int len = Integer.parseInt(arr[1]);

					// Note that because of speculative execution there may be
					// multiple copies of doclength data. Therefore, we can't
					// just count number of doclengths read. Instead, keep track
					// of largest docno encountered.

					if (docno < docnoOffset) {
						throw new RuntimeException("Error: docno " + docno + " < docnoOffset "
								+ docnoOffset + "!");
					}

					doclengths[docno - docnoOffset] = len;

					if (docno > maxDocno)
						maxDocno = docno;

					if (docno < minDocno)
						minDocno = docno;
				}
				reader.close();
				reporter.incrCounter(DocLengths.Files, 1);
			}

			sLogger.info("min docno: " + minDocno);
			sLogger.info("max docno: " + maxDocno);

			// write out the doc length data into a single file
			FSDataOutputStream out = fs.create(new Path(dataFile), true);

			out.writeInt(docnoOffset);

			// first, write out the collection size
			out.writeInt(maxDocno - docnoOffset);

			// write out length of each document (docnos are sequentially
			// ordered starting from one, so no need to explicitly keep track)
			int n = 0;
			for (int i = 1; i <= maxDocno - docnoOffset; i++) {
				out.writeInt(doclengths[i]);
				n++;
				reporter.incrCounter(DocLengths.Count, 1);
				reporter.incrCounter(DocLengths.SumOfDocLengths, doclengths[i]);
			}
			sLogger.info(n + " doc lengths written");

			out.close();
		}
	}

	public static final String[] RequiredParameters = { "Ivory.NumMapTasks",
			"Ivory.CollectionName", "Ivory.CollectionPath", "Ivory.IndexPath", "Ivory.InputFormat",
			"Ivory.Tokenizer", "Ivory.DocnoMappingClass", };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildTermDocVectors(Configuration conf) {
		super(conf);
	}

	@SuppressWarnings("unchecked")
	public int runTool() throws Exception {
		// create a new JobConf, inheriting from the configuration of this
		// PowerTool
		JobConf conf = new JobConf(getConf(), BuildTermDocVectors.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = conf.get("Ivory.IndexPath");
		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);

		String collectionName = conf.get("Ivory.CollectionName");
		String collectionPath = conf.get("Ivory.CollectionPath");
		String inputFormat = conf.get("Ivory.InputFormat");
		String tokenizer = conf.get("Ivory.Tokenizer");
		String mappingClass = conf.get("Ivory.DocnoMappingClass");

		sLogger.info("PowerTool: BuildTermDocVectors");
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - CollectionPath: " + collectionPath);
		sLogger.info(" - InputputFormat: " + inputFormat);
		sLogger.info(" - Tokenizer: " + tokenizer);
		sLogger.info(" - DocnoMappingClass: " + mappingClass);
		sLogger.info(" - NumMapTasks: " + mapTasks);
		sLogger.info(" - NumReduceTasks: " + 0);

		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
		Path mappingFile = env.getDocnoMappingData();

		if (!fs.exists(mappingFile)) {
			sLogger.error("Error, docno mapping data file " + mappingFile
					+ "doesn't exist!");
			return 0;
		}

		DistributedCache.addCacheFile(mappingFile.toUri(), conf);

		conf.setJobName("BuildTermDocVectors:" + collectionName);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		if (collectionPath.indexOf(",") == -1) {
			FileInputFormat.setInputPaths(conf, new Path(collectionPath));
			sLogger.info("Adding input path " + collectionPath);
		} else {
			String[] paths = collectionPath.split(",");
			for (String p : paths) {
				FileInputFormat.addInputPath(conf, new Path(p));
				sLogger.info("Adding input path " + p);
			}
		}

		Path outputPath = new Path(env.getTermDocVectorsDirectory());
		if (fs.exists(outputPath)) {
			sLogger.info("TermDocVectors already exist: Skipping!");
		} else {
			env.writeCollectionName(collectionName);
			env.writeCollectionPath(collectionPath);
			env.writeInputFormat(inputFormat);
			env.writeDocnoMappingClass(mappingClass);
			env.writeTokenizerClass(tokenizer);

			conf.set("mapred.child.java.opts", "-Xmx2048m");
			conf.setInt("mapred.task.timeout", 60000000);

			FileOutputFormat.setOutputPath(conf, outputPath);

			conf.setInputFormat((Class<? extends InputFormat>) Class.forName(inputFormat));
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputCompressionType(conf,
					SequenceFile.CompressionType.RECORD);

			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(LazyTermDocVector.class);
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(LazyTermDocVector.class);

			conf.setMapperClass(MyMapper.class);

			long startTime = System.currentTimeMillis();
			RunningJob job = JobClient.runJob(conf);
			sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds");

			Counters counters = job.getCounters();

			// write out number of postings
			int collectionDocCount = (int) counters.findCounter(Docs.Total).getCounter();
			env.writeCollectionDocumentCount(collectionDocCount);
		}

		if (fs.exists(env.getDoclengthsData())) {
			sLogger.info("DocLength data exists: Skipping!");
			return 0;
		}

		int collectionDocCount = env.readCollectionDocumentCount();
		long startTime = System.currentTimeMillis();
		writeDoclengthsData(collectionDocCount);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		return 0;
	}

	private void writeDoclengthsData(int collectionDocCount) throws IOException {
		JobConf conf = new JobConf(getConf(), GetTermCount.class);

		String indexPath = conf.get("Ivory.IndexPath");
		String collectionName = conf.get("Ivory.CollectionName");
		int docnoOffset = conf.getInt("Ivory.DocnoOffset", 0);

		FileSystem fs = FileSystem.get(conf);
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		Path dlFile = env.getDoclengthsData();
		Path inputPath = env.getDoclengthsDirectory();

		sLogger.info("Writing doc length data to " + dlFile + "...");

		conf.setJobName("DocLengthTable:" + collectionName);

		conf.setInt("Ivory.CollectionDocumentCount", collectionDocCount);
		conf.set("InputPath", inputPath.toString());
		conf.set("DocLengthDataFile", dlFile.toString());
		conf.set("mapred.child.java.opts", "-Xmx4096m");

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);
		conf.setSpeculativeExecution(false);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(DocLengthDataWriterMapper.class);

		RunningJob job = JobClient.runJob(conf);

		env.writeDocnoOffset(docnoOffset);
		Counters counters = job.getCounters();

		long collectionSumOfDocLengths = (long) counters.findCounter(DocLengths.SumOfDocLengths)
				.getCounter();
		env.writeCollectionAverageDocumentLength((float) collectionSumOfDocLengths / collectionDocCount);
	}

}
