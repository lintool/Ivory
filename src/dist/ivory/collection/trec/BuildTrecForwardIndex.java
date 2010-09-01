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

package ivory.collection.trec;

import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.trec.DemoCountTrecDocuments;
import edu.umd.cloud9.collection.trec.TrecDocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;
import edu.umd.cloud9.util.FSLineReader;
import edu.umd.cloud9.util.PowerTool;

/**
 * <p>
 * Tool for building a document forward index for TREC collections.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class BuildTrecForwardIndex extends PowerTool {

	private static final Logger sLogger = Logger.getLogger(BuildTrecForwardIndex.class);

	private static enum Count {
		DOCS
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, TrecDocument, IntWritable, Text> {

		private final static IntWritable sInt = new IntWritable(1);
		private final static Text sText = new Text();
		private DocnoMapping mDocMapping;

		public void configure(JobConf job) {
			// load the docid to docno mappings
			try {
				mDocMapping = new TrecDocnoMapping();

				// Detect if we're in standalone mode; if so, we can't us the
				// DistributedCache because it does not (currently) work in
				// standalone mode...
				if (job.get("mapred.job.tracker").equals("local")) {
					FileSystem fs = FileSystem.get(job);
					String indexPath = job.get("Ivory.IndexPath");
					String mappingFile = RetrievalEnvironment.getDocnoMappingFile(indexPath);
					mDocMapping.loadMapping(new Path(mappingFile), fs);
				} else {
					Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
					mDocMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing DocnoMapping!");
			}
		}

		public void map(LongWritable key, TrecDocument doc,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			reporter.incrCounter(Count.DOCS, 1);

			int len = doc.getContent().getBytes().length;
			sInt.set(mDocMapping.getDocno(doc.getDocid()));
			sText.set(key + "\t" + len);
			output.collect(sInt, sText);
		}
	}

	public static final String[] RequiredParameters = { "Ivory.IndexPath" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildTrecForwardIndex(Configuration conf) {
		super(conf);
	}

	/**
	 * Runs this tool.
	 */
	public int runTool() throws Exception {
		JobConf conf = new JobConf(getConf(), DemoCountTrecDocuments.class);
		FileSystem fs = FileSystem.get(getConf());

		String indexPath = conf.get("Ivory.IndexPath");
		String collectionPath = RetrievalEnvironment.readCollectionPath(fs, indexPath);
		String outputPath = RetrievalEnvironment.getDocumentForwardIndexDirectory(indexPath);
		String mappingFile = RetrievalEnvironment.getDocnoMappingFile(indexPath);

		sLogger.info("index path: " + indexPath);
		sLogger.info("output path: " + outputPath);
		sLogger.info("collection path: " + collectionPath);
		sLogger.info("mapping file: " + mappingFile);

		conf.setJobName("BuildTrecForwardIndex");

		conf.setNumReduceTasks(1);

		DistributedCache.addCacheFile(new URI(mappingFile), conf);

		FileInputFormat.setInputPaths(conf, new Path(collectionPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(TrecDocumentInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		RetrievalEnvironment.writeDocumentForwardIndexClass(fs, indexPath,
				"ivory.collection.trec.TrecForwardIndex");

		writeData(indexPath);
		return 0;
	}

	private void writeData(String indexPath) throws IOException {
		String outputFile = RetrievalEnvironment.getDocumentForwardIndex(indexPath);
		String inputFile = RetrievalEnvironment.getDocumentForwardIndexDirectory(indexPath)
				+ "part-00000";

		FileSystem fs = FileSystem.get(getConf());

		int numDocs = RetrievalEnvironment.readCollectionDocumentCount(fs, indexPath);
		sLogger.info("Writing doc offset data to " + outputFile);
		FSLineReader reader = new FSLineReader(inputFile, fs);

		FSDataOutputStream writer = fs.create(new Path(outputFile), true);

		writer.writeInt(numDocs);

		int cnt = 0;
		Text line = new Text();
		while (reader.readLine(line) > 0) {
			String[] arr = line.toString().split("\\t");
			long offset = Long.parseLong(arr[1]);
			int len = Integer.parseInt(arr[2]);

			// sLogger.info(arr[0] + " " + offset + " " + len);
			writer.writeLong(offset);
			writer.writeInt(len);

			cnt++;
			if (cnt % 100000 == 0) {
				sLogger.info(cnt + " docs");
			}
		}
		reader.close();
		writer.close();
		sLogger.info(cnt + " docs total. Done!");

		if (numDocs != cnt) {
			throw new RuntimeException("Unexpected number of documents in building forward index!");
		}
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new BuildTrecForwardIndex(conf), args);
		System.exit(res);
	}
}
