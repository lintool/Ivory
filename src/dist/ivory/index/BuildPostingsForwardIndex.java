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

package ivory.index;

import ivory.data.PostingsList;
import ivory.data.PrefixEncodedTermSet;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.PowerTool;

public class BuildPostingsForwardIndex extends PowerTool {

	private static final Logger sLogger = Logger.getLogger(BuildPostingsForwardIndex.class);

	protected static enum Dictionary {
		Size
	};

	private static class MyMapRunner implements MapRunnable<Text, PostingsList, Text, Text> {

		private String mInputFile;
		private Text outputValue = new Text();

		private static Pattern sPatternAlphanumeric = Pattern.compile("[\\w]+");

		int dfThreshold;

		public void configure(JobConf job) {
			mInputFile = job.get("map.input.file");
			dfThreshold = job.getInt("Ivory.DfThreshold", 1);
		}

		public void run(RecordReader<Text, PostingsList> input, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			Text key = input.createKey();
			PostingsList value = input.createValue();
			int fileNo;

			long pos = input.getPos();
			while (input.next(key, value)) {
				fileNo = Integer.parseInt(mInputFile.substring(mInputFile.lastIndexOf("-") + 1));
				outputValue.set(fileNo + "\t" + pos);

				Matcher m = sPatternAlphanumeric.matcher(key.toString());
				if (!m.matches())
					continue;

				if (value.getDf() > dfThreshold) {
					output.collect(key, outputValue);
					reporter.incrCounter(Dictionary.Size, 1);
				}
				pos = input.getPos();
			}
		}
	}

	public static final long BIG_LONG_NUMBER = 1000000000;

	private static class MyReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		String termsFile;
		String positionsFile;
		int nTerms;
		int window;

		FileSystem fileSys;

		FSDataOutputStream termsOut;
		FSDataOutputStream posOut;

		@Override
		public void close() throws IOException {
			super.close();
			termsOut.close();
			posOut.close();
		}

		public void configure(JobConf job) {
			try {
				fileSys = FileSystem.get(job);
			} catch (Exception e) {
				throw new RuntimeException("error in fileSys");
			}
			termsFile = job.get("Ivory.PrefixEncodedTermsFile");
			positionsFile = job.get("Ivory.PostingsPositionsFile");
			nTerms = job.getInt("Ivory.IndexNumberOfTerms", 0);
			window = job.getInt("Ivory.ForwardIndexWindow", 8);

			sLogger.info("Ivory.PrefixEncodedTermsFile: " + termsFile);
			sLogger.info("Ivory.PostingsPositionsFile: " + positionsFile);
			sLogger.info("Ivory.IndexNumberOfTerms: " + nTerms);
			sLogger.info("Ivory.ForwardIndexWindow: " + window);

			try {
				termsOut = fileSys.create(new Path(termsFile), true);
				posOut = fileSys.create(new Path(positionsFile), true);
				termsOut.writeInt(nTerms);
				termsOut.writeInt(window);
				posOut.writeInt(nTerms);
			} catch (Exception e) {
				throw new RuntimeException("error in creating files");
			}

		}

		int curKeyIndex = 0;
		String lastKey = "";

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {

			String[] s;
			int fileNo;
			long filePos, pos;
			String term = key.toString();

			int prefixLength;

			s = values.next().toString().split("\\s+");

			if (values.hasNext())
				throw new RuntimeException("There shouldn't be more than one value, key=" + key);

			fileNo = Integer.parseInt(s[0]);
			filePos = Long.parseLong(s[1]);
			pos = BIG_LONG_NUMBER * fileNo + filePos;

			if (curKeyIndex % window == 0) {
				byte[] byteArray = term.getBytes();
				termsOut.writeByte((byte) (byteArray.length)); // suffix length
				for (int j = 0; j < byteArray.length; j++)
					termsOut.writeByte(byteArray[j]);
			} else {
				prefixLength = PrefixEncodedTermSet.getPrefix(lastKey, term);
				byte[] suffix = term.substring(prefixLength).getBytes();

				if (prefixLength > Byte.MAX_VALUE || suffix.length > Byte.MAX_VALUE)
					throw new RuntimeException("prefix/suffix length overflow");

				termsOut.writeByte((byte) suffix.length); // suffix length
				termsOut.writeByte((byte) prefixLength); // prefix length
				for (int j = 0; j < suffix.length; j++)
					termsOut.writeByte(suffix[j]);
			}
			lastKey = term;
			curKeyIndex++;
			posOut.writeLong(pos);
		}
	}

	public BuildPostingsForwardIndex(Configuration conf) {
		super(conf);
	}

	public static final String[] RequiredParameters = { "Ivory.IndexPath", "Ivory.NumMapTasks",
			"Ivory.IndexNumberOfTerms", "Ivory.ForwardIndexWindow", "Ivory.DfThreshold" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public int runTool() throws Exception {
		sLogger.info("Building terms to postings forward index...");

		JobConf conf = new JobConf(getConf(), BuildPostingsForwardIndex.class);
		FileSystem fs = FileSystem.get(conf);

		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
		int minSplitSize = conf.getInt("Ivory.MinSplitSize", 0);
		int nTerms = conf.getInt("Ivory.IndexNumberOfTerms", 10000);
		int indexWindow = conf.getInt("Ivory.ForwardIndexWindow", 8);
		int dfThreshold = conf.getInt("Ivory.DfThreshold", 1);

		String indexPath = conf.get("Ivory.IndexPath");
		String collectionName = RetrievalEnvironment.readCollectionName(fs, indexPath);

		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - IndexPath: " + indexPath);
		sLogger.info(" - IndexNumberOfTerms: " + nTerms);
		sLogger.info(" - ForwardIndexWindow: " + indexWindow);
		sLogger.info(" - DfThreshold: " + dfThreshold);

		conf.setJobName("BuildPostingsForwardIndexTable: " + collectionName);

		Path inputPath = new Path(RetrievalEnvironment.getPostingsDirectory(indexPath));
		Path outputPath = new Path(RetrievalEnvironment.getTempDirectory(indexPath));

		fs.delete(outputPath, true);
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);
		conf.setInt("mapred.min.split.size", minSplitSize);

		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.setInt("mapred.map.max.attempts", 10);
		conf.setInt("mapred.reduce.max.attempts", 10);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.set("Ivory.PrefixEncodedTermsFile", RetrievalEnvironment
				.getPostingsIndexTerms(indexPath));
		conf.set("Ivory.PostingsPositionsFile", RetrievalEnvironment
				.getPostingsIndexPositions(indexPath));

		conf.setMapRunnerClass(MyMapRunner.class);
		conf.setReducerClass(MyReducer.class);

		JobClient.runJob(conf);

		fs.delete(outputPath, true);

		RetrievalEnvironment.writePostingsIndexTermCount(FileSystem.get(conf), indexPath, nTerms);

		return 0;
	}

}
