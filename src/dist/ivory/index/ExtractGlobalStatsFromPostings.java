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
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.FSProperty;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.io.PairOfIntLong;

import ivory.data.PrefixEncodedGlobalStats;

public class ExtractGlobalStatsFromPostings extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(ExtractGlobalStatsFromPostings.class);

	private static class MyMapper extends MapReduceBase implements
	Mapper<Text, PostingsList, Text, PairOfIntLong> {

		private int mDfThreshold;
		private static Pattern sPatternAlphanumeric = Pattern.compile("[\\w]+");

		public void configure(JobConf job) {
			mDfThreshold = job.getInt("Ivory.DfThreshold", 0);
		}

		private static final PairOfIntLong stats = new PairOfIntLong();

		public void map(Text key, PostingsList p, OutputCollector<Text, PairOfIntLong> output,
				Reporter reporter) throws IOException {
			Matcher m = sPatternAlphanumeric.matcher(key.toString());
			if (!m.matches())
				return;
			if (p.getDf() > mDfThreshold) {
				stats.set(p.getDf(), p.getCf());
				output.collect(key, stats);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
	Reducer<Text, PairOfIntLong, Text, Text> {

		String termsFile;
		String dfStatsFile;
		String cfStatsFile;
		int nTerms;
		int window;

		FileSystem fileSys;

		FSDataOutputStream termsOut;
		FSDataOutputStream dfStatsOut;
		FSDataOutputStream cfStatsOut;

		public void close() throws IOException {
			super.close();
			termsOut.close();
			dfStatsOut.close();
			cfStatsOut.close();
		}

		public void configure(JobConf job) {
			try {
				fileSys = FileSystem.get(job);
			} catch (Exception e) {
				throw new RuntimeException("error in fileSys");
			}
			termsFile = job.get("Ivory.PrefixEncodedTermsFile");
			dfStatsFile = job.get("Ivory.DFStatsFile");
			cfStatsFile = job.get("Ivory.CFStatsFile");

			nTerms = job.getInt("Ivory.IndexNumberOfTerms", 0);
			window = job.getInt("Ivory.ForwardIndexWindow", 8);

			sLogger.info("Ivory.PrefixEncodedTermsFile: " + termsFile);
			sLogger.info("Ivory.DFStatsFile: " + dfStatsFile);
			sLogger.info("Ivory.CFStatsFile: " + cfStatsFile);
			sLogger.info("Ivory.IndexNumberOfTerms: " + nTerms);
			sLogger.info("Ivory.ForwardIndexWindow: " + window);

			try {
				termsOut = fileSys.create(new Path(termsFile), true);
				dfStatsOut = fileSys.create(new Path(dfStatsFile), true);
				cfStatsOut = fileSys.create(new Path(cfStatsFile), true);
				termsOut.writeInt(nTerms);
				termsOut.writeInt(window);
				dfStatsOut.writeInt(nTerms);
				cfStatsOut.writeInt(nTerms);
			} catch (Exception e) {
				throw new RuntimeException("error in creating files");
			}

		}

		int curKeyIndex = 0;
		String lastKey = "";

		public void reduce(Text key, Iterator<PairOfIntLong> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			String term = key.toString();

			int prefixLength;

			int df = 0;
			long cf = 0;

			while (values.hasNext()) {
				PairOfIntLong p = values.next();
				df += p.getLeftElement();
				cf += p.getRightElement();
			}

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

			dfStatsOut.writeInt(df);
			cfStatsOut.writeLong(cf);
		}
	}

	public static final String[] RequiredParameters = { "Ivory.IndexPath", "Ivory.GlobalStatsPath",
		"Ivory.CollectionName", "Ivory.NumMapTasks", "Ivory.TmpPath",
		"Ivory.IndexNumberOfTerms", "Ivory.ForwardIndexWindow", "Ivory.PrefixEncodedTermsFile",
		"Ivory.DFStatsFile", "Ivory.CFStatsFile", };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public ExtractGlobalStatsFromPostings(Configuration conf) {
		super(conf);
	}

	public int runTool() throws Exception {
		sLogger.info("Extracting global stats from all postings ...");

		JobConf conf = new JobConf(getConf(), ExtractGlobalStatsFromPostings.class);
		FileSystem fs = FileSystem.get(conf);

		int mapTasks = conf.getInt("Ivory.NumMapTasks", 200);
		int minSplitSize = conf.getInt("Ivory.MinSplitSize", 0);

		String indexPath = conf.get("Ivory.IndexPath");
		String collectionName = conf.get("Ivory.CollectionName");

		sLogger.info("Characteristics of the collection:");
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - IndexPath: " + indexPath);

		Path outputPath = new Path(conf.get("Ivory.TmpPath"));

		fs.delete(outputPath, true);

		conf.setJobName("ExtractGlobalStatsFromPostings:" + collectionName);

		FileInputFormat.addInputPaths(conf, indexPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);
		conf.setInt("mapred.min.split.size", minSplitSize);

		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.setInt("mapred.map.max.attempts", 10);
		conf.setInt("mapred.reduce.max.attempts", 10);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(PairOfIntLong.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		//

		/*conf.set("Ivory.IndexPath", "/umd/indexes/medline04.segmentA/postings,"
				+ "/umd/indexes/medline04.segmentB/postings,"
				+ "/umd/indexes/medline04.segmentC/postings,"
				+ "/umd/indexes/medline04.segmentD/postings,"
				+ "/umd/indexes/medline04.segmentE/postings");
		String collectionName = "medline04.all";*/

		conf.set("Ivory.IndexPath", "/umd/indexes/clue.en.segment.01.stable/postings,"
				+ "/umd/indexes/clue.en.segment.02.stable/postings,"
				+ "/umd/indexes/clue.en.segment.03.stable/postings,"
				+ "/umd/indexes/clue.en.segment.04.stable/postings,"
				+ "/umd/indexes/clue.en.segment.05.stable/postings,"
				+ "/umd/indexes/clue.en.segment.06.stable/postings,"
				+ "/umd/indexes/clue.en.segment.07.stable/postings,"
				+ "/umd/indexes/clue.en.segment.08.stable/postings,"
				+ "/umd/indexes/clue.en.segment.09.stable/postings,"
				+ "/umd/indexes/clue.en.segment.10.stable/postings"
		);
		String collectionName = "clue.all";


		conf.set("Ivory.CollectionName", collectionName);
		String dictSizePath = "/user/telsayed/ivory/collections/" + collectionName + "/dict.size";
		conf.set("Ivory.DictSizePath", dictSizePath);
		conf.setInt("Ivory.DfThreshold", 3);
		conf.setInt("Ivory.NumMapTasks", 200);

		ComputeDistributedDictionarySize dictSizeTask = new ComputeDistributedDictionarySize(conf);
		dictSizeTask.run();

		int nTerms = FSProperty.readInt(FileSystem.get(conf), dictSizePath);
		sLogger.info("nTerms = " + nTerms);

		String prefixEncodedTermsFile = "/user/telsayed/ivory/collections/" + collectionName
		+ "/dict.terms";
		String dfStatsPath = "/user/telsayed/ivory/collections/" + collectionName + "/dict.df";
		String cfStatsPath = "/user/telsayed/ivory/collections/" + collectionName + "/dict.cf";
		String tmpPath = "/user/telsayed/ivory/collections/"+collectionName+"/tmp";

		conf.set("Ivory.TmpPath", tmpPath);
		conf.setInt("Ivory.IndexNumberOfTerms", nTerms);
		conf.setInt("Ivory.ForwardIndexWindow", 8);
		conf.set("Ivory.PrefixEncodedTermsFile", prefixEncodedTermsFile);
		conf.set("Ivory.DFStatsFile", dfStatsPath);
		conf.set("Ivory.CFStatsFile", cfStatsPath);

		ExtractGlobalStatsFromPostings task = new ExtractGlobalStatsFromPostings(conf);
		task.runTool();

		PrefixEncodedGlobalStats gs = new PrefixEncodedGlobalStats(new Path(prefixEncodedTermsFile));
		gs.loadDFStats(new Path(dfStatsPath));
		System.out.println(gs.getDF("iraq"));
		gs.loadCFStats(new Path(cfStatsPath));
		System.out.println(gs.getCF("iraq"));
		gs.printKeys();
	}
}
