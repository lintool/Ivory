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

import ivory.data.PrefixEncodedTermSet;
import ivory.util.QuickSort;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.PairOfIntLong;
import edu.umd.cloud9.util.PowerTool;

@SuppressWarnings("deprecation")
public class BuildTermIdMap extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(BuildTermIdMap.class);

	protected static enum Terms {
		Total
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, PairOfIntLong, NullWritable, NullWritable> {

		FSDataOutputStream mTermsOut, mIdsOut, mIdsToTermOut;
		FSDataOutputStream mDfByTermOut, mCfByTermOut;
		FSDataOutputStream mDfByIntOut, mCfByIntOut;

		int nTerms, window;

		int[] seqNums = null;
		int[] dfs = null;
		long[] cfs = null;

		public void configure(JobConf job) {
			FileSystem fs;
			try {
				fs = FileSystem.get(job);
			} catch (IOException e) {
				throw new RuntimeException("Error opening the FileSystem!");
			}

			String indexPath = job.get("Ivory.IndexPath");
			
			RetrievalEnvironment env = null;
			try {
				env = new RetrievalEnvironment(indexPath, fs);
			} catch (IOException e) {
				throw new RuntimeException("Unable to create RetrievalEnvironment!");
			}

			String termsFile = env.getIndexTermsData();
			String idsFile = env.getIndexTermIdsData();
			String idToTermFile = env.getIndexTermIdMappingData();

			String dfByTermFile = env.getDfByTermData();
			String cfByTermFile = env.getCfByTermData();
			String dfByIntFile = env.getDfByIntData();
			String cfByIntFile = env.getCfByIntData();

			nTerms = job.getInt("Ivory.CollectionTermCount", 0);
			window = job.getInt("Ivory.TermIndexWindow", 8);

			seqNums = new int[nTerms];
			dfs = new int[nTerms];
			cfs = new long[nTerms];

			sLogger.info("Ivory.PrefixEncodedTermsFile: " + termsFile);
			sLogger.info("Ivory.TermIDsFile" + idsFile);
			sLogger.info("Ivory.IDToTermFile" + idToTermFile);
			sLogger.info("Ivory.CollectionTermCount: " + nTerms);
			sLogger.info("Ivory.ForwardIndexWindow: " + window);

			try {
				mTermsOut = fs.create(new Path(termsFile), true);
				mIdsOut = fs.create(new Path(idsFile), true);
				mIdsToTermOut = fs.create(new Path(idToTermFile), true);
				mTermsOut.writeInt(nTerms);
				mTermsOut.writeInt(window);
				mIdsOut.writeInt(nTerms);
				mIdsToTermOut.writeInt(nTerms);

				mDfByTermOut = fs.create(new Path(dfByTermFile), true);
				mCfByTermOut = fs.create(new Path(cfByTermFile), true);
				mDfByTermOut.writeInt(nTerms);
				mCfByTermOut.writeInt(nTerms);

				mDfByIntOut = fs.create(new Path(dfByIntFile), true);
				mCfByIntOut = fs.create(new Path(cfByIntFile), true);
				mDfByIntOut.writeInt(nTerms);
				mCfByIntOut.writeInt(nTerms);
			} catch (Exception e) {
				throw new RuntimeException("error in creating files");
			}
			sLogger.info("Finished config.");
		}

		int curKeyIndex = 0;
		String lastKey = "";

		public void reduce(Text key, Iterator<PairOfIntLong> values,
				OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
				throws IOException {
			String term = key.toString();
			PairOfIntLong p = values.next();
			int df = p.getLeftElement();
			long cf = p.getRightElement();
			WritableUtils.writeVInt(mDfByTermOut, df);
			WritableUtils.writeVLong(mCfByTermOut, cf);
			if (values.hasNext()) {
				throw new RuntimeException("More than one record for term: " + term);
			}

			int prefixLength;

			if (curKeyIndex % window == 0) {
				byte[] byteArray = term.getBytes();
				mTermsOut.writeByte((byte) (byteArray.length)); // suffix length
				for (int j = 0; j < byteArray.length; j++)
					mTermsOut.writeByte(byteArray[j]);
			} else {
				prefixLength = PrefixEncodedTermSet.getPrefix(lastKey, term);
				byte[] suffix = term.substring(prefixLength).getBytes();

				if (prefixLength > Byte.MAX_VALUE || suffix.length > Byte.MAX_VALUE)
					throw new RuntimeException("prefix/suffix length overflow");

				mTermsOut.writeByte((byte) suffix.length); // suffix length
				mTermsOut.writeByte((byte) prefixLength); // prefix length
				for (int j = 0; j < suffix.length; j++)
					mTermsOut.writeByte(suffix[j]);
			}
			lastKey = term;
			seqNums[curKeyIndex] = curKeyIndex;
			dfs[curKeyIndex] = -df;
			cfs[curKeyIndex] = cf;
			curKeyIndex++;

			reporter.incrCounter(Terms.Total, 1);
		}

		public void close() throws IOException {
			sLogger.info("Finished reduce.");
			if (curKeyIndex != nTerms)
				throw new RuntimeException("Total expected Terms: " + nTerms
						+ ", Total observed terms: " + curKeyIndex + "!!!");
			// sort based on df and change seqNums accordingly
			QuickSort.quicksortWithSecondary(seqNums, dfs, cfs, 0, nTerms - 1);

			// ========= write sorted dfs and cfs by int here
			for (int i = 0; i < nTerms; i++) {
				WritableUtils.writeVInt(mDfByIntOut, -dfs[i]);
				WritableUtils.writeVLong(mCfByIntOut, cfs[i]);
			}
			cfs = null;
			// encode the sorted dfs into ids ==> df values erased and become
			// ids instead
			// notice that first term id is 1 not 0, because 0 cannot be easily
			// encoded (?)
			for (int i = 0; i < nTerms; i++)
				dfs[i] = i + 1;

			// idToTermIndex = new int[nTerms];
			// for(int i = 0; i<nTerms; i++) idToTermIndex[i] = seqNums[i];

			// write current seq nums to be index into the term array
			for (int i = 0; i < nTerms; i++)
				mIdsToTermOut.writeInt(seqNums[i]);

			// sort on seqNums to get the right writing order
			QuickSort.quicksort(dfs, seqNums, 0, nTerms - 1);
			for (int i = 0; i < nTerms; i++)
				mIdsOut.writeInt(dfs[i]);

			mTermsOut.close();
			mIdsOut.close();
			mIdsToTermOut.close();
			mDfByTermOut.close();
			mCfByTermOut.close();
			mDfByIntOut.close();
			mCfByIntOut.close();
			sLogger.info("Finished close.");
		}
	}

	public static final String[] RequiredParameters = { "Ivory.NumMapTasks",
			"Ivory.CollectionName", "Ivory.IndexPath", "Ivory.TermIndexWindow" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildTermIdMap(Configuration conf) {
		super(conf);
	}

	@SuppressWarnings("unused")
	public int runTool() throws Exception {
		// create a new JobConf, inheriting from the configuration of this
		// PowerTool
		JobConf conf = new JobConf(getConf(), BuildTermIdMap.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = conf.get("Ivory.IndexPath");
		String collectionName = conf.get("Ivory.CollectionName");

		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
		int reduceTasks = 1;
		int minSplitSize = conf.getInt("Ivory.MinSplitSize", 0);

		sLogger.info("PowerTool: BuildTermIdMap");
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - IndexPath: " + indexPath);
		sLogger.info(" - NumMapTasks: " + mapTasks);
		sLogger.info(" - NumReduceTasks: " + reduceTasks);

		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		if (!fs.exists(new Path(indexPath))) {
			sLogger.error("index path doesn't existing: skipping!");
			return 0;
		}

		Path termsFilePath = new Path(env.getIndexTermsData());
		Path termIDsFilePath = new Path(env.getIndexTermIdsData());
		Path idToTermFilePath = new Path(env.getIndexTermIdMappingData());
		Path dfByTermFilePath = new Path(env.getDfByTermData());
		Path cfByTermFilePath = new Path(env.getCfByTermData());
		Path dfByIntFilePath = new Path(env.getDfByIntData());
		Path cfByIntFilePath = new Path(env.getCfByIntData());

		if (fs.exists(termsFilePath) || fs.exists(termIDsFilePath) || fs.exists(idToTermFilePath)
				|| fs.exists(dfByTermFilePath) || fs.exists(cfByTermFilePath)
				|| fs.exists(dfByIntFilePath) || fs.exists(cfByIntFilePath)) {
			sLogger.info("term and term id data exist: skipping!");
			return 0;
		}

		Path tmpPath = new Path(env.getTempDirectory());
		fs.delete(tmpPath, true);

		conf.setJobName("BuildTermIdMap:" + collectionName);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		conf.setInt("Ivory.CollectionTermCount", (int) env.readCollectionTermCount());
		conf.setInt("mapred.min.split.size", minSplitSize);
		conf.set("mapred.child.java.opts", "-Xmx2048m");

		FileInputFormat.setInputPaths(conf, new Path(env.getTermDfCfDirectory()));
		FileOutputFormat.setOutputPath(conf, tmpPath);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(PairOfIntLong.class);
		conf.setOutputKeyClass(Text.class);

		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		RunningJob job = JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		fs.delete(tmpPath, true);

		return 0;
	}
}
