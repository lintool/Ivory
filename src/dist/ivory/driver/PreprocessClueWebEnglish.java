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

package ivory.driver;

import ivory.preprocess.BuildIntDocVectors;
import ivory.preprocess.BuildIntDocVectorsForwardIndex;
import ivory.preprocess.BuildTermDocVectors;
import ivory.preprocess.BuildTermDocVectorsForwardIndex;
import ivory.preprocess.BuildTermIdMap;
import ivory.preprocess.GetTermCount;
import ivory.util.RetrievalEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class PreprocessClueWebEnglish extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(PreprocessClueWebEnglish.class);

	public static int[] SegmentDocCounts = new int[] { 3382356, 50220423, 51577077, 50547493,
			52311060, 50756858, 50559093, 52472358, 49545346, 50738874, 45175228 };

	public static int[] DocnoOffsets = new int[] { 0, 0, 50220423, 101797500, 152344993, 204656053,
			255412911, 305972004, 358444362, 407989708, 458728582 };

	private static int printUsage() {
		System.out
				.println("usage: [input-path] [index-path] [segment-num] [num-of-mappers] [num-of-reducers]");
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

		String collection = args[0];
		String indexPath = args[1];
		int segment = Integer.parseInt(args[2]);
		int numMappers = Integer.parseInt(args[3]);
		int numReducers = Integer.parseInt(args[4]);

		sLogger.info("Tool name: BuildIndexClueWebEnglish");
		sLogger.info(" - Collection path: " + collection);
		sLogger.info(" - Index path: " + indexPath);
		sLogger.info(" - segement number: " + segment);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		Path p = new Path(indexPath);
		if (!fs.exists(p)) {
			sLogger.error("Error: index path doesn't exist!");
			return 0;
		}

		p = new Path(env.getDocnoMappingData());
		if (!fs.exists(p)) {
			sLogger.error("Error: docno mapping data doesn't exist!");
			return 0;
		}

		conf.setInt("Ivory.NumMapTasks", numMappers);
		conf.setInt("Ivory.NumReduceTasks", numReducers);

		conf.set("Ivory.CollectionName", "ClueWeb:English:Segment" + segment);
		conf.set("Ivory.CollectionPath", collection);
		conf.set("Ivory.IndexPath", indexPath);
		conf.set("Ivory.InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat");
		conf.set("Ivory.Tokenizer", "ivory.util.GalagoTokenizer");
		conf.set("Ivory.DocnoMappingClass", "edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping");
		conf.set("Ivory.DocnoMappingFile", env.getDocnoMappingData());

		conf.setInt("Ivory.DocnoOffset", DocnoOffsets[segment]);
		conf.setInt("Ivory.MinDf", 10);
		conf.setInt("Ivory.MaxDf", Integer.MAX_VALUE);
		conf.setInt("Ivory.TermIndexWindow", 8);

		new BuildTermDocVectors(conf).run();
		new GetTermCount(conf).run();
		new BuildTermIdMap(conf).run();
		new BuildIntDocVectors(conf).run();

		new BuildIntDocVectorsForwardIndex(conf).run();
		new BuildTermDocVectorsForwardIndex(conf).run();

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PreprocessClueWebEnglish(), args);
		System.exit(res);
	}
}
