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

package ivory.core.driver;


import ivory.core.Constants;
import ivory.core.RetrievalEnvironment;
import ivory.core.preprocess.BuildIntDocVectors;
import ivory.core.preprocess.BuildIntDocVectorsForwardIndex;
import ivory.core.preprocess.BuildTermDocVectors;
import ivory.core.preprocess.BuildTermDocVectorsForwardIndex;
import ivory.core.preprocess.BuildDictionary;
import ivory.core.preprocess.ComputeGlobalTermStatistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


@SuppressWarnings("deprecation")
public class PreprocessClueWebEnglishMultipleSegments extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(PreprocessClueWebEnglishMultipleSegments.class);

	public int run(String[] args) throws Exception {
		if (args.length < 4) {
			System.out.println("usage: [index-path] [num-of-mappers] [num-of-reducers] [input-path]...");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String indexPath = args[0];
		int numMappers = Integer.parseInt(args[1]);
		int numReducers = Integer.parseInt(args[2]);

		StringBuilder sb = new StringBuilder(args[3]);
		if (args.length > 4) {
			for (int i = 4; i < args.length; i++) {
				sb.append(",");
				sb.append(args[i]);
			}
		}
		String collection = sb.toString();

		LOG.info("Tool name: PreprocessClueWebEnglishMultipleSegments");
		LOG.info(" - Index path: " + indexPath);
		LOG.info(" - Collections: " + collection);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		Path p = new Path(indexPath);
		if (!fs.exists(p)) {
			LOG.error("Error: index path doesn't exist!");
			return 0;
		}

		if (!fs.exists(env.getDocnoMappingData())) {
			LOG.error("Error: docno mapping data doesn't exist!");
			return 0;
		}

		conf.setInt(Constants.NumMapTasks, numMappers);
		conf.setInt(Constants.NumReduceTasks, numReducers);

		conf.set(Constants.CollectionName, "ClueWeb:English");
		conf.set(Constants.CollectionPath, collection);
		conf.set(Constants.IndexPath, indexPath);
		conf.set(Constants.InputFormat, org.apache.hadoop.mapred.SequenceFileInputFormat.class.getCanonicalName());
		conf.set(Constants.Tokenizer, ivory.core.tokenize.GalagoTokenizer.class.getCanonicalName());
		conf.set(Constants.DocnoMappingClass, edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping.class.getCanonicalName());
		conf.set(Constants.DocnoMappingFile, env.getDocnoMappingData().toString());

		conf.setInt(Constants.DocnoOffset, 0);
		conf.setInt(Constants.MinDf, 50);
		conf.setInt(Constants.MaxDf, Integer.MAX_VALUE);
		conf.setInt(Constants.TermIndexWindow, 8);

		new BuildTermDocVectors(conf).run();
		new ComputeGlobalTermStatistics(conf).run();
		new BuildDictionary(conf).run();
		new BuildIntDocVectors(conf).run();

		new BuildIntDocVectorsForwardIndex(conf).run();
		new BuildTermDocVectorsForwardIndex(conf).run();

		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PreprocessClueWebEnglishMultipleSegments(), args);
	}
}
