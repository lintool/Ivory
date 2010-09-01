/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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

import ivory.index.BuildInvertedIndexDocSorted;
import ivory.index.BuildPostingsForwardIndex;
import ivory.index.ComputeDictionarySize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>Tool for indexing the ClueWeb09 collection.</p>
 * 
 * @author Jimmy Lin
 *
 */
public class BuildIndexClueWebEnglish extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(BuildIndexClueWebEnglish.class);

	private static int[] sSegmentDocCounts = new int[] { 3382356, 50220423, 51577077, 50547493,
			52311060, 50756858, 50559093, 52472358, 49545346, 50738874, 45175228 };

	private static int[] sDocnoOffsets = new int[] { 0, 0, 50220423, 101797500, 152344993,
			204656053, 255412911, 305972004, 358444362, 407989708, 458728582 };

	private BuildIndexClueWebEnglish() {
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [index-path] [segment-num]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			return -1;
		}

		String collection = args[0];
		String root = args[1];
		int segment = Integer.parseInt(args[2]);

		sLogger.info("Tool name: BuildIndexClueWebEnglish");
		sLogger.info(" - Collection path: " + collection);
		sLogger.info(" - Index path: " + root);
		sLogger.info(" - segement number: " + segment);

		Configuration config = new Configuration();

		config.set("Ivory.CollectionName", "ClueWeb:English:Segment" + segment);
		config.setInt("Ivory.NumMapTasks", 1000);
		config.setInt("Ivory.NumReduceTasks", 200);

		// config.setInt("Ivory.NumReduceTasks", 400);
		// config.setInt("Ivory.MinSplitSize", 256 * 1024 * 1024);
		// config.setInt("Ivory.MinSplitSize", 512 * 1024 * 1024);
		
		config.set("Ivory.CollectionPath", collection);
		config.set("Ivory.InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat");
		config.set("Ivory.IndexPath", root);
		config.set("Ivory.Tokenizer", "ivory.util.GalagoTokenizer");
		config
				.set("Ivory.DocnoMappingClass",
						"edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping");
		config.set("Ivory.DocnoMappingFile", root + "docno.mapping");
		config.setInt("Ivory.CollectionDocumentCount", sSegmentDocCounts[segment]);
		config.setBoolean("Ivory.Positional", true);

		config.setInt("Ivory.DocnoOffset", sDocnoOffsets[segment]);

		BuildInvertedIndexDocSorted indexTool = new BuildInvertedIndexDocSorted(config);
		indexTool.run();

		int dfThreshold = 100;

		config.setInt("Ivory.DfThreshold", dfThreshold);

		ComputeDictionarySize dictTool = new ComputeDictionarySize(config);
		int size = dictTool.run();
		sLogger.info("Size of dictionary (df>" + dfThreshold + "): " + size);

		config.setInt("Ivory.IndexNumberOfTerms", size);
		config.setInt("Ivory.ForwardIndexWindow", 8);
		BuildPostingsForwardIndex postingsTool = new BuildPostingsForwardIndex(config);
		postingsTool.run();
		
		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BuildIndexClueWebEnglish(), args);
		System.exit(res);
	}
}
