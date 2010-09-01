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

import ivory.index.BuildInvertedIndexDocSorted;
import ivory.index.BuildPostingsForwardIndex;
import ivory.index.ComputeDictionarySize;
import ivory.index.ExtractCfFromPostings;
import ivory.index.ExtractDfFromPostings;
import ivory.index.MergePostings;
import ivory.util.RetrievalEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.medline.NumberMedlineCitations;

/**
 * <p>
 * Tool for indexing the MEDLINE04 collection.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class BuildIndexMedline extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(BuildIndexMedline.class);

	private static int printUsage() {
		System.out.println("usage: [input-path] [index-path] [num-mappers] [num-reduers]");
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

		String collection = args[0];
		String root = args[1];
		int numMappers = Integer.parseInt(args[2]);
		int numReducers = Integer.parseInt(args[3]);

		boolean positional = true;

		sLogger.info("Tool name: BuildIndexMedline");
		sLogger.info(" - Collection path: " + collection);
		sLogger.info(" - Index path: " + root);
		sLogger.info(" - positional? " + positional);
		sLogger.info("Launching with " + numMappers + " mappers, " + numReducers + " reducers...");

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path p = new Path(root);
		if (!fs.exists(p)) {
			sLogger.info("Index path doesn't exist, creating...");
			fs.mkdirs(p);
		}

		String mappingFile = RetrievalEnvironment.getDocnoMappingFile(root);
		p = new Path(mappingFile);
		if (!fs.exists(p)) {
			sLogger.info(mappingFile + " doesn't exist, creating...");
			String[] arr = new String[] { collection, root + "/medline-docid-tmp",
					mappingFile, new Integer(numMappers).toString() };
			NumberMedlineCitations tool = new NumberMedlineCitations();
			tool.setConf(conf);
			tool.run(arr);

			fs.delete(new Path(root + "/medline-docid-tmp"), true);
		}

		conf.setInt("Ivory.NumMapTasks", numMappers);
		conf.setInt("Ivory.NumReduceTasks", numReducers);

		conf.set("Ivory.CollectionName", "medline04");
		conf.set("Ivory.CollectionPath", collection);
		conf.set("Ivory.IndexPath", root);
		conf.set("Ivory.InputFormat",
				"edu.umd.cloud9.collection.medline.MedlineCitationInputFormat");
		conf
				.set("Ivory.DocnoMappingClass",
						"edu.umd.cloud9.collection.medline.MedlineDocnoMapping");
		conf.set("Ivory.DocnoMappingFile", root + "docno.mapping");
		conf.set("Ivory.Tokenizer", "ivory.util.GalagoTokenizer");

		conf.setInt("Ivory.CollectionDocumentCount", 4591008);
		conf.setBoolean("Ivory.Positional", positional);

		BuildInvertedIndexDocSorted indexTool = new BuildInvertedIndexDocSorted(conf);
		indexTool.run();

		MergePostings mergeTool = new MergePostings(conf);
		mergeTool.run();

		ExtractDfFromPostings dfTool = new ExtractDfFromPostings(conf);
		dfTool.run();

		ExtractCfFromPostings cfTool = new ExtractCfFromPostings(conf);
		cfTool.run();

		int dfThreshold = 0;

		conf.setInt("Ivory.DfThreshold", dfThreshold);

		ComputeDictionarySize dictTool = new ComputeDictionarySize(conf);
		int n = dictTool.run();

		sLogger.info("Building terms to postings forward index with " + n + " terms");

		conf.setInt("Ivory.IndexNumberOfTerms", n);
		conf.setInt("Ivory.ForwardIndexWindow", 8);
		conf.setInt("Ivory.DfThreshold", 0);

		BuildPostingsForwardIndex postingsTool = new BuildPostingsForwardIndex(conf);
		postingsTool.run();

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BuildIndexMedline(), args);
		System.exit(res);
	}
}
