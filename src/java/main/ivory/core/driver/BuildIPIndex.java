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


import ivory.core.index.BuildIPInvertedIndexDocSorted;
import ivory.core.index.BuildIntPostingsForwardIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class BuildIPIndex extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(BuildIPIndex.class);

	private static int printUsage() {
		System.out.println("usage: [index-path] [num-of-mappers] [num-of-reducers]");
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

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		String indexPath = args[0];

		Path p = new Path(indexPath);
		if (!fs.exists(p)) {
			sLogger.warn("Index path doesn't exist...");
			return -1;
		}

		int numMappers = Integer.parseInt(args[1]);
		int numReducers = Integer.parseInt(args[2]);

		sLogger.info("Tool name: BuildIPIndex");
		sLogger.info(" - Index path: " + indexPath);

		conf.set("Ivory.IndexPath", indexPath);
		conf.setInt("Ivory.NumMapTasks", numMappers);
		conf.setInt("Ivory.NumReduceTasks", numReducers);

		BuildIPInvertedIndexDocSorted indexTool = new BuildIPInvertedIndexDocSorted(conf);
		indexTool.run();

		BuildIntPostingsForwardIndex postingsIndexer = new BuildIntPostingsForwardIndex(conf);
		postingsIndexer.run();

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BuildIPIndex(), args);
		System.exit(res);
	}
}
