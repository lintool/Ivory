/**
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

package ivory.ptc.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import ivory.ptc.AnchorTextInvertedIndex;

/**
 * Driver to build anchor text inverted index. This driver computes a
 * confidence score for target documents (i.e., potential pseudo judgments)
 * according to the provided {@link ivory.ptc.judgments.weighting.WeightingScheme}.
 *
 * @author Nima Asadi
 */
public class BuildAnchorTextInvertedIndex extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(BuildAnchorTextInvertedIndex.class);

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-of-reducers] "
				+ "[weighting-scheme-class] [weighting-scheme-params]\n"
				    + " - [input-path] is the path to an (weighted) inverse webgraph w/ anchor text\n"
				        + " - [weighting-scheme-params] is a list of parameters separated by \""
				            + AnchorTextInvertedIndex.PARAMETER_SEPARATER + "\"");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

  @Override
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage();
			return -1;
		}

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		// Command line arguments
		String inPath = args[0];
		String outPath = args[1];
		int numReducers = Integer.parseInt(args[2]);
		String weightingSchemeClass = args[3];
		String weightingSchemeParameters = args[4];
		int numMappers = 1;
		Path inputPath = new Path(inPath);
		if (!fs.exists(inputPath)) {
			LOG.warn("Input webgraph doesn't exist...");
			return -1;
		}

		conf.set("Ivory.InputPath", inPath);
		conf.set("Ivory.OutputPath", outPath);
		conf.setInt("Ivory.NumMapTasks", numMappers);
		conf.setInt("Ivory.NumReduceTasks", numReducers);
		conf.set("Ivory.WeightingScheme", weightingSchemeClass);
		conf.set("Ivory.WeightingSchemeParameters", weightingSchemeParameters);

		AnchorTextInvertedIndex indexTool = new AnchorTextInvertedIndex(conf);
		indexTool.run();
		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BuildAnchorTextInvertedIndex(), args);
		System.exit(res);
	}
}
