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

package ivory.pwsim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RunPCP extends Configured implements Tool {

	private static int printUsage() {
		System.out
				.println("usage: [index-root] [num-of-mappers] [num-of-reducers] [dfCut] [BlockSize] [scoringModel] [Top-N]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {

		if (args.length < 6) {
			printUsage();
			return -1;
		}

		String indexPath = args[0];
		int numMappers = Integer.parseInt(args[1]);
		int numReducers = Integer.parseInt(args[2]);

		Configuration config = new Configuration();

		config.setInt("Ivory.NumMapTasks", numMappers);
		config.setInt("Ivory.NumReduceTasks", numReducers);

		int dfCut = Integer.parseInt(args[3]);

		int blockSize = Integer.parseInt(args[4]);

		String scoringModel = args[5];
		String fn = args[5];
		int i = scoringModel.lastIndexOf(".");
		if (i >= 0)
			fn = scoringModel.substring(i + 1);

		int topN = -1;
		if (args.length == 7)
			topN = Integer.parseInt(args[6]);

		String pwsimOutputPath = indexPath + "/pcp-dfCut=" + dfCut + "-blk=" + blockSize
			+ "-" + fn + (topN > 0 ? "-topN=" + topN : "");
		String pwsimResultsOutputPath = pwsimOutputPath + "-results";

		//config.set("mapred.child.java.opts", "-Xmx1024m");
		config.set("mapred.child.java.opts", "-Xmx2048m");
		config.set("Ivory.IndexPath", indexPath);
		config.set("Ivory.PwsimOutputPath", pwsimOutputPath);
		config.set("Ivory.PwsimResultsOutputPath", pwsimResultsOutputPath);

		config.set("Ivory.ScoringModel", scoringModel);

		config.setInt("Ivory.DfCut", dfCut);
		config.setInt("Ivory.BlockSize", blockSize);

		config.setInt("Ivory.TopN", topN);

		PCP pwsimTask = new PCP(config);
		pwsimTask.run();

		PrintPCP printPCPTask = new PrintPCP(config);
		printPCPTask.run();

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RunPCP(), args);
		System.exit(res);
	}
}
