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

import ivory.ptc.SortedPseudoTestCollection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



/**
 * Driver that extracts a set of pseudo queries and pseudo judgments according
 * to a sampling criterion ({@link ivory.ptc.sampling.Criterion}).
 *
 * @author Nima Asadi
 */
public class ExtractSortedPseudoTestCollection extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(ExtractSortedPseudoTestCollection.class);

	private static int printUsage() {
		System.out.println("usage: [AnchorText-inverted-index-path] [output-path] "
		    + "[judgment-extractor-class] [judgment-extractor-parameters] "
				    + "[query-scorer-class]"
						    + "[sampling-criterion-class] [sampling-criterion-parameters]\n"
							      + " - [judgment-extractor-parameters] is a list of parameters "
							          + "separated by \"" + SortedPseudoTestCollection.PARAMETERS_SPERATOR + "\"\n"
							              + " - [sampling-criterion-parameters] is a list of parameters "
							                  + "separated by \"" + SortedPseudoTestCollection.PARAMETERS_SPERATOR + "\"");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

  @Override
	public int run(String[] args) throws Exception {
		if (args.length != 7) {
			printUsage();
			return -1;
		}

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
    // Command line arguments
		String indexPath = args[0];
		String outputPath = args[1];
		String judgmentExtractor = args[2];
		String judgmentExtractorParameters = args[3];
		String queryScorer = args[4];
		String samplingCriterion = args[5];
		String samplingCriterionParameters = args[6];

		Path p = new Path(indexPath);
		if (!fs.exists(p)) {
			LOG.warn("AnchorText inverted index path doesn't exist...");
			return -1;
		}

		conf.set("Ivory.InputPath", indexPath);
		conf.set("Ivory.OutputPath", outputPath);
		conf.set("Ivory.JudgmentExtractor", judgmentExtractor);
		conf.set("Ivory.JudgmentExtractorParameters", judgmentExtractorParameters);
		conf.set("Ivory.QueryScorer", queryScorer);
		conf.set("Ivory.SamplingCriterion", samplingCriterion);
		conf.set("Ivory.SamplingCriterionParameters", samplingCriterionParameters);

		SortedPseudoTestCollection ex = new SortedPseudoTestCollection(conf);
		ex.run();
		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ExtractSortedPseudoTestCollection(), args);
		System.exit(res);
	}
}
