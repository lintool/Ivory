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

package ivory.ptc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.util.PowerTool;

import ivory.ptc.data.AnchorTextTarget;
import ivory.ptc.data.PseudoJudgments;
import ivory.ptc.data.PseudoQuery;
import ivory.ptc.judgments.extractor.PseudoJudgmentExtractor;
import ivory.ptc.sampling.Criterion;
import ivory.ptc.scorer.PseudoQueryScorer;

/**
 * Map-Reduce job to extract a Pseudo Test Collection according to
 * a sampling criterion.
 *
 * @author Nima Asadi
 */
@SuppressWarnings("deprecation")
public class SortedPseudoTestCollection extends PowerTool {
	public static final String PARAMETERS_SPERATOR =",";
	private static final Logger LOG = Logger.getLogger(SortedPseudoTestCollection.class);
	static {
		LOG.setLevel(Level.INFO);
	}

	private static class MyMapper extends MapReduceBase implements
	    Mapper<Text, ArrayListWritable<AnchorTextTarget>, PseudoQuery, PseudoJudgments> {
		private static final PseudoQuery pseudoQuery = new PseudoQuery();
		private static PseudoJudgmentExtractor pseudoJudgmentExtractor;
		private static String queryText;
		private static PseudoJudgments pseudoJudgments;
		private static PseudoQueryScorer queryScorer;

		public void configure(JobConf job) {
			try {
				pseudoJudgmentExtractor = (PseudoJudgmentExtractor) Class.forName(
						job.get("Ivory.JudgmentExtractor")).newInstance();
				String[] params = job.get("Ivory.JudgmentExtractorParameters").split(PARAMETERS_SPERATOR);
				pseudoJudgmentExtractor.setParameters(params);
			} catch (Exception e) {
				throw new RuntimeException("Mapper failed to initialize the judgment extractor: "
				    + job.get("Ivory.QueryExtractor") + " with parameters: "
						    + job.get("Ivory.QueryExtractorParameters"));
			}

			try {
				queryScorer = (PseudoQueryScorer) Class.forName(
				    job.get("Ivory.QueryScorer")).newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Mapper failed to initialize the scorer");
			}
		}

		public void map(Text key, ArrayListWritable<AnchorTextTarget> anchorTextTargets,
				OutputCollector<PseudoQuery, PseudoJudgments> output, Reporter reporter)
				    throws IOException {
			queryText = key.toString();
			pseudoJudgments = pseudoJudgmentExtractor.getPseudoJudgments(anchorTextTargets);

			if (pseudoJudgments.size() > 0) {
				pseudoQuery.set(queryText, queryScorer.getScore(queryText, pseudoJudgments));
				output.collect(pseudoQuery, pseudoJudgments);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
	    Reducer<PseudoQuery, PseudoJudgments, PseudoQuery, PseudoJudgments> {
		private static Criterion criterion;
		private static PseudoJudgments nextJudgments;

		public void configure(JobConf job) {
			try {
				criterion = (Criterion) Class.forName(
											job.get("Ivory.SamplingCriterion")).newInstance();
				String[] params = job.get("Ivory.SamplingCriterionParameters").split(PARAMETERS_SPERATOR);
				criterion.initialize(FileSystem.get(job), params);
			} catch (Exception e) {
				throw new RuntimeException("Mapper failed to initialize the sampling criterion: "
						+ job.get("Ivory.SamplingCriterion") + " with parameters: "
						    + job.get("Ivory.SamplingCriterionParameters"));
			}
		}

		public void reduce(PseudoQuery query, Iterator<PseudoJudgments> judgments,
				OutputCollector<PseudoQuery, PseudoJudgments> output, Reporter reporter) throws IOException {
			while (judgments.hasNext()) {
				nextJudgments = judgments.next();

				if (criterion.meets(query, nextJudgments)) {
					output.collect(query, nextJudgments);
				}
			}
		}
	}

	public static final String[] RequiredParameters = {
		"Ivory.JudgmentExtractor",
		"Ivory.JudgmentExtractorParameters",
		"Ivory.QueryScorer",
		"Ivory.SamplingCriterion",
		"Ivory.SamplingCriterionParameters",
		"Ivory.InputPath",
		"Ivory.OutputPath",
	};

  @Override
	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public SortedPseudoTestCollection(Configuration conf) {
		super(conf);
	}

	public int runTool() throws Exception {
		JobConf conf = new JobConf(getConf(), SortedPseudoTestCollection.class);
		FileSystem fs = FileSystem.get(conf);
		String inPath = conf.get("Ivory.InputPath");
		String outPath = conf.get("Ivory.OutputPath");
		Path inputPath = new Path(inPath);
		Path outputPath = new Path(outPath);
		int mapTasks = 1;
		int reduceTasks = 1;

		LOG.info("SortedPseudoTestCollection");
		LOG.info(" - Input path: " + conf.get("Ivory.InputPath"));
		LOG.info(" - Output path: " + conf.get("Ivory.OutputPath"));
		LOG.info(" - JudgmentExtractor: " + conf.get("Ivory.JudgmentExtractor"));
		LOG.info(" - JudgmentExtractorParameters: " + conf.get("Ivory.JudgmentExtractorParameters"));
		LOG.info(" - SamplingCriterion: " + conf.get("Ivory.SamplingCriterion"));
		LOG.info(" - SamplingCriterionParameters: " + conf.get("Ivory.SamplingCriterionParameters"));
		LOG.info(" - QueryScorer: " + conf.get("Ivory.QueryScorer"));

		conf.setJobName("SortedPTC");
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		conf.set("mapred.child.java.opts", "-Xmx4096m");

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setMapOutputKeyClass(PseudoQuery.class);
		conf.setMapOutputValueClass(PseudoJudgments.class);
		conf.setOutputKeyClass(PseudoQuery.class);
		conf.setOutputValueClass(PseudoJudgments.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		fs.delete(outputPath);
		JobClient.runJob(conf);
		return 0;
	}
}
