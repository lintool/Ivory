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
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;

import ivory.ptc.data.AnchorTextTarget;
import ivory.ptc.judgments.weighting.WeightingScheme;

/**
 * Map-Reduce job that constructs anchor text-inverted index.
 * The inverted index contains, for each unique anchor text,
 * a list of documents that are pointed to by that anchor text.
 *
 * @author Nima Asadi
 */
@SuppressWarnings("deprecation")
public class AnchorTextInvertedIndex extends PowerTool {
	private static final Logger LOG = Logger.getLogger(AnchorTextInvertedIndex.class);
	public static final String PARAMETER_SEPARATER = ",";

	static {
		LOG.setLevel(Level.INFO);
	}

	private static class MyMapper extends MapReduceBase implements
	    Mapper<IntWritable, ArrayListWritable<AnchorText>, Text, AnchorTextTarget> {
		private static final AnchorTextTarget anchorTextTarget = new AnchorTextTarget();
		private static final Text keyOut = new Text();
		// Weighting scheme used to rank target documents
		private static WeightingScheme weightingScheme;

    @Override
		public void configure(JobConf job) {
			Path[] localFiles;
      try {
        localFiles = DistributedCache.getLocalCacheFiles(job);
      } catch (IOException e) {
        throw new RuntimeException("Local cache files not read properly.");
      }
      
      String[] params = new String[localFiles.length];
      for (int i = 0; i < params.length; i++) {
      	params[i] = localFiles[i].toString();
    	}

			try {
				weightingScheme = (WeightingScheme) Class.forName(
				    job.get("Ivory.WeightingScheme")).newInstance();
				weightingScheme.initialize(FileSystem.getLocal(job), params);
			} catch (Exception e) {
				throw new RuntimeException("Mapper failed to initialize the weighting scheme: "
						+ job.get("Ivory.WeightingScheme") + " with parameters: "
						    + job.get("Ivory.WeightingSchemeParameters"));
			}
		}

    @Override
		public void map(IntWritable key, ArrayListWritable<AnchorText> anchors,
				OutputCollector<Text, AnchorTextTarget> output, Reporter reporter) throws IOException {
			anchorTextTarget.setTarget(key.get());
			for (AnchorText anchor : anchors) {
			  // Internal links provide navigational information which are not useful for our purposes.
				if (!anchor.isExternalInLink() && !anchor.isInternalInLink()) {
					continue;
				}

				keyOut.set(anchor.getText());
				anchorTextTarget.setSources(new ArrayListOfIntsWritable(anchor.getDocuments()));
				anchorTextTarget.setWeight(weightingScheme.getWeight(key.get(), anchor));
				output.collect(keyOut, anchorTextTarget);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
	    Reducer<Text, AnchorTextTarget, Text, ArrayListWritable<AnchorTextTarget>> {
		private static final ArrayListWritable<AnchorTextTarget> outList =
			  new ArrayListWritable<AnchorTextTarget>();

    @Override
		public void reduce(Text anchorText, Iterator<AnchorTextTarget> values,
				OutputCollector<Text, ArrayListWritable<AnchorTextTarget>> output, Reporter reporter)
		        throws IOException {
			outList.clear();
			while (values.hasNext()) {
				outList.add(new AnchorTextTarget(values.next()));
			}

			Collections.sort(outList);
			output.collect(anchorText, outList);
		}
	}

	public static final String[] RequiredParameters = {
		"Ivory.NumMapTasks",
		"Ivory.NumReduceTasks",
		"Ivory.InputPath",
		"Ivory.OutputPath",
		"Ivory.WeightingScheme",
		"Ivory.WeightingSchemeParameters",
	};

  @Override
	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public AnchorTextInvertedIndex(Configuration conf) {
		super(conf);
	}

  @Override
	public int runTool() throws Exception {
		JobConf conf = new JobConf(getConf(), AnchorTextInvertedIndex.class);
		FileSystem fs = FileSystem.get(conf);
		String inPath = conf.get("Ivory.InputPath");
		String outPath = conf.get("Ivory.OutputPath");
		Path inputPath = new Path(inPath);
		Path outputPath = new Path(outPath);
		int mapTasks = conf.getInt("Ivory.NumMapTasks", 1);
		int reduceTasks = conf.getInt("Ivory.NumReduceTasks", 100);
		String weightingSchemeParameters = conf.get("Ivory.WeightingSchemeParameters");

		LOG.info("BuildAnchorTextInvertedIndex");
		LOG.info(" - input path: " + inPath);
		LOG.info(" - output path: " + outPath);
		LOG.info(" - number of reducers: " + reduceTasks);
		LOG.info(" - weighting scheme: " + conf.get("Ivory.WeightingScheme"));
		LOG.info(" - weighting scheme parameters: " + weightingSchemeParameters);

		String[] params = weightingSchemeParameters.split(PARAMETER_SEPARATER);
		for(String param : params) {
			DistributedCache.addCacheFile(new URI(param), conf);
		}

		conf.setJobName("BuildAnchorTextInvertedIndex");
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		conf.set("mapred.child.java.opts", "-Xmx4096m");
		conf.setInt("mapred.task.timeout", 60000000);

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(AnchorTextTarget.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(ArrayListWritable.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		fs.delete(outputPath);
		JobClient.runJob(conf);
		return 0;
	}
}
