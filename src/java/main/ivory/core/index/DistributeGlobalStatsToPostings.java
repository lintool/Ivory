package ivory.core.index;


import ivory.core.RetrievalEnvironment;
import ivory.core.data.dictionary.DefaultCachedFrequencySortedDictionary;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsListDocSortedPositional;
import ivory.core.data.stat.PrefixEncodedGlobalStats;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Logger;


import edu.umd.cloud9.io.pair.PairOfIntLong;
import edu.umd.cloud9.util.PowerTool;

public class DistributeGlobalStatsToPostings extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(DistributeGlobalStatsToPostings.class);

	private static class MyMapper extends MapReduceBase implements
			Mapper<IntWritable, PostingsList, IntWritable, PostingsList> {

		private PrefixEncodedGlobalStats gs;

		private DefaultCachedFrequencySortedDictionary mTermIdMap;

		public void configure(JobConf job) {
			try {
				Path[] localFiles = DistributedCache.getLocalCacheFiles(job);

				sLogger.info("0: " + localFiles[0]);
				sLogger.info("1: " + localFiles[1]);
				sLogger.info("2: " + localFiles[2]);
				sLogger.info("3: " + localFiles[3]);
				sLogger.info("4: " + localFiles[4]);
				sLogger.info("5: " + localFiles[5]);

				FileSystem fs = FileSystem.getLocal(job);

				gs = new PrefixEncodedGlobalStats(localFiles[0], fs);
				gs.loadDFStats(localFiles[1], fs);
				gs.loadCFStats(localFiles[2], fs);

				String indexPath = job.get("Ivory.IndexPath");
				sLogger.info("loading TermIdMap from " + indexPath);
				mTermIdMap = new DefaultCachedFrequencySortedDictionary(localFiles[3], localFiles[4],
						localFiles[5], 0.2f, fs);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Error loading global term stats!");
			}
		}

		public void map(IntWritable key, PostingsList p,
				OutputCollector<IntWritable, PostingsList> output, Reporter reporter)
				throws IOException {

			// map from the id back to text
			// sLogger.info("termid: " + key);
			String term = mTermIdMap.getTerm(key.get());
			// sLogger.info("term: " + term);
			PairOfIntLong pair = gs.getStats(term);

			if (pair == null) {
				p.setCf(-1);
				p.setDf(-1);
			} else {
				p.setCf(pair.getRightElement());
				p.setDf(pair.getLeftElement());
			}

			output.collect(key, p);
		}
	}

	public static final String[] RequiredParameters = { "Ivory.IndexPath", "Ivory.GlobalStatsPath",
			"Ivory.NumMapTasks" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public DistributeGlobalStatsToPostings(Configuration conf) {
		super(conf);
	}

	public int runTool() throws Exception {
		sLogger.info("Distributing df/cf stats...");

		JobConf conf = new JobConf(getConf(), DistributeGlobalStatsToPostings.class);
		FileSystem fs = FileSystem.get(conf);

		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
		int reduceTasks = conf.getInt("Ivory.NumReduceTasks", 1);

		String indexPath = conf.get("Ivory.IndexPath");
		String statsPath = conf.get("Ivory.GlobalStatsPath");

		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
		String collectionName = env.readCollectionName();

		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - IndexPath: " + indexPath);
		sLogger.info(" - NumMapTasks: " + mapTasks);
		sLogger.info(" - NumReduceTasks: " + reduceTasks);

		// back up old stats
		Path p1 = new Path(indexPath + "/property.CollectionDocumentCount");
		Path p2 = new Path(indexPath + "/property.CollectionDocumentCount.local");

		if (!fs.exists(p2)) {
			sLogger.info("preserving local " + p1.getName());
			fs.rename(p1, p2);
		}

		p1 = new Path(indexPath + "/property.CollectionAverageDocumentLength");
		p2 = new Path(indexPath + "/property.CollectionAverageDocumentLength.local");

		if (!fs.exists(p2)) {
			sLogger.info("preserving local " + p1.getName());
			fs.rename(p1, p2);
		}

		p1 = new Path(indexPath + "/property.CollectionLength");
		p2 = new Path(indexPath + "/property.CollectionLength.local");

		if (!fs.exists(p2)) {
			sLogger.info("preserving local " + p1.getName());
			fs.rename(p1, p2);
		}

		// distribute global stats
		RetrievalEnvironment genv = new RetrievalEnvironment(statsPath, fs);
		long collectionLength = genv.readCollectionLength();
		int docCount = genv.readCollectionDocumentCount();
		float avgdl = genv.readCollectionAverageDocumentLength();

		sLogger.info("writing global stats from all index segments: ");
		sLogger.info(" - CollectionLength: " + collectionLength);
		sLogger.info(" - CollectionDocumentCount: " + docCount);
		sLogger.info(" - AverageDocumentLength: " + avgdl);

		env.writeCollectionLength(collectionLength);
		env.writeCollectionDocumentCount(docCount);
		env.writeCollectionAverageDocumentLength(avgdl);

		// preserve old postings
		Path postingsPath1 = new Path(indexPath + "/postings/");
		Path postingsPath2 = new Path(indexPath + "/postings.old/");

		if (fs.exists(postingsPath1)) {
			sLogger.info("renaming " + postingsPath1.getName() + " to " + postingsPath2.getName());
			fs.rename(postingsPath1, postingsPath2);
		}

		conf.setJobName("DistributeGlobalStatsToPostings:" + collectionName);

		FileInputFormat.setInputPaths(conf, postingsPath2);
		FileOutputFormat.setOutputPath(conf, postingsPath1);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		DistributedCache.addCacheFile(new URI(statsPath + "/dict.terms"), conf);
		DistributedCache.addCacheFile(new URI(statsPath + "/dict.df"), conf);
		DistributedCache.addCacheFile(new URI(statsPath + "/dict.cf"), conf);

		DistributedCache.addCacheFile(new URI(env.getIndexTermsData()), conf);
		DistributedCache.addCacheFile(new URI(env.getIndexTermIdsData()), conf);
		DistributedCache.addCacheFile(new URI(env.getIndexTermIdMappingData()), conf);

		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.setInt("mapred.map.max.attempts", 10);
		conf.setInt("mapred.reduce.max.attempts", 10);
		conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(PostingsListDocSortedPositional.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("usage: [global-stats] [index-path]");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		String gsPath = args[0];
		String indexPath = args[1];

		conf.set("Ivory.IndexPath", indexPath);
		conf.set("Ivory.GlobalStatsPath", gsPath);
		conf.setInt("Ivory.NumMapTasks", 100);

		sLogger.info("Distributing global statistics to " + indexPath);
		new DistributeGlobalStatsToPostings(conf).run();
	}
}
