package ivory.index;

import ivory.data.PostingsList;
import ivory.data.PrefixEncodedGlobalStats;
import ivory.util.RetrievalEnvironment;
import edu.umd.cloud9.io.PairOfIntLong;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.PowerTool;

public class DistributeGlobalStatsToPostings extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(DistributeGlobalStatsToPostings.class);

	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, PostingsList, Text, PostingsList> {

		private PrefixEncodedGlobalStats gs;

		public void configure(JobConf job) {
			try {
				Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
				
				sLogger.info("0: " + localFiles[0]);
				sLogger.info("1: " + localFiles[1]);
				sLogger.info("2: " + localFiles[2]);
				
				FileSystem fs = FileSystem.getLocal(job);
				
				gs = new PrefixEncodedGlobalStats(localFiles[0], fs);
				gs.loadDFStats(localFiles[1], fs);
				gs.loadCFStats(localFiles[2], fs);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Error loading global term stats!");
			}
		}

		public void map(Text key, PostingsList p, OutputCollector<Text, PostingsList> output,
				Reporter reporter) throws IOException {
			String term = key.toString();

			//sLogger.info("old: term: " + key + ", cf=" + p.getCf() + ", df=" + p.getDf());
			//sLogger.info("new: term: " + key + ", cf=" + gs.getCF(term) + ", df=" + gs.getDF(term));
			
			PairOfIntLong pair = gs.getStats(term);
			
			if ( pair == null) {
				p.setCf(-1);
				p.setDf(-1);
			} else {
				p.setCf(pair.getRightElement());
				p.setDf(pair.getLeftElement());
			}
			/*
			p.setCf(gs.getCF(term));
			p.setDf(gs.getDF(term));
			*/

			output.collect(key, p);
		}
	}

	public static final String[] RequiredParameters = { "Ivory.IndexPath",
			"Ivory.PrefixEncodedTermsFile", "Ivory.DFStatsFile", "Ivory.CFStatsFile",
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
		int reduceTasks = conf.getInt("Ivory.NumMapTasks", 1);

		String indexPath = conf.get("Ivory.IndexPath");
		String collectionName = RetrievalEnvironment.readCollectionName(fs, indexPath);

		sLogger.info("Characteristics of the collection:");
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - IndexPath: " + indexPath);
		sLogger.info("Characteristics of the job:");
		sLogger.info(" - NumMapTasks: " + mapTasks);
		sLogger.info(" - NumReduceTasks: " + reduceTasks);

		Path postingsOld = new Path(indexPath + "/postings.old/");
		Path postingsCur = new Path(indexPath + "/postings/");
		
		fs.rename(postingsCur, postingsOld);

		conf.setJobName("DistributeGlobalStatsToPostings:" + collectionName);

		FileInputFormat.setInputPaths(conf, postingsOld);
		FileOutputFormat.setOutputPath(conf, postingsCur);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		
		DistributedCache.addCacheFile(new URI(conf.get("Ivory.PrefixEncodedTermsFile")), conf);
		DistributedCache.addCacheFile(new URI(conf.get("Ivory.DFStatsFile")), conf);
		DistributedCache.addCacheFile(new URI(conf.get("Ivory.CFStatsFile")), conf);

		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.setInt("mapred.map.max.attempts", 10);
		conf.setInt("mapred.reduce.max.attempts", 10);

		String postingsType = RetrievalEnvironment.readPostingsType(fs, indexPath);
		Class<PostingsList> postingsClass = (Class<PostingsList>) Class.forName(postingsType);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(postingsClass);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		JobClient.runJob(conf);

		return 0;
	}
}
