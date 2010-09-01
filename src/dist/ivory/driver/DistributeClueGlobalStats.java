package ivory.driver;

import ivory.index.BuildPostingsForwardIndex;
import ivory.index.ComputeDictionarySize;
import ivory.index.DistributeGlobalStatsToPostings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.FSProperty;

public class DistributeClueGlobalStats {

	private static final Logger sLogger = Logger.getLogger(DistributeClueGlobalStats.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String indexPath = "/umd/indexes/clue.en.global.segment.01/";

		String gsPath = "/umd/indexes/clue.en.global.stats.df10/";
		String prefixEncodedTermsFile = gsPath + "dict.terms";
		String dfStatsPath = gsPath + "dict.df";
		String cfStatsPath = gsPath + "dict.cf";

		conf.set("Ivory.IndexPath", indexPath);
		conf.set("Ivory.PrefixEncodedTermsFile", prefixEncodedTermsFile);
		conf.set("Ivory.DFStatsFile", dfStatsPath);
		conf.set("Ivory.CFStatsFile", cfStatsPath);
		conf.setInt("Ivory.NumMapTasks", 100);
		conf.setInt("Ivory.NumReduceTasks", 100);

		DistributeGlobalStatsToPostings tool = new DistributeGlobalStatsToPostings(conf);
		tool.run();

		int dfThreshold = 100;

		conf.setInt("Ivory.DfThreshold", dfThreshold);

		ComputeDictionarySize dictTool = new ComputeDictionarySize(conf);
		int n = dictTool.run();

		sLogger.info("Building terms to postings forward index with " + n + " terms");

		conf.setInt("Ivory.IndexNumberOfTerms", n);
		conf.setInt("Ivory.ForwardIndexWindow", 8);

		BuildPostingsForwardIndex postingsTool = new BuildPostingsForwardIndex(conf);
		postingsTool.run();

		FileSystem fs = FileSystem.get(conf);

		int collectionDocCnt = FSProperty.readInt(fs, gsPath + "/property.CollectionDocumentCount");
		long collectionTermCnt = FSProperty.readLong(fs, gsPath + "/property.CollectionTermCount");

		fs.rename(new Path(indexPath + "/property.CollectionDocumentCount"), new Path(indexPath
				+ "/property.CollectionDocumentCount.local"));
		fs.rename(new Path(indexPath + "/property.CollectionTermCount"), new Path(indexPath
				+ "/property.CollectionTermCount.local"));

		FSProperty.writeInt(fs, indexPath + "/property.CollectionDocumentCount", collectionDocCnt);
		FSProperty.writeLong(fs, indexPath + "/property.CollectionTermCount", collectionTermCnt);
	}
}
