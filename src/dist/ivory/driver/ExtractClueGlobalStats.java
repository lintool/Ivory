package ivory.driver;

import ivory.index.ComputeDistributedDictionarySize;
import ivory.index.ExtractGlobalStatsFromPostings;
import ivory.util.RetrievalEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.FSProperty;

public class ExtractClueGlobalStats {

	private static final Logger sLogger = Logger.getLogger(ExtractClueGlobalStats.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] indexes = { "/umd/indexes/clue.en.segment.01.stable/",
				"/umd/indexes/clue.en.segment.02.stable/",
				"/umd/indexes/clue.en.segment.03.stable/",
				"/umd/indexes/clue.en.segment.04.stable/",
				"/umd/indexes/clue.en.segment.05.stable/",
				"/umd/indexes/clue.en.segment.06.stable/",
				"/umd/indexes/clue.en.segment.07.stable/",
				"/umd/indexes/clue.en.segment.08.stable/",
				"/umd/indexes/clue.en.segment.09.stable/",
				"/umd/indexes/clue.en.segment.10.stable/" };

		String paths = "";
		for (int i = 0; i < indexes.length; i++) {
			if (i != 0)
				paths += ",";

			paths += indexes[i] + "postings";
		}

		conf.set("Ivory.IndexPath", paths);
		String collectionName = "clue.en.all";

		String outputPath = "/umd/indexes/clue.en.global.stats.df10";
		String dictSizePath = outputPath + "/dict.size";

		conf.set("Ivory.CollectionName", collectionName);
		conf.set("Ivory.DictSizePath", dictSizePath);
		conf.setInt("Ivory.DfThreshold", 10);
		conf.setInt("Ivory.NumMapTasks", 200);

		ComputeDistributedDictionarySize dictSizeTask = new ComputeDistributedDictionarySize(conf);
		dictSizeTask.run();

		int nTerms = FSProperty.readInt(FileSystem.get(conf), dictSizePath);
		sLogger.info("nTerms = " + nTerms);

		String prefixEncodedTermsFile = outputPath + "/dict.terms";
		String dfStatsPath = outputPath + "/dict.df";
		String cfStatsPath = outputPath + "/dict.cf";
		String tmpPath = outputPath + "/tmp";

		conf.set("Ivory.TmpPath", tmpPath);
		conf.setInt("Ivory.IndexNumberOfTerms", nTerms);
		conf.setInt("Ivory.ForwardIndexWindow", 8);
		conf.set("Ivory.PrefixEncodedTermsFile", prefixEncodedTermsFile);
		conf.set("Ivory.DFStatsFile", dfStatsPath);
		conf.set("Ivory.CFStatsFile", cfStatsPath);

		ExtractGlobalStatsFromPostings task = new ExtractGlobalStatsFromPostings(conf);
		task.runTool();

		FileSystem fs = FileSystem.get(conf);

		int docCnt = 0;
		long termCnt = 0;
		for (String index : indexes) {
			docCnt += RetrievalEnvironment.readCollectionDocumentCount(fs, index);
			termCnt += RetrievalEnvironment.readCollectionTermCount(fs, index);
		}

		sLogger.info("collection doc count: " + docCnt);
		sLogger.info("collection term count: " + termCnt);

		FSProperty.writeInt(fs, outputPath + "/property.CollectionDocumentCount", docCnt);
		FSProperty.writeLong(fs, outputPath + "/property.CollectionTermCount", termCnt);
	}
}
