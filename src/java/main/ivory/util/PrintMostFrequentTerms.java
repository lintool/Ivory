package ivory.util;

import ivory.core.data.dictionary.DefaultFrequencySortedDictionary;
import ivory.data.DfTableArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PrintMostFrequentTerms {
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: [index-path]");
			System.exit(-1);
		}

		String indexPath = args[0];

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		DfTableArray dfs = new DfTableArray(new Path(env.getDfByIntData()), fs);

		Path termsFilePath = new Path(env.getIndexTermsData());
		Path termIDsFilePath = new Path(env.getIndexTermIdsData());
		Path idToTermFilePath = new Path(env.getIndexTermIdMappingData());

		DefaultFrequencySortedDictionary termIDMap = new DefaultFrequencySortedDictionary(termsFilePath, termIDsFilePath, idToTermFilePath, fs);

		for (int i=1; i<=200; i++) {
			System.out.println(String.format("%d\t%s\t%d", i, termIDMap.getTerm(i), dfs.getDf(i)));
		}
	}
}
