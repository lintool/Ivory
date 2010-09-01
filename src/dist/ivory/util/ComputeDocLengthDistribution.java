package ivory.util;

import ivory.data.DocLengthTable4B;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import edu.umd.cloud9.util.Histogram;
import edu.umd.cloud9.util.MapKI;

public class ComputeDocLengthDistribution {

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("usage: [index-path]");
			System.exit(-1);
		}

		String indexPath = args[0];
		FileSystem fs = FileSystem.get(new Configuration());
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		DocLengthTable4B doclen = new DocLengthTable4B(env.getDoclengthsData(), fs);
		Histogram<Integer> h = new Histogram<Integer>();

		int cnt = 0;
		for (int docno = doclen.getDocnoOffset() + 1; docno < doclen.getDocCount() + 1; docno++) {
			cnt++;
			int len = doclen.getDocLength(docno);
			// System.out.println(docno + "\t" + len);
			h.count(len);
		}

		for (MapKI.Entry<Integer> e : h.getEntriesSortedByKey()) {
			System.out.println(e.getKey() + "\t" + e.getValue());
		}

	}

}
