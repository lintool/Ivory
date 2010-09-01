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

package ivory.data;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.PairOfIntLong;

public class PrefixEncodedGlobalStats {

	/**
	 * logger
	 */
	private static final Logger LOGGER = Logger.getLogger(PrefixEncodedGlobalStats.class);

	Configuration conf = new Configuration();
	FileSystem fileSys = FileSystem.get(conf);
	PrefixEncodedTermSet prefixSet = new PrefixEncodedTermSet();
	int[] df = null;
	long[] cf = null;

	FSDataInputStream termsInput = null;
	FSDataInputStream dfStatsInput = null;
	FSDataInputStream cfStatsInput = null;

	public PrefixEncodedGlobalStats(Path prefixSetPath) throws IOException {
		termsInput = fileSys.open(prefixSetPath);

		prefixSet.readFields(termsInput);
		termsInput.close();
	}

	public PrefixEncodedGlobalStats(Path prefixSetPath, FileSystem fs) throws IOException {
		fileSys = fs;
		termsInput = fileSys.open(prefixSetPath);

		prefixSet.readFields(termsInput);
		termsInput.close();
	}

	public void loadDFStats(Path dfStatsPath) throws IOException {
		loadDFStats(dfStatsPath, fileSys);
	}
	
	public void loadDFStats(Path dfStatsPath, FileSystem fs) throws IOException {
		dfStatsInput = fs.open(dfStatsPath);

		int l = dfStatsInput.readInt();
		if (l != prefixSet.length()) {
			throw new RuntimeException("df length mismatch: " + l + "\t" + prefixSet.length());
		}
		df = new int[l];
		for (int i = 0; i < l; i++)
			df[i] = dfStatsInput.readInt();
		dfStatsInput.close();
	}

	public void loadCFStats(Path cfStatsPath) throws IOException {
		loadCFStats(cfStatsPath, fileSys);
	}
	
	public void loadCFStats(Path cfStatsPath, FileSystem fs) throws IOException {
		cfStatsInput = fs.open(cfStatsPath);

		int l = cfStatsInput.readInt();
		if (l != prefixSet.length()) {
			throw new RuntimeException("cf length mismatch: " + l + "\t" + prefixSet.length());
		}
		cf = new long[l];
		for (int i = 0; i < l; i++)
			cf[i] = cfStatsInput.readLong();
		cfStatsInput.close();
	}

	public int getDF(String term) {
		if(df == null) 
			throw new RuntimeException("DF-Stats must be loaded first!");
		int index = prefixSet.getIndex(term);
		LOGGER.info("index of " + term + ": " + index);
		if (index < 0)
			return -1;
		return df[index];
	}

	public long getCF(String term) {
		if(cf == null) 
			throw new RuntimeException("CF-Stats must be loaded first!");
		int index = prefixSet.getIndex(term);
		LOGGER.info("index of " + term + ": " + index);
		if (index < 0)
			return -1;
		return cf[index];
	}

	public PairOfIntLong getStats(String term) {
		int index = prefixSet.getIndex(term);
		LOGGER.info("index of " + term + ": " + index);
		if (index < 0)
			return null;
		PairOfIntLong p = new PairOfIntLong();
		p.set(df[index], cf[index]);
		return p;
	}

	public int length() {
		return prefixSet.length();
	}

	public void printKeys() {
		System.out.println("Window: " + this.prefixSet.getWindow());
		System.out.println("Length: " + this.length());
		// int window = prefixSet.getWindow();
		for (int i = 0; i < length() && i < 100; i++) {
			System.out.print(i + "\t" + prefixSet.getKey(i));
			if (df != null)
				System.out.print("\t" + df[i]);
			if (cf != null)
				System.out.print("\t" + cf[i]);
			System.out.println();
		}
	}

	/*
	 * public void printPrefixSetContent(){ prefixSet.printCompressedKeys();
	 * prefixSet.printKeys(); }
	 */
}
