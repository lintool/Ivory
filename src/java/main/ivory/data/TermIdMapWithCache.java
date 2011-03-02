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

import ivory.util.RetrievalEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umd.cloud9.util.map.HMapKI;

public class TermIdMapWithCache extends TermIdMap {

	HMapKI<String> cache = null;

	public TermIdMapWithCache(Path prefixSetPath, Path idsPath, Path idToTermPath,
			int cachedFrequent, FileSystem fileSys) throws IOException {
		super(prefixSetPath, idsPath, idToTermPath, fileSys);
		loadFrequentMap(cachedFrequent);
	}

	public TermIdMapWithCache(Path prefixSetPath, Path idsPath, Path idToTermPath,
			float cachedFrequentPercent, FileSystem fileSys) throws IOException {
		super(prefixSetPath, idsPath, idToTermPath, fileSys);

		if (cachedFrequentPercent < 0 || cachedFrequentPercent > 1.0)
			return;

		int cachedFrequent = (int) (cachedFrequentPercent * getVocabularySize());
		loadFrequentMap(cachedFrequent);
	}

	private void loadFrequentMap(int n) {
		if (getVocabularySize() < n) {
			n = getVocabularySize();
		}

		cache = new HMapKI<String>(n);

		for (int id = 1; id <= n; id++) {
			cache.put(getTerm(id), id);
		}
	}

	public int getID(String term) {
		if (cache != null && cache.containsKey(term)) {
				return cache.get(term);
		}

		return super.getID(term);
	}	
	
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: [index-path]");
			System.exit(-1);
		}

		String indexPath = args[0];

		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);

		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fileSys);

		Path termsFilePath = new Path(env.getIndexTermsData());
		Path termIDsFilePath = new Path(env.getIndexTermIdsData());
		Path idToTermFilePath = new Path(env.getIndexTermIdMappingData());

		TermIdMapWithCache termIDMap = new TermIdMapWithCache(termsFilePath, termIDsFilePath,
				idToTermFilePath, 100, fileSys);

		int nTerms = termIDMap.getVocabularySize();
		System.out.println("nTerms: " + nTerms);

		System.out.println(" \"term word\" to lookup termid; \"termid 234\" to lookup term");
		String cmd = null;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("lookup > ");
		while ((cmd = stdin.readLine()) != null) {

			String[] tokens = cmd.split("\\s+");

			if (tokens.length != 2) {
				System.out.println("Error: unrecognized command!");
				System.out.print("lookup > ");

				continue;
			}

			if (tokens[0].equals("termid")) {
				int termid;
				try {
					termid = Integer.parseInt(tokens[1]);
				} catch (Exception e) {
					System.out.println("Error: invalid termid!");
					System.out.print("lookup > ");

					continue;
				}

				System.out.println("termid=" + termid + ", term=" + termIDMap.getTerm(termid));
			} else if (tokens[0].equals("term")) {
				String term = tokens[1];

				System.out.println("term=" + term + ", termid=" + termIDMap.getID(term));
			} else {
				System.out.println("Error: unrecognized command!");
				System.out.print("lookup > ");
				continue;
			}

			System.out.print("lookup > ");
		}
	}
}
