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

package ivory.core.data.dictionary;

import ivory.util.RetrievalEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DefaultFrequencySortedDictionary {

	private PrefixEncodedLexicographicallySortedDictionary mDictionary = new PrefixEncodedLexicographicallySortedDictionary();
	private int[] mIds;
	private int[] mIdToTerm;

	public DefaultFrequencySortedDictionary(Path prefixSetPath, Path idsPath, Path idToTermPath, FileSystem fileSys)
			throws IOException {
		FSDataInputStream termsInput, idsInput, idToTermInput;

		termsInput = fileSys.open(prefixSetPath);
		mDictionary.readFields(termsInput);
		termsInput.close();

		int l = 0;

		idsInput = fileSys.open(idsPath);
		l = idsInput.readInt();
		mIds = new int[l];
		for (int i = 0; i < l; i++)
			mIds[i] = idsInput.readInt();
		idsInput.close();

		idToTermInput = fileSys.open(idToTermPath);
		l = idToTermInput.readInt();
		mIdToTerm = new int[l];
		for (int i = 0; i < l; i++)
			mIdToTerm[i] = idToTermInput.readInt();
		idToTermInput.close();
	}

	public void close() throws IOException {
	}

	public int getVocabularySize() {
		return mIds.length;
	}

	public int getID(String term) {
		int index = mDictionary.getIndex(term);

		if (index < 0)
			return -1;

		return mIds[index];
	}

	public String getTerm(int id) {
		if (id > mIds.length || id == 0 || mIdToTerm == null)
			return null;
		String term = mDictionary.getKey(mIdToTerm[id - 1]);

		return term;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: [index-path]");
			System.exit(-1);
		}

		String indexPath = args[0];

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		Path termsFilePath = new Path(env.getIndexTermsData());
		Path termIDsFilePath = new Path(env.getIndexTermIdsData());
		Path idToTermFilePath = new Path(env.getIndexTermIdMappingData());

		DefaultFrequencySortedDictionary termIDMap = new DefaultFrequencySortedDictionary(termsFilePath, termIDsFilePath, idToTermFilePath, fs);

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
