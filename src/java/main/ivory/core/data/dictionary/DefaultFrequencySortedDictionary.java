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


import ivory.core.RetrievalEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class DefaultFrequencySortedDictionary implements FrequencySortedDictionary {
	private PrefixEncodedLexicographicallySortedDictionary dictionary =
	    new PrefixEncodedLexicographicallySortedDictionary();
	private int[] ids;
	private int[] idsToTerm;

	public DefaultFrequencySortedDictionary(Path prefixSetPath, Path idsPath, Path idToTermPath,
	    FileSystem fs) throws IOException {
		FSDataInputStream in;

		in = fs.open(prefixSetPath);
		dictionary.readFields(in);
		in.close();

		int l = 0;

		in = fs.open(idsPath);
		l = in.readInt();
		ids = new int[l];
		for (int i = 0; i < l; i++) {
			ids[i] = in.readInt();
		}
		in.close();

		in = fs.open(idToTermPath);
		l = in.readInt();
		idsToTerm = new int[l];
		for (int i = 0; i < l; i++) {
			idsToTerm[i] = in.readInt();
		}
		in.close();
	}

	@Override
	public int size() {
		return ids.length;
	}

	@Override
	public int getId(String term) {
		int index = dictionary.getId(term);

		if (index < 0) {
			return -1;
		}

		return ids[index];
	}

	@Override
	public String getTerm(int id) {
		if (id > ids.length || id == 0 || idsToTerm == null) {
			return null;
		}
		String term = dictionary.getTerm(idsToTerm[id - 1]);

		return term;
	}

  @Override
  public Iterator<String> iterator() {
    return new Iterator<String>() {
      private int cur = 1;
      final private int end = dictionary.size();

      @Override
      public boolean hasNext() {
        return cur < end+1;
      }

      @Override
      public String next() {
        return getTerm(cur++);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
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

		DefaultFrequencySortedDictionary dictionary =
		    new DefaultFrequencySortedDictionary(termsFilePath, termIDsFilePath, idToTermFilePath, fs);

		int nTerms = dictionary.size();
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

				System.out.println("termid=" + termid + ", term=" + dictionary.getTerm(termid));
			} else if (tokens[0].equals("term")) {
				String term = tokens[1];

				System.out.println("term=" + term + ", termid=" + dictionary.getId(term));
			} else {
				System.out.println("Error: unrecognized command!");
				System.out.print("lookup > ");
				continue;
			}

			System.out.print("lookup > ");
		}
	}
}
