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

package ivory.util;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * Wrapper around the English Truncator class in Hadoop-Aligner.
 * 
 * @author ferhanture
 *
 */
public class WikipediaEnglishTokenizer implements Tokenizer {
	GalagoTokenizer t;

	public WikipediaEnglishTokenizer(){
		t = new GalagoTokenizer();
	}

	public String[] processContent(String text) {
		return preprocessWordsImpl(WikipediaPage.parseAndCleanPage(text).trim());
	}

	protected String[] preprocessWordsImpl(String text) {
		int length = 4;
		String delims =
            " \t\n\r\f`~!@#$%^&*()-_=+]}[{\\|'\";:/?.>,<";
		StringTokenizer st = new StringTokenizer(text,delims);
		
		List<String> res = new ArrayList<String>();
		while(st.hasMoreTokens()) {
			final String cur = st.nextToken().toLowerCase();
			if(!t.isStopWord(cur)){
				int l = length;
				if (cur.startsWith("con"))
					l+=2;
				else if (cur.startsWith("intra"))
					l+=4;
				else if (cur.startsWith("pro"))
					l+=2;
				else if (cur.startsWith("anti"))
					l+=3;
				else if (cur.startsWith("inter"))
					l+=4;
				else if (cur.startsWith("in"))
					l+=2;
				else if (cur.startsWith("im"))
					l+=2;
				else if (cur.startsWith("re"))
					l+=2;
				else if (cur.startsWith("de"))
					l+=1;
				else if (cur.startsWith("pre"))
					l+=2;
				else if (cur.startsWith("un"))
					l+=2;
				else if (cur.startsWith("co"))
					l+=2;
				else if (cur.startsWith("qu"))
					l+=1;
				else if (cur.startsWith("ad"))
					l+=1;
				else if (cur.startsWith("en"))
					l+=2;
				else if (cur.startsWith("al-"))
					l+=2;
				else if (cur.startsWith("sim"))
					l+=2;
				else if (cur.startsWith("sym"))
					l+=2;
				if (cur.length() < l) l = cur.length();
				res.add(cur.substring(0, l));
			}
		}
		String[] result = new String[res.size()];
		int cnt2 = 0;
		for(String s : res){
			result[cnt2++] = s;
		}
		return result;
	}

	public void configure(Configuration conf) {
	}
	
	public static void main(String[] args){
		String text = " this is a the for the score excess figure <complicated 	word> !---ferhanture";

		Tokenizer tokenizer;
		String[] tokens;

		System.out.println("tokenization according to WikiTokenizer: ");
		tokenizer = new WikipediaEnglishTokenizer();
		tokens = tokenizer.processContent(text);
		for(String t : tokens)	System.out.println(t);


		System.out.println("\n\ntokenization according to Lucene: ");
		tokenizer = new LuceneTokenizer();
		tokens = tokenizer.processContent(text);
		for(String t : tokens)	System.out.println(t);
		
		System.out.println("\n\ntokenization according to Galago: ");
		tokenizer = new GalagoTokenizer();
		tokens = tokenizer.processContent(text);
		for(String t : tokens)	System.out.println(t);
	}
}
