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
 * Wrapper around the German Truncator class in Hadoop-Aligner.
 * 
 * @author ferhanture
 *
 */
public class WikipediaGermanTokenizer implements Tokenizer {

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
			final String cur = st.nextToken().toLowerCase().replaceAll("sch", "S");
			int l = length;
			int s = 0;
			if (cur.startsWith("gegen"))
				l+=5;
			else if (cur.startsWith("zusammen"))
				l+=8;
			else if (cur.startsWith("zuge"))
				l+=4;
			else if (cur.startsWith("einge"))
				l+=5;
			else if (cur.startsWith("aufge"))
				l+=5;
			else if (cur.startsWith("ausge"))
				l+=5;
			else if (cur.startsWith("hinge"))
				l+=5;
			else if (cur.startsWith("herge"))
				l+=5;
			else if (cur.startsWith("ein"))
				l+=3;
			else if (cur.startsWith("zer"))
				l+=2;
			else if (cur.startsWith("ver"))
				l+=3;
			else if (cur.startsWith("ent"))
				l+=2;
			else if (cur.startsWith("auf"))
				l+=3;
			else if (cur.startsWith("aus"))
				l+=3;
			else if (cur.startsWith("abge"))
				l+=4;
			else if (cur.startsWith("bei"))
				l+=3;
			else if (cur.startsWith("voran"))
				l+=5;
			else if (cur.startsWith("vor"))
				l+=3;
			else if (cur.startsWith("mit"))
				l+=3;
			else if (cur.startsWith("ab"))
				l+=2;
			else if (cur.startsWith("be"))
				l+=1;
			else if (cur.startsWith("Ÿber"))
				l+=4;
			else if (cur.startsWith("unter"))
				l+=5;
			else if (cur.startsWith("ge"))
				s+=2;
			else if (cur.startsWith("er"))
				l+=1;
			else if (cur.startsWith("zu"))
				l+=2;
			else if (cur.startsWith("ange"))
				l+=3;
			else if (cur.startsWith("an"))
				l+=2;
			else if (cur.startsWith("durch"))
				l+=5;
			else if (cur.startsWith("nieder"))
				l+=5;
			else if (cur.startsWith("dar"))
				l+=2;
			if (s >= cur.length()) s=0;
			if (cur.length() < (s+l)) l = cur.length() - s;
			res.add(cur.substring(0, l));
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
}
