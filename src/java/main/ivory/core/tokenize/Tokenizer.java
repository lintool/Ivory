/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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

package ivory.core.tokenize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public abstract class Tokenizer {
	public abstract void configure(Configuration conf);
	public abstract String[] processContent(String text);
		
	/**
	 * Method to return number of tokens in text. Subclasses may override for more efficient implementations.
	 * 
	 * @param text
	 * 		text to be processed.
	 * @return
	 * 		number of tokens in text.
	 */
	public int getNumberTokens(String text){
		return processContent(text).length;
	}
	
	/**
	 * Method to remove non-unicode characters from token, to prevent errors in the preprocessing pipeline. Such cases exist in German Wikipedia. 
	 * 
	 * @param token
	 * 		token to check for non-unicode character
	 * @return
	 * 		token without the non-unicode characters
	 */
	public String removeNonUnicodeChars(String token) {
		StringBuffer fixedToken = new StringBuffer();
		for(int i=0; i<token.length(); i++){
			char c = token.charAt(i);
			if(Character.getNumericValue(c)>=0){
				fixedToken.append(c);
			}
		}
		return fixedToken.toString();
	}
	
	public boolean isStopWord(String eTerm) {
		return false;
	}

	public abstract void configure(Configuration mJobConf, FileSystem fs);
}
