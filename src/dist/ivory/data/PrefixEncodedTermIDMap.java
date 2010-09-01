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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PrefixEncodedTermIDMap{

	/**
	 * logger
	 */
	private static final Logger LOGGER = Logger.getLogger(PrefixEncodedTermIDMap.class);

	static {
		LOGGER.setLevel(Level.WARN);
	}

	Configuration conf = new Configuration();
	//FileSystem fileSys = FileSystem.get(conf);
	PrefixEncodedTermSet prefixSet = new PrefixEncodedTermSet();
	int[] ids;
	
	/*public PrefixEncodedTermSet getDictionary(){
		return prefixSet;
	}*/
	public PrefixEncodedTermIDMap(Path prefixSetPath , Path idsPath, FileSystem fileSys) throws IOException{
		init(prefixSetPath, idsPath, fileSys);
	}

	private void init(Path prefixSetPath , Path idsPath, FileSystem fileSys)throws IOException{
		FSDataInputStream termsInput;
		FSDataInputStream idsInput;

		termsInput = fileSys.open(prefixSetPath);
		idsInput = fileSys.open(idsPath);

		prefixSet.readFields(termsInput);
		LOGGER.info("Loading positions ...");
		int l = idsInput.readInt();
		ids= new int[l];
		for(int i = 0 ; i<l; i++) ids[i] = idsInput.readInt();
		LOGGER.info("Loading done.");
		termsInput.close();
		idsInput.close();
	}

	public void close() throws IOException {
		
	}
	
	public int getNumOfTerms(){
		return ids.length;
	}

	public int getID(String term){
		int index = prefixSet.getIndex(term);
		
		if(index < 0) return -1;
		LOGGER.info("id of "+term+": "+ids[index]);
		return ids[index];
	}
	
}
