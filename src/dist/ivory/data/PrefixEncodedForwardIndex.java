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

import ivory.index.BuildPostingsForwardIndex;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

public class PrefixEncodedForwardIndex{

	Configuration conf = new Configuration();
	FileSystem fileSys = FileSystem.get(conf);
	PrefixEncodedTermSet prefixSet = new PrefixEncodedTermSet();
	long[] positions;
	FSDataInputStream termsInput;
	FSDataInputStream posInput;
	String postingsPath;
	
	public PrefixEncodedForwardIndex(Path prefixSetPath , Path positionsPath, String postingsPath) throws IOException{
		this.postingsPath = postingsPath;
		termsInput = fileSys.open(prefixSetPath);
		posInput = fileSys.open(positionsPath);

		prefixSet.readFields(termsInput);
		//System.out.println("Loading positions ...");
		int l = posInput.readInt();
		positions= new long[l];
		for(int i = 0 ; i<l; i++) positions[i] = posInput.readLong();
		//System.out.println("Loading done.");
	}
	
	public void close() throws IOException{
		termsInput.close();
		posInput.close();
	}
	
	private long getPos(String term){
		int index = prefixSet.getIndex(term);
		//System.out.println("index of "+term+": "+index);
		if(index < 0) return -1;
		return positions[index];
	}
	
	public long getTermPosting(String term, Text key, PostingsList value) throws IOException{
		long origPos = getPos(term);
		long pos = origPos;
		if(pos<0) return -1; 
		int fileNo = (int)(pos*1.0f / BuildPostingsForwardIndex.BIG_LONG_NUMBER);
		pos = pos % BuildPostingsForwardIndex.BIG_LONG_NUMBER;

		String fileNoStr = fileNo+"";
		String padd="";
		for(int i=5; i>fileNoStr.length(); i--) padd+="0";
		fileNoStr=padd+fileNoStr;

		// open up the SequenceFile
		
		//System.out.println("fileNoSte: "+fileNoStr);
		//System.out.println("file pos: "+pos);
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, new Path(postingsPath+"/part-"+fileNoStr), conf);

		reader.seek(pos);
		reader.next(key, value);
		reader.close();
		 
		return origPos;
	}
	
	public int length(){
		return prefixSet.length();
	}
	
	public void printKeys(){
		System.out.println("Window: "+this.prefixSet.getWindow());
		System.out.println("Length: "+this.length());
		//int window = prefixSet.getWindow();
		for(int i = 0 ; i<length() && i<100; i++){
			long pos = positions[i];
			System.out.println(i+"\t"+prefixSet.getKey(i)+"\t"+positions[i]);
			
			int fileNo = (int)(pos*1.0f / BuildPostingsForwardIndex.BIG_LONG_NUMBER);
			pos = pos % BuildPostingsForwardIndex.BIG_LONG_NUMBER;
			System.out.println("\t\t\t"+fileNo+"\t"+pos);
			
			
		}

	}
	
	
	/*public void printPrefixSetContent(){
		prefixSet.printCompressedKeys();
		prefixSet.printKeys();
	}*/
}
