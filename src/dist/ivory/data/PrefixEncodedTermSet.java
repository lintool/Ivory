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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class PrefixEncodedTermSet implements Writable {

	/**
	 * logger
	 */
	private static final Logger LOGGER = Logger.getLogger(PrefixEncodedTermSet.class);
	
	public long totalOriginalBytes = 0;
	public long totalMemoryBytes = 0;

	private static final int DEFAULT_INITIAL_SIZE=10000000;
	private String lastKey = "";

	private int curKeyIndex = 0;
	private byte[][] keys; 
	//private int[] values;

	private int window = 4;
	float resizeFactor = 1; // >0 and <=1

	public int getWindow(){
		return window; 
	}
	
	public int length(){
		return curKeyIndex;
	}
	
	public PrefixEncodedTermSet(){
		
	}
	public PrefixEncodedTermSet(int indexWindow){
		keys = new byte[DEFAULT_INITIAL_SIZE][];
		//values = new int[DEFAULT_INITIAL_SIZE];
		window = indexWindow;
	}

	public PrefixEncodedTermSet(int initialSize, int indexWindow){
		keys = new byte[initialSize][];
		//values = new int[initialSize];
		window = indexWindow;
	}

	public PrefixEncodedTermSet(int initialSize, float resizeF, int indexWindow){
		keys = new byte[initialSize][];
		//values = new int[initialSize];
		window = indexWindow;
		resizeFactor = resizeF;
	}

	public void readFields(DataInput in) throws IOException {
		curKeyIndex = in.readInt();
		//System.out.println("Loading index ...");
		//System.out.println("nTerms: "+curKeyIndex);
		keys = new byte[curKeyIndex][];
		window = in.readInt();
		//System.out.println("Window: "+window);
		for(int i = 0; i < curKeyIndex; i++){
			//System.out.print(i+"\t");
			if(i % window != 0){ // not a root
				int suffix = in.readByte();
				if(suffix<0) suffix+=256;
				//System.out.println(suffix);
				keys[i] = new byte[suffix+1];
				keys[i][0] = in.readByte();
				for(int j = 1; j<keys[i].length; j++) keys[i][j] = in.readByte();
			}
			else{ // root
				int suffix = in.readByte();
				if(suffix<0) suffix+=256;
				//System.out.println(suffix);
				keys[i] = new byte[suffix];
				for(int j = 0; j<keys[i].length; j++) keys[i][j] = in.readByte();
			}
		}
		//System.out.println("Loading done.");
	}

	/*public void readFields(DataInput in) throws IOException {

		keys = new byte[DEFAULT_INITIAL_SIZE][];
		window = in.readInt();
		curKeyIndex = 0;
		while(true){

			try{
				if(curKeyIndex % window != 0){ // not a root
					int suffix = in.readByte();
					if(suffix<0) suffix+=256;
					keys[curKeyIndex] = new byte[suffix+1];
					keys[curKeyIndex][0] = in.readByte();
					for(int j = 1; j<keys[curKeyIndex].length; j++) keys[curKeyIndex][j] = in.readByte();
				}
				else{ // root
					int suffix = in.readByte();
					if(suffix<0) suffix+=256;
					keys[curKeyIndex] = new byte[suffix];
					for(int j = 0; j<keys[curKeyIndex].length; j++) keys[curKeyIndex][j] = in.readByte();
				}
				curKeyIndex++;
				if(curKeyIndex >= keys.length){
					System.out.println("------ resizing from:" + curKeyIndex + " to " + (1+resizeFactor)*curKeyIndex);
					keys = PrefixUtils.resize2DimByteArray(keys, (int)(curKeyIndex*(1+resizeFactor)));
					//values = PrefixUtils.resizeIntArray(values, (int)(curKeyIndex*(1+resizeFactor)));
					System.out.println("------ done.");
				}
			}catch(IOException e){
				return;
			}

		}
	}*/

	public void write(DataOutput out) throws IOException {
		out.writeInt(curKeyIndex);
		out.writeInt(window);
		for(int i = 0; i < curKeyIndex; i++){
			if(i % window != 0){ // not a root
				out.writeByte((byte)(keys[i].length-1)); // suffix length
				out.writeByte(keys[i][0]); // prefix length
				for(int j = 1; j<keys[i].length; j++) out.writeByte(keys[i][j]); 
			}
			else{ // root
				out.writeByte((byte)(keys[i].length)); // suffix length
				for(int j = 0; j<keys[i].length; j++) out.writeByte(keys[i][j]);
			}
		}
		//out.writeInt(curKeyIndex);
	}

	public void put(String key, int value){
		totalOriginalBytes+=key.getBytes().length;
		int prefix;
		/*if(curKeyIndex == 186){
			int kk=0;
		}*/
		if(curKeyIndex >= keys.length){
			//System.out.println("------ resizing from:" + curKeyIndex + " to " + (1+resizeFactor)*curKeyIndex);
			keys = resize2DimByteArray(keys, (int)(curKeyIndex*(1+resizeFactor)));
			//values = PrefixUtils.resizeIntArray(values, (int)(curKeyIndex*(1+resizeFactor)));
			//System.out.println("------ done.");
		}
		byte[] byteArray = null;
		if(curKeyIndex % window == 0){
			prefix = 0;
			byteArray = key.getBytes();
		}
		else{
			prefix = getPrefix(lastKey, key);
			byte[] suffix = key.substring(prefix).getBytes();
			byteArray = new byte[suffix.length+1];
			byteArray[0] = (byte) prefix;
			for(int i=0; i<suffix.length; i++) byteArray[i+1] = suffix[i];
		}
		totalMemoryBytes+=byteArray.length;
		keys[curKeyIndex]=byteArray;
		//values[curKeyIndex]=value;

		lastKey = key;
		curKeyIndex++;
	}

	public static byte[][] resize2DimByteArray(byte[][] input, int size) {
		if (size < 0) return null;

		if (size <= input.length) return input;

		byte[][] newArray = new byte[size][];
		System.arraycopy(input, 0, newArray, 0, input.length);
		return newArray;
	}
	
	private String getString(int pos, String previousKey){
		if(pos % window == 0){
			return new String(keys[pos]);
		}
		else{
			int prefix = getPrefixLength(pos);
			return previousKey.substring(0, prefix) + getSuffixString(pos);
		}
	}

	private String getSuffixString(int pos){
		if(pos % window == 0){
			return new String(keys[pos]);
		}
		byte[] b = new byte[keys[pos].length-1];
		for(int i=0; i<keys[pos].length-1; i++) b[i]=keys[pos][i+1];
		return new String(b);
	}

	public String getKey(int pos){
		if(pos % window == 0) return new String(keys[pos]);
		int rootPos = (pos / window) * window;
		String s = getSuffixString(rootPos);
		for(int i=rootPos+1; i<=pos; i++){
			s = getString(i, s);
		}
		return s;
	}

	/*public int getValue(int pos){
		return values[pos];
	}*/

	private int getPosFromRoot(String key, int pos){
		//LOGGER.info("looking at index "+pos);
		for(int i= pos+1; i< pos + window && i<curKeyIndex ; i++){
			String s = getKey(i);
			//LOGGER.info("\tcur index: "+i+"\tcur key :"+s);
			if(s.equals(key)) return i;
		}
		return -1;
	}

	public int getIndex(String key){
		int l = 0, h = (curKeyIndex-1) / window;
		String s = getKey(h * window);
		int i = s.compareTo(key);
		if(i == 0) return h * window;
		else if(i<0)
			return getPosFromRoot(key, h*window);

		while(l<=h){
			int m = ((l+h)/2);
			s = getKey(m * window);
			i = s.compareTo(key);
			if(i == 0) return m * window;
			else if(i<0){
				int k = getPosFromRoot(key, m*window);
				if(k>=0) return k;
				l = m +  1;
			}
			else{
				int k = getPosFromRoot(key, (m-1)*window);
				if(k>=0) return k;
				h = m - 1;
			}
		}
		return -1;
	}

	public float getLexiconSavings(){
		return 100.0f*(1-(totalMemoryBytes*1.0f/totalOriginalBytes));
	}
	
	public static int getPrefix(String s1, String s2){
		int i=0;
		for(i = 0; i<s1.length() && i<s2.length(); i++){
			if(s1.charAt(i) != s2.charAt(i)) return i; 
		}
		return i;
	}

	/*public static void test1(){
		int value = 0x10F;
		System.out.println(value);
		byte[] b = PrefixUtils.intToByteArray(value);
		for(int i=0; i<4; i++) 
			System.out.println(b[i]);
		value = PrefixUtils.byteArrayToInt(b);
		System.out.println(value);
		System.out.println(PrefixUtils.intToOneByte(value));
		System.out.println("----------");
		char ch='a';
		System.out.println(ch);
		System.out.println((int)ch);
		byte[] c = PrefixUtils.charToByteArray(ch);
		for(int i=0; i<2; i++) 
			System.out.println(c[i]);
		ch = PrefixUtils.byteArrayToChar(c);
		System.out.println((int)ch);
		System.out.println(ch);
		System.out.println(PrefixUtils.intToOneByte(ch));
		System.out.println("----------");
		String s = "abc";
		byte[] sb= s.getBytes();
		for(int i=0; i<sb.length; i++) 
			System.out.println(sb[i]);
	}*/

	public int getPrefixLength(int pos){
		int prefix = keys[pos][0];
		if(prefix < 0){
			prefix+=256;
		}
		return prefix;
	}
	public void printCompressedKeys(){
		//System.out.println("Window: "+this.window);
		for(int i = 0 ; i<curKeyIndex && i<20; i++){
			//if(i%window == 0)
			//	System.out.println(i+"\t"+0+", "+getSuffixString(i));
			//else 
			//	System.out.println(i+"\t"+keys[i][0]+", "+getSuffixString(i));
		}

	}

	public void printKeys(){
		for(int i = 0 ; i<curKeyIndex && i<20; i++){
			System.out.println(i+"\t"+getKey(i));
		}

	}



	public int totalMemorySizeInBytes(){
		int s = 0;
		for(int i = 0; i<curKeyIndex; i++){
			s+=keys[i].length;
		}
		return s;
	}

	public static void test2(){
		PrefixEncodedTermSet m = new PrefixEncodedTermSet(8);
		m.put("a", 0);
		m.put("aa", 1);
		m.put("aaa", 2);
		m.put("aaaa", 3);
		m.put("aaaaaa", 4);
		m.put("aab", 5);
		m.put("aabb", 6);
		m.put("aaabcb", 7);
		m.put("aad", 8);
		m.put("abd", 9);
		m.put("abde", 10);
		m.printCompressedKeys();
		m.printKeys();

		System.out.println(m.getIndex("a"));
		System.out.println(m.getIndex("aa"));
		System.out.println(m.getIndex("aaa"));
		System.out.println(m.getIndex("aaaa"));
		System.out.println(m.getIndex("aaaaaa"));
		System.out.println(m.getIndex("aab"));
		System.out.println(m.getIndex("aabb"));
		System.out.println(m.getIndex("aaabcb"));
		System.out.println(m.getIndex("aad"));
		System.out.println(m.getIndex("abd"));
		System.out.println(m.getIndex("abde"));
	}

	public static void test3(){
		String fname ="c:/cygwin/home/Tamer/part-00000";
		PrefixEncodedTermSet m = new PrefixEncodedTermSet(4);
		try {
			FileReader fileReader = new FileReader(fname);
			BufferedReader reader = new BufferedReader(fileReader);
			String line;
			int i=0;
			ArrayList<String> a = new ArrayList<String>(); 
			while ((line = reader.readLine()) != null && i < 1000000) {
				String[] s = line.split("\\s+");
				i++;
				if(i == 9993){
					int kk=0;
				}
				m.put(s[0], Integer.parseInt(s[1]));

				if(i%1000000 == 0){
					System.out.println(i+ " so far.");
					//System.out.println(i+"\t"+m.getLexiconSavings());
				}
				//System.out.println(i-1 + "\t"+ m.getPos(s[0]));
				a.add(s[0]);
			}


			for(int k = 0; k<1000000; k++){
				if(k == 9993){
					int kk=0;
				}
				int pos = m.getIndex(a.get(k));
				//System.out.println(k + "\t"+ pos);
				if(k != pos){
					System.out.println(k+"\tnot found: "+ a.get(k));
					/*byte[] sb= a.get(k).getBytes();
					for(int r=0; r<sb.length; r++) 
						System.out.println(sb[r]);
					String s = new String(sb);
					System.out.println(s);
					String orig = a.get(k);
					System.out.println(s.equals(orig));
					byte[] sb2= s.getBytes();
					for(int r=0; r<sb2.length; r++) 
						System.out.println(sb2[r]);
					System.out.println(sb.equals(sb2));
					for(int r=0; r<sb2.length; r++){
						if(sb[r] == sb2[r]) System.out.println("1");
						else System.out.println("0");
					}
					System.out.println(m.getKey(k));
					System.out.println(a.get(k));
					System.out.println(a.get(k).equals(m.getKey(k)));
					System.out.println(a.get(k).getBytes().toString());*/
				}
			}
			//m.printCompressedKeys();
			//m.printKeys();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public void write(String fname) {
		System.out.println("Writing prefix map: " + fname);
		ObjectOutputStream out = null;
		try {
			out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fname)));
			write(out);
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Writing done.");
	}

	public void read(String fname) {
		System.out.println("Read prefix map from: " + fname);
		ObjectInputStream in = null;
		try {
			in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(fname)));
			try {
				readFields(in);
			} catch (EOFException e) {
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Reading done.");
	}


	/*public static void convertTextToPrefixMapFile(int window, String textfname, String objfname){
		//PrefixMap m = new PrefixMap(windo);
		try {
			FileReader fileReader = new FileReader(textfname);
			BufferedReader reader = new BufferedReader(fileReader);
			String line;
			int curKeyIndex=0;
			String lastKey="";
			int prefix = 0;
			String key  = "";
			while ((line = reader.readLine()) != null) {
				String[] s = line.split("\\s+");
				key = s[0];
				byte[] byteArray = null;
				if( curKeyIndex % window == 0){
					byteArray = key.getBytes();

				}
				else{
					prefix = PrefixUtils.getPrefix(lastKey, key);
					byte[] suffix = key.substring(prefix).getBytes();
					byteArray = new byte[suffix.length+1];
					byteArray[0] = (byte) prefix;
					for(int i=0; i<suffix.length; i++) byteArray[i+1] = suffix[i];
				}
				lastKey = key;
				curKeyIndex++;
				if(curKeyIndex%1000000 == 0){
					System.out.println(curKeyIndex+ " so far.");
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}*/


	public static void testReadWrite(){
		int n=1000000;
		String fname ="c:/cygwin/home/Tamer/part-00000";
		PrefixEncodedTermSet m = new PrefixEncodedTermSet(4);
		try {
			FileReader fileReader = new FileReader(fname);
			BufferedReader reader = new BufferedReader(fileReader);
			String line;
			int i=0;
			ArrayList<String> a = new ArrayList<String>(); 
			while ((line = reader.readLine()) != null && i < n) {
				String[] s = line.split("\\s+");
				i++;
				m.put(s[0], Integer.parseInt(s[1]));

				if(i%1000000 == 0){
					System.out.println(i+ " so far.");
					//System.out.println(i+"\t"+m.getLexiconSavings());
				}
				//System.out.println(i-1 + "\t"+ m.getPos(s[0]));
				a.add(s[0]);
			}
			//m.printCompressedKeys();
			//m.printKeys();
			/*for(int k = 0; k<n; k++){
				int pos = m.getPos(a.get(k));
				//System.out.println(k + "\t"+ pos);
				if(k != pos){
					System.out.println(k+"\tnot found: "+ a.get(k));
				}
			}*/

			String pfname = "c:/Research/test.obj";

			m.write(pfname);
			//if(true) return;

			m = new PrefixEncodedTermSet(23);
			m.read(pfname);
			//m.printCompressedKeys();
			//m.printKeys();

			int f = 0;
			for(int k = 0; k<n; k++){
				int pos = m.getIndex(a.get(k));
				//System.out.println(k + "\t"+ pos);
				if(k != pos){
					f++;
					System.out.println("("+f+")\t"+k+"\tnot found: "+ a.get(k));
				}
			}
			//m.printCompressedKeys();
			//m.printKeys();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void main(String[] args){
		testReadWrite();
	}
}
