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

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.PairOfIntLong;
import edu.umd.cloud9.util.HMapKI;
import edu.umd.cloud9.util.HMapKL;

public class PrefixEncodedGlobalStatsWithIndex {

	/**
	 * logger
	 */
	private static final Logger LOGGER = Logger.getLogger(PrefixEncodedGlobalStatsWithIndex.class);
	static{
		LOGGER.setLevel(Level.WARN);
	}
	

	Configuration conf = new Configuration();
	FileSystem fileSys = FileSystem.get(conf);
	
	PrefixEncodedTermSet prefixSet = new PrefixEncodedTermSet();
	
	int[] dfs = null;
	HMapKI<String> frequentTermsDfs = null;
	
	long[] cfs = null;
	HMapKL<String> frequentTermsCfs = null;
	
	int[] idToTerm = null;
	

	//FSDataInputStream termsInput = null;
	//FSDataInputStream dfStatsInput = null;
	//FSDataInputStream cfStatsInput = null;

	public PrefixEncodedGlobalStatsWithIndex(Path prefixSetPath) throws IOException {
		FSDataInputStream termsInput = fileSys.open(prefixSetPath);
		prefixSet.readFields(termsInput);
		termsInput.close();
	}

	public PrefixEncodedGlobalStatsWithIndex(Path prefixSetPath, FileSystem fs) throws IOException {
		fileSys = fs;
		FSDataInputStream termsInput = fileSys.open(prefixSetPath);

		prefixSet.readFields(termsInput);
		termsInput.close();
	}

	public void loadDFStats(Path dfStatsPath, Path idToTermPath, float cachedFrequentPercent, boolean keepIDToTermMap) throws IOException {
		loadDfs(dfStatsPath);
		
		if(cachedFrequentPercent < 0 || cachedFrequentPercent > 1.0)
			return;
		
		if(cachedFrequentPercent > 0 || keepIDToTermMap){
			loadIdToTerm(idToTermPath);
			if(cachedFrequentPercent > 0.2) cachedFrequentPercent = 0.2f; 
			int cachedFrequent = (int)(cachedFrequentPercent*dfs.length);
			if(cachedFrequent > 0) loadFrequentDfMap(cachedFrequent);
			if(!keepIDToTermMap) idToTerm= null;
		}
	}
	
	private void loadDfs(Path dfStatsPath)throws IOException {
		if(dfs != null) return;
		FSDataInputStream dfStatsInput = fileSys.open(dfStatsPath);
		int l = dfStatsInput.readInt();
		if (l != prefixSet.length()) {
			throw new RuntimeException("df length mismatch: " + l + "\t" + prefixSet.length());
		}
		dfs = new int[l];
		for (int i = 0; i < l; i++)
			dfs[i] = WritableUtils.readVInt(dfStatsInput);
		dfStatsInput.close();
	}
	private void loadIdToTerm(Path idToTermPath)throws IOException {
		if(idToTerm != null) return;
		FSDataInputStream idToTermInput;
		idToTermInput = fileSys.open(idToTermPath);
		LOGGER.info("Loading id to term array ...");
		int k = idToTermInput.readInt();
		idToTerm= new int[k];
		for(int i = 0 ; i<k; i++) idToTerm[i] = idToTermInput.readInt();
		LOGGER.info("Loading done.");
		idToTermInput.close();
	}

	private void loadFrequentDfMap(int n){
		if(frequentTermsDfs != null) return;
		frequentTermsDfs = new HMapKI<String>();
		if(dfs.length<n) n = dfs.length;  
		for(int id = 1; id<=n; id++){
			frequentTermsDfs.put(prefixSet.getKey(idToTerm[id-1]), dfs[idToTerm[id-1]]);
		}
		//return frequentTermsMap;
	}
	public int getDF(String term) {
		//if(dfs == null) 
		//	throw new RuntimeException("DF-Stats must be loaded first!");
		
		if(frequentTermsDfs != null){
			try{
				int df = frequentTermsDfs.get(term);
				LOGGER.info("[cached] df of "+term+": "+df);
				return df;
			}catch (NoSuchElementException e){
			}
		}
		int index = prefixSet.getIndex(term);
		LOGGER.info("index of " + term + ": " + index);
		if (index < 0)
			return -1;
		return dfs[index];
	}

	/*public void loadCFStats(Path cfStatsPath) throws IOException {
		loadCFStats(cfStatsPath, fileSys);
	}*/
	
	public void loadCFStats(Path cfStatsPath, Path idToTermPath, float cachedFrequentPercent, boolean keepIDToTermMap) throws IOException {
		loadCfs(cfStatsPath);
		
		if(cachedFrequentPercent < 0 || cachedFrequentPercent > 1.0)
			return;
		
		if(cachedFrequentPercent > 0 || keepIDToTermMap){
			loadIdToTerm(idToTermPath);
			if(cachedFrequentPercent > 0.2) cachedFrequentPercent = 0.2f; 
			int cachedFrequent = (int)(cachedFrequentPercent*dfs.length);
			if(cachedFrequent > 0) loadFrequentCfMap(cachedFrequent);
			if(!keepIDToTermMap) idToTerm= null;
		}
	}
	public void loadCfs(Path cfStatsPath) throws IOException {
		if(cfs != null) return;
		FSDataInputStream cfStatsInput = fileSys.open(cfStatsPath);

		int l = cfStatsInput.readInt();
		if (l != prefixSet.length()) {
			throw new RuntimeException("cf length mismatch: " + l + "\t" + prefixSet.length());
		}
		cfs = new long[l];
		for (int i = 0; i < l; i++)
			cfs[i] = WritableUtils.readVLong(cfStatsInput);
		cfStatsInput.close();
	}

	private void loadFrequentCfMap(int n){
		if(frequentTermsCfs != null) return;
		frequentTermsCfs = new HMapKL<String>();
		if(cfs.length<n) n = cfs.length;  
		for(int id = 1; id<=n; id++){
			frequentTermsCfs.put(prefixSet.getKey(idToTerm[id-1]), cfs[idToTerm[id-1]]);
		}
	}
	
	public long getCF(String term) {
		//if(cfs == null) 
		//	throw new RuntimeException("CF-Stats must be loaded first!");
		
		if(frequentTermsDfs != null){
			try{
				long cf = frequentTermsCfs.get(term);
				LOGGER.info("[cached] df of "+term+": "+cf);
				return cf;
			}catch (NoSuchElementException e){
			}
		}
		int index = prefixSet.getIndex(term);
		LOGGER.info("index of " + term + ": " + index);
		if (index < 0)
			return -1;
		return cfs[index];
	}

	public PairOfIntLong getStats(String term) {
		int df = -1;
		long cf = -1;
		PairOfIntLong p = new PairOfIntLong();
		if(frequentTermsDfs != null){
			try{
				df = frequentTermsDfs.get(term);
				LOGGER.info("[cached] df of "+term+": "+df);
				if(frequentTermsCfs != null){
					try{
						cf = frequentTermsCfs.get(term);
						LOGGER.info("[cached] cf of "+term+": "+cf);
						p.set(df, cf);
						return p;
					}catch (NoSuchElementException e){
					}
				}
			}catch (NoSuchElementException e){
			}
		}
		int index = prefixSet.getIndex(term);
		LOGGER.info("index of " + term + ": " + index);
		if (index < 0)
			return null;
		p.set(dfs[index], cfs[index]);
		return p;
	}
	
	/*public PairOfIntLong getStats(String term) {
		int index = prefixSet.getIndex(term);
		LOGGER.info("index of " + term + ": " + index);
		if (index < 0)
			return null;
		PairOfIntLong p = new PairOfIntLong();
		p.set(dfs[index], cfs[index]);
		return p;
	}*/
	
	public PairOfIntLong getStats(int index) {
		if (index < 0)
			return null;
		PairOfIntLong p = new PairOfIntLong();
		p.set(dfs[index], cfs[index]);
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
			if (dfs != null)
				System.out.print("\t" + dfs[i]);
			if (cfs != null)
				System.out.print("\t" + cfs[i]);
			System.out.println();
		}
	}

	/*
	 * public void printPrefixSetContent(){ prefixSet.printCompressedKeys();
	 * prefixSet.printKeys(); }
	 */
	public static void main(String[] args) throws Exception{
		//String indexPath = "/umd-lin/telsayed/indexes/medline04";
		String indexPath = "c:/Research/ivory-workspace";
		
		Configuration conf = new Configuration();
		FileSystem fileSys= FileSystem.getLocal(conf);
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fileSys);
		
		Path termsFilePath = new Path(env.getIndexTermsData());
		
		Path dfByTermFilePath = new Path(env.getDfByTermData());
		Path cfByTermFilePath = new Path(env.getCfByTermData());
		
		Path idToTermFilePath = new Path(env.getIndexTermIdMappingData());

		System.out.println("PrefixEncodedGlobalStats");

		PrefixEncodedGlobalStatsWithIndex globalStatsMap = new PrefixEncodedGlobalStatsWithIndex(termsFilePath);
		System.out.println("PrefixEncodedGlobalStats1");
		globalStatsMap.loadDFStats(dfByTermFilePath, idToTermFilePath, 0.2f, true);
		System.out.println("PrefixEncodedGlobalStats2");
		globalStatsMap.loadCFStats(cfByTermFilePath, idToTermFilePath, 0.2f, false);
		System.out.println("PrefixEncodedGlobalStats3");
		//String[] firstKeys = termIDMap.getDictionary().getFirstKeys(100);
		int nTerms = globalStatsMap.length();
		System.out.println("nTerms: "+nTerms);
		/*for(int i = 0; i < nTerms; i++){
			
			PairOfIntLong p = globalStatsMap.getStats(i);
			System.out.println(i+"\t"+p.getLeftElement() +"\t"+ p.getRightElement());
			//if(i%10000 == 0) System.out.println(i+" terms so far ("+p+").");
		}*/
		String term;
		term = "0046";	System.out.println(term+"\t"+globalStatsMap.getDF(term));
		term = "00565";	System.out.println(term+"\t"+globalStatsMap.getDF(term));
		term = "01338";	System.out.println(term+"\t"+globalStatsMap.getDF(term));
		term = "01hz";	System.out.println(term+"\t"+globalStatsMap.getDF(term));
		term = "03x";	System.out.println(term+"\t"+globalStatsMap.getDF(term));
		term = "0278x";	System.out.println(term+"\t"+globalStatsMap.getDF(term));
		
		term = "0081";	System.out.println(term+"\t"+globalStatsMap.getDF(term));
		term = "0183";	System.out.println(term+"\t"+globalStatsMap.getDF(term));
		term = "0244";	System.out.println(term+"\t"+globalStatsMap.getDF(term));
		term = "032";	System.out.println(term+"\t"+globalStatsMap.getDF(term));
		//for(int i = 1; i<=200; i++){
		//	term = termIDMap.getTerm(i);
		//	System.out.println(i+"\t"+term+"\t"+termIDMap.getID(term));
		//}
	}
}
