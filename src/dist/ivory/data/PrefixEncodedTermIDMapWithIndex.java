/**
 * 
 */
package ivory.data;

import ivory.util.RetrievalEnvironment;
import java.util.NoSuchElementException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

//import edu.umd.cloud9.util.OHMapKI;
import edu.umd.cloud9.util.HMapKI;

/**
 * @author Tamer
 *
 */
public class PrefixEncodedTermIDMapWithIndex extends PrefixEncodedTermIDMap {


	private static final Logger LOGGER = Logger.getLogger(PrefixEncodedTermIDMapWithIndex.class);
	static {
		LOGGER.setLevel(Level.WARN);
	}
	int[] idToTerm = null;
	
	public PrefixEncodedTermIDMapWithIndex(Path prefixSetPath , Path idsPath, FileSystem fileSys) throws IOException{
		super(prefixSetPath, idsPath, fileSys);
	}
		
	public PrefixEncodedTermIDMapWithIndex(Path prefixSetPath , Path idsPath, Path idToTermPath, FileSystem fileSys) throws IOException{
		super(prefixSetPath, idsPath, fileSys);
		FSDataInputStream idToTermInput;

		idToTermInput = fileSys.open(idToTermPath);
		LOGGER.info("Loading id to term array ...");
		int l = idToTermInput.readInt();
		idToTerm= new int[l];
		for(int i = 0 ; i<l; i++) idToTerm[i] = idToTermInput.readInt();
		LOGGER.info("Loading done.");
		idToTermInput.close();
		//for(int i = 0; i<100; i++)	System.out.println(i+"\t"+idToTerm[i]);
	}


	public PrefixEncodedTermIDMapWithIndex(Path prefixSetPath , Path idsPath, Path idToTermPath, int cachedFrequent, boolean keepIDToTermMap, FileSystem fileSys) throws IOException{
		super(prefixSetPath, idsPath, fileSys);
		if(cachedFrequent > 0 || keepIDToTermMap){
			FSDataInputStream idToTermInput;
			idToTermInput = fileSys.open(idToTermPath);
			LOGGER.info("Loading id to term array ...");
			int l = idToTermInput.readInt();
			idToTerm= new int[l];
			for(int i = 0 ; i<l; i++) idToTerm[i] = idToTermInput.readInt();
			LOGGER.info("Loading done.");
			idToTermInput.close();
			if(cachedFrequent > 0) loadFrequentMap(cachedFrequent);
			if(!keepIDToTermMap) idToTerm= null;
		}
		//for(int i = 0; i<100; i++)	System.out.println(i+"\t"+idToTerm[i]);
	}

	public PrefixEncodedTermIDMapWithIndex(Path prefixSetPath , Path idsPath, Path idToTermPath, float cachedFrequentPercent, boolean keepIDToTermMap, FileSystem fileSys) throws IOException{
		super(prefixSetPath, idsPath, fileSys);
		if(cachedFrequentPercent < 0 || cachedFrequentPercent > 1.0)
			return;
		
		if(cachedFrequentPercent > 0 || keepIDToTermMap){
			FSDataInputStream idToTermInput;
			idToTermInput = fileSys.open(idToTermPath);
			LOGGER.info("Loading id to term array ...");
			int l = idToTermInput.readInt();
			idToTerm= new int[l];
			for(int i = 0 ; i<l; i++) idToTerm[i] = idToTermInput.readInt();
			LOGGER.info("Loading done.");
			idToTermInput.close();
			if(cachedFrequentPercent > 0.3) cachedFrequentPercent = 0.3f; 
			int cachedFrequent = (int)(cachedFrequentPercent*ids.length);
			if(cachedFrequent > 0) loadFrequentMap(cachedFrequent);
			if(!keepIDToTermMap) idToTerm= null;
		}
		//for(int i = 0; i<100; i++)	System.out.println(i+"\t"+idToTerm[i]);
	}
	
	public String getTerm(int id){
		if(id>ids.length || id == 0 || idToTerm == null) return null;
		String term = prefixSet.getKey(idToTerm[id-1]);
		//LOGGER.info("ID: "+id+", TermIndex: "+idToTerm[id-1]+", Term: "+term);
		return term;
	}

	HMapKI<String> frequentTermsMap = null;

	private void loadFrequentMap(int n){
		if(ids.length<n) n = ids.length;
		frequentTermsMap = new HMapKI<String>(n);
		for(int id = 1; id<=n; id++){
			frequentTermsMap.put(prefixSet.getKey(idToTerm[id-1]), id);
		}
		//return frequentTermsMap;
	}

	public int getID(String term){
		if(frequentTermsMap == null) return super.getID(term);
		try{
			int id = frequentTermsMap.get(term);
			LOGGER.info("[cached] id of "+term+": "+id);
			return id;
		}catch (NoSuchElementException e){
		}
		return super.getID(term);
	}




	/*public static void main(String[] args) throws Exception{
		//String indexPath = "/umd-lin/telsayed/indexes/medline04";
		String indexPath = "/umd-lin/telsayed/indexes/clue.01";
		Path termsFilePath = new Path(RetrievalEnvironment.getPostingsIndexTerms(indexPath));
		Path termIDsFilePath = new Path(RetrievalEnvironment.getPostingsIndexTermIDs(indexPath));
		Path idToTermFilePath = new Path(RetrievalEnvironment.getIDToTermFile(indexPath));

		System.out.println("PrefixEncodedTermIDMapWithIndex");
		Configuration conf = new Configuration();
		FileSystem fileSys= FileSystem.get(conf);
		PrefixEncodedTermIDMapWithIndex termIDMap = new PrefixEncodedTermIDMapWithIndex(termsFilePath, termIDsFilePath, idToTermFilePath, 100, true, fileSys);
		//String[] firstKeys = termIDMap.getDictionary().getFirstKeys(100);
		System.out.println("nTerms: "+termIDMap.getNumOfTerms());
		String term;
		//for(int i = 1; i<=200; i++){
		//	term = termIDMap.getTerm(i);
		//	System.out.println(i+"\t"+term+"\t"+termIDMap.getID(term));
		//}
		int i = 563251;
		term = termIDMap.getTerm(i);
		System.out.println(i+"\t"+term+"\t"+termIDMap.getID(term));
	}*/
	
	public static void test(String[] args) throws Exception{
		//String indexPath = "/umd-lin/telsayed/indexes/medline04";
		String indexPath = "c:/Research/ivory-workspace";

		Configuration conf = new Configuration();
		FileSystem fileSys= FileSystem.getLocal(conf);

		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fileSys);
		
		Path termsFilePath = new Path(env.getIndexTermsData());
		Path termIDsFilePath = new Path(env.getIndexTermIdsData());
		Path idToTermFilePath = new Path(env.getIndexTermIdMappingData());

		System.out.println("PrefixEncodedTermIDMapWithIndex");

		PrefixEncodedTermIDMapWithIndex termIDMap = new PrefixEncodedTermIDMapWithIndex(termsFilePath, termIDsFilePath, idToTermFilePath, 100, true, fileSys);
		//String[] firstKeys = termIDMap.getDictionary().getFirstKeys(100);
		int nTerms = termIDMap.getNumOfTerms();
		System.out.println("nTerms: "+nTerms);
		int p = 0;
		for(int i = 1; i <= nTerms; i++){
			String s = termIDMap.getTerm(i);
			int k = termIDMap.getID(s);
			//System.out.println(i+"\t"+s +"\t"+ k);
			if(i!=k){
				p++;
				//System.out.println(i+"\t"+s +"\t"+ k);
				//System.out.println("!!!!!!!!!!!!!!!");
			}
			if(i%10000 == 0) System.out.println(i+" terms so far ("+p+").");
		}
		String term;
		//for(int i = 1; i<=200; i++){
		//	term = termIDMap.getTerm(i);
		//	System.out.println(i+"\t"+term+"\t"+termIDMap.getID(term));
		//}
	}

	public static void main(String[] args) throws Exception{
		if (args.length != 1) {
			System.out.println("usage: [index-path]");
			System.exit(-1);
		}
		
		String indexPath = args[0];

		Configuration conf = new Configuration();
		FileSystem fileSys= FileSystem.get(conf);

		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fileSys);
		
		Path termsFilePath = new Path(env.getIndexTermsData());
		Path termIDsFilePath = new Path(env.getIndexTermIdsData());
		Path idToTermFilePath = new Path(env.getIndexTermIdMappingData());

		System.out.println("PrefixEncodedTermIDMapWithIndex");

		PrefixEncodedTermIDMapWithIndex termIDMap = new PrefixEncodedTermIDMapWithIndex(termsFilePath, termIDsFilePath, idToTermFilePath, 100, true, fileSys);

		int nTerms = termIDMap.getNumOfTerms();
		System.out.println("nTerms: "+nTerms);

		System.out.println(" \"term word\" to lookup termid; \"termid 234\" to lookup term");
		String cmd = null;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("lookup > ");
		while ((cmd = stdin.readLine()) != null) {
			
			String[] tokens = cmd.split("\\s+");
			
			if ( tokens.length != 2) {
				System.out.println("Error: unrecognized command!");
				System.out.print("lookup > ");

				continue;
			}
			
			if ( tokens[0].equals("termid")) {
				int termid;
				try {
					termid = Integer.parseInt(tokens[1]);
				} catch ( Exception e ) {
					System.out.println("Error: invalid termid!");
					System.out.print("lookup > ");

					continue;
				}
			
				System.out.println("termid=" + termid + ", term=" + termIDMap.getTerm(termid));
			} else if ( tokens[0].equals("term") ) {
				String term = tokens[1];
				
				System.out.println("term=" + term + ", termid=" + termIDMap.getID(term));

			} else  {
				System.out.println("Error: unrecognized command!");
				System.out.print("lookup > ");
				continue;
			}

			System.out.print("lookup > ");
		}
		
	}
}
