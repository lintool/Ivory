package ivory.smrf.retrieval;

import ivory.data.PrefixEncodedTermIDMapWithIndex;
import ivory.util.Tokenizer;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umd.cloud9.util.HMapIV;
import edu.umd.cloud9.util.HMapKI;

/**
 * A class to read query files from an XML file and iterate over query_id and query_text values.
 * A tokenizer preprocesses query text and the array of tokens are returned.
 * 
 * @author ferhanture
 *
 */
public abstract class QueryFileReaderBase {
	String[][] queryTexts;
	String[] queryIds;
	int numQueries, iter;
	Tokenizer mTokenizer;
	
	public QueryFileReaderBase(FileSystem fs, Path queriesFile, Tokenizer tokenizer){
		
	}
	
	public String[] nextQuery(){
		if(hasMoreQueries()){
			return queryTexts[iter++];
		}else{
			return null;
		}
	}
	public String nextQueryId(){
		if(hasMoreQueries()){
			return queryIds[iter];
		}else{
			return null;
		}
	}
	public boolean hasMoreQueries() {
		return iter<numQueries;
	}
	
	public void reset(){
		iter = 0;
	}
	
	public int numOfQueries(){
		return numQueries;
	}
	
	public void GetTermQueryAndQueryLengthMaps(PrefixEncodedTermIDMapWithIndex termIDMap, HMapIV<Set<String>> termQueryMap, HMapKI<String> queryLength){
		Set<String> queries;
		while(hasMoreQueries()){
			String qId = nextQueryId();
			String[] qTokens = nextQuery();
			queryLength.put(qId, qTokens.length);
			for(String qToken : qTokens){
				int qTokenID = termIDMap.getID(qToken);
				if(qTokenID<0){
					continue;
				}
				queries = termQueryMap.get(qTokenID);
				if(queries == null){
					queries = new HashSet<String>();
					termQueryMap.put(qTokenID, queries);
				}
				queries.add(qId);
			}
		}
	}
}
