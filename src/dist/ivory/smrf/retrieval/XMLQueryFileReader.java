package ivory.smrf.retrieval;

import ivory.data.PrefixEncodedTermIDMapWithIndex;
import ivory.util.GalagoTokenizer;
import ivory.util.Tokenizer;
import ivory.util.XMLTools;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.umd.cloud9.util.HMapIV;
import edu.umd.cloud9.util.HMapKI;

/**
 * A class to read query files from an XML file and iterate over query_id and query_text values.
 * A tokenizer preprocesses query text and the array of tokens are returned.
 * 
 * @author ferhanture
 *
 */
public class XMLQueryFileReader extends QueryFileReaderBase{
	Document queryDoc;

	public XMLQueryFileReader(FileSystem fs,  Path queriesFile) {
		this(fs, queriesFile, new GalagoTokenizer());
	}
	
	public XMLQueryFileReader(Path queriesFile) throws Exception{
		this(FileSystem.get(new Configuration()), queriesFile, new GalagoTokenizer());
	}

	public XMLQueryFileReader(FileSystem fs, Path queriesFile, Tokenizer tokenizer) {
		super(fs, queriesFile, tokenizer);
		mTokenizer = tokenizer;
		/*
		 * Build document object from file containing batch of queries
		 * 
		 */
		try {
			//FileSystem fs = FileSystem.get(conf);
			queryDoc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
					fs.open(queriesFile));
		} catch (Exception e1) {
			throw new RuntimeException("Could not read query file from "+queriesFile+"\n"+e1);
		}
		
		/*
		 * Extract queries from query document, then store them in a hashmap of the form queryID --> query
		 * 
		 */
		NodeList queries = queryDoc.getElementsByTagName("query");
		numQueries= queries.getLength();
		queryTexts = new String[numQueries][];
		queryIds = new String[numQueries];

		for (int i = 0; i < numQueries; i++) {
			// query XML node
			Node node = queries.item(i);

			// get query id
			String queryID = XMLTools.getAttributeValue(node, "id", null);
			if (queryID == null) {
				throw new RuntimeException("Must specify a query id attribute for every query!");
			}
			queryIds[i] = queryID;

			// get query text
			String queryText = node.getTextContent();

			queryTexts[i] = mTokenizer.processContent(queryText);
		}
		iter = 0;
	}

	/*
	 * Extract queries from query document, then store them in a hashmap of the form queryID --> query
	 * No tokenization or other preprocessing done.
	 * 
	 */
	public HashMap<String,String> getQueries(){
		HashMap<String,String> mQueries = new HashMap<String,String>();
		NodeList queries = queryDoc.getElementsByTagName("query");
		for (int i = 0; i < queries.getLength(); i++) {
			// query XML node
			Node node = queries.item(i);

			// get query id
			String queryID = XMLTools.getAttributeValue(node, "id", null);
			if (queryID == null) {
				throw new RuntimeException("Must specify a query id attribute for every query!");
			}

			// get query text
			String queryText = node.getTextContent();

			// add query to lookup
			if (mQueries.get(queryID) != null) {
				throw new RuntimeException(
						"Duplicate query ids not allowed! Already parsed query with id=" + queryID);
			}
			mQueries.put(queryID, queryText);
		}
		return mQueries;
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
	
	

	
	public static void main(String[] args) throws Exception{
		QueryFileReaderBase qReader = new XMLQueryFileReader(new Path("/Users/ferhanture/Documents/workspace/ivory/docs/data/queries_robust04.xml"));
//		QueryFileReaderBase qReader = new XMLQueryFileReader(new Path("c:/Research/ivory-workspace/efficiency06_queries4.xml"));
	int cnt=0;
		while(qReader.hasMoreQueries()){
			String qId = qReader.nextQueryId();
			String[] qTokens = qReader.nextQuery();
			System.out.println(qId+" --> "+qTokens);
			cnt++;
		}			
		System.out.println("read "+cnt);

		qReader.reset();
		
		cnt=0;
		while(qReader.hasMoreQueries()){
			String qId = qReader.nextQueryId();
			String[] qTokens = qReader.nextQuery();
			System.out.println(qId+" --> "+qTokens);
			cnt++;
		}
		System.out.println("read "+cnt);
	}

}
