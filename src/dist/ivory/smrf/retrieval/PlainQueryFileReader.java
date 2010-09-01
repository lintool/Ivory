package ivory.smrf.retrieval;

import ivory.data.PrefixEncodedTermIDMapWithIndex;
import ivory.util.GalagoTokenizer;
import ivory.util.Tokenizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.util.FSLineReader;
import edu.umd.cloud9.util.HMapIV;
import edu.umd.cloud9.util.HMapKI;

/**
 * A class to read query files from an XML file and iterate over query_id and query_text values.
 * A tokenizer preprocesses query text and the array of tokens are returned.
 * 
 * @author ferhanture
 *
 */
public class PlainQueryFileReader extends QueryFileReaderBase{
	String[][] queryTexts;
	String[] queryIds;
	int numQueries, iter;
	Tokenizer mTokenizer;
	
	/**
	 * Create QueryFileReader instance from specified file. 
	 * Default tokenizer is ivory.util.GalagoTokenizer
	 * 
	 * @param queriesFile
	 * 		line-formatted file that contains queries in the format: "id:query"
	 */
	public PlainQueryFileReader(Path queriesFile) throws Exception{
		this(FileSystem.get(new Configuration()), queriesFile, new GalagoTokenizer());
	}
	

	public PlainQueryFileReader(FileSystem fs,  Path queriesFile) {
		this(fs, queriesFile, new GalagoTokenizer());
	}
		
	public PlainQueryFileReader(FileSystem fs, Path queriesFile, Tokenizer tokenizer) {
		super(fs, queriesFile, tokenizer);
		mTokenizer = tokenizer;
		ArrayList<String[]> queryTextList = new ArrayList<String[]>();
		ArrayList<String> queryIdsList = new ArrayList<String>();

		try {
			FSLineReader reader = new FSLineReader(queriesFile, fs);
			Text line = new Text();
			String s;
			while (reader.readLine(line) > 0) {
				s = line.toString();
				int p = s.indexOf(':');
				String qid = s.substring(0, p);
				String query = s.substring(p+1);
				query = query.replaceAll("&", "&amp;").trim();
				queryTextList.add(mTokenizer.processContent(query));
				queryIdsList.add(qid);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		numQueries = queryTextList.size();
		queryTexts = new String[numQueries][];
		queryTextList.toArray(queryTexts);
		queryIds = new String[numQueries];
		queryIdsList.toArray(queryIds);
	}
	
	public static void convert(String inputFile, String outputFile){
		try {
			File theFile = new File(inputFile);
			FileReader fileReader = new FileReader(theFile);
			BufferedReader reader = new BufferedReader(fileReader);
			FileWriter writer = new FileWriter(outputFile);
			String line;
			//System.out.println("<parameters>");
			writer.write("<parameters>");
			while ((line = reader.readLine()) != null) {
				int p = line.indexOf(':');
				String qid = line.substring(0, p);
				String query = line.substring(p+1);
				query = query.replaceAll("&", "&amp;").trim();
				//System.out.println("<query id=\""+qid+"\">"+query+"</query>");
				writer.write("<query id=\""+qid+"\">"+query+"</query>\n");
			}
			//System.out.println("</parameters>");
			writer.write("</parameters>");
			writer.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
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
		//QueryFileReader qReader = new QueryFileReader("/Users/ferhanture/Documents/workspace/ivory/docs/data/queries_robust04.xml");
		PlainQueryFileReader qReader = new PlainQueryFileReader(new Path("c:/Research/ivory-workspace/06.efficiency_topics.all"));
	int cnt=0;
		while(qReader.hasMoreQueries()){
			String qId = qReader.nextQueryId();
			String[] qTokens = qReader.nextQuery();
			System.out.print(qId+" --> ");
			for(String s : qTokens) System.out.print(s+" ");
			System.out.println();
			cnt++;
		}			
		System.out.println("read "+cnt);

		qReader.reset();
		
		cnt=0;
		while(qReader.hasMoreQueries()){
			String qId = qReader.nextQueryId();
			String[] qTokens = qReader.nextQuery();
			System.out.print(qId+" --> ");
			for(String s : qTokens) System.out.print(s+" ");
			System.out.println();
			cnt++;
		}
		System.out.println("read "+cnt);
	}

}
