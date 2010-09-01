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

package ivory.smrf.retrieval;

import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.builder.MRFBuilderFactory;
import ivory.smrf.model.expander.MRFExpander;
import ivory.smrf.model.expander.MRFExpanderFactory;
import ivory.util.OutputTools;
import ivory.util.ResultWriter;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * 
 * @author Don Metzler
 *
 */
public class RunQueryLocal {

	private static final Logger LOGGER = Logger.getLogger(RunQueryLocal.class);

	/**
	 * interface for obtaining metadata about documents 
	 */
	//private MetadataReader _metadataReader = null;
	
	/**
	 * index location
	 */
	private String mIndexPath = null;
	
	/**
	 * retrieval environment
	 */
	private RetrievalEnvironment mEnv = null;
	
	/**
	 *  maps query ids to query texts
	 */
	private Map<String,String> mQueries = null;
	
	/**
	 * maps model ids to model XML parameter nodes
	 */
	private Map<String,Node> mModels  = null;

	/**
	 * maps model ids to expansion XML parameter nodes 
	 */
	private Map<String,Node> mExpanders = null;
	
	/**
	 * stopword list
	 */
	private HashSet<String> mStopwords = null;
	
	/**
	 * @param args
	 * @throws SAXException
	 * @throws IOException
	 * @throws ParserConfigurationException
	 * @throws SMRFException
	 * @throws NotBoundException
	 */
	public RunQueryLocal(String [] args) throws SAXException, IOException, ParserConfigurationException, Exception, NotBoundException {
		mQueries = new LinkedHashMap<String,String>();
		mModels = new LinkedHashMap<String,Node>();
		mExpanders = new HashMap<String,Node>();
		mStopwords = new HashSet<String>();
		
		for (String element : args) {
			Document d = null;
			d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(element);
			_parseParameters(d);
		}

		// make sure we have an index to run against
		if( mIndexPath == null ) {
			throw new Exception("Must specify an index!");
		}
		
		// initialize retrieval environment
		mEnv = new RetrievalEnvironment( mIndexPath );
		
		//if( _metadataReader == null ) {
		//	throw new SMRFException("Must specify metadata reader!");
		//}
		
		// make sure we have some queries to run
		if( mQueries.size() == 0 ) {
			throw new Exception("Must specify at least one query!");
		}
		
		// make sure there are models that need evaluated
		if( mModels.size() == 0 ) {
			throw new Exception("Must specify at least one model!");
		}		
	}

	/**
	 * runs the queries
	 */
	public void runQueries() throws Exception {
		for (String modelID : mModels.keySet()) {
			// where should we output these results?
			ResultWriter resultWriter = OutputTools.getResultWriter( mModels.get( modelID ) );

			Node modelNode = mModels.get(modelID); 
			Node expanderNode = mExpanders.get(modelID);
			
			// initialize retrieval environment variables
			ThreadedQueryRunner runner = null;
			MRFBuilder builder = null;
			MRFExpander expander = null;
			try {
				// get the MRF builder
				builder = MRFBuilderFactory.getBuilder( mEnv, modelNode.cloneNode(true) );

				// get the MRF expander
				expander = null;
				if( expanderNode != null ) {
					expander = MRFExpanderFactory.getExpander( mEnv, expanderNode.cloneNode(true) );
				}
				if( mStopwords != null && mStopwords.size() != 0 ) {
					expander.setStopwordList( mStopwords );
				}

				// query runner
				runner = new ThreadedQueryRunner( builder, expander, 1 );
				// multi-threaded query evaluation still a bit unstable, setting thread=1 for now
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			for (String queryID : mQueries.keySet()) {				
				String rawQueryText = mQueries.get(queryID);
				
				// tokenize query (stopping, stemming, etc.)
				String [] queryTokens = mEnv.tokenize(rawQueryText);
				StringBuffer queryText = new StringBuffer();
				for( String token : queryTokens ) {
					queryText.append(token);
					queryText.append(" ");
				}
				
				LOGGER.info("query id:" + queryID + ", query text:" + queryText.toString());

				// get the query spec
				//QuerySpecification spec = new QuerySpecification(queryID, queryText.toString(), mModels.get(modelID), mExpanders.get(modelID), mStopwords);
				
				// execute the query
				runner.runQuery(queryID, queryText.toString());
			}
			
			for (String queryID : mQueries.keySet()) { 
				// get the ranked list for this query
				Accumulator [] list = runner.getResults( queryID );
				
				// get doc ids corresponding to Accumulators in result set
				//int [] docids = Accumulator.accumulatorsToDocIDS( list );

				// get the document names
				//String [] docNames = _metadataReader.getDocumentNames( docids );
			
				// print the results
				for( int i = 0; i < list.length; i++ ) {
					resultWriter.println( queryID + " Q0 " + list[i].docid  + " " + (i+1) + " " + list[i].score + " " + modelID );
					//resultWriter.println( queryID + " Q0 " + docNames[ i ]  + " " + (i+1) + " " + list[i].score + " " + modelID );
				}
			}	
		
			// flush the result writer
			resultWriter.flush();
		}
	}	
	
	/**
	 * @param d
	 * @throws NotBoundException 
	 * @throws RemoteException 
	 */
	private void _parseParameters(Document d) throws Exception, RemoteException, NotBoundException {
		// parse query elements
		NodeList queries = d.getElementsByTagName("query");
		for( int i = 0; i < queries.getLength(); i++ ) {
			// query XML node
			Node node = queries.item( i );
			
			// get query id
			String queryID = XMLTools.getAttributeValue(node, "id", null);
			if( queryID == null ) {
				throw new Exception("Must specify a query id attribute for every query!");
			}
			
			// get query text
			String queryText = node.getTextContent();

			// add query to lookup
			if( mQueries.get(queryID) != null ) {
				throw new Exception("Duplicate query ids not allowed! Already parsed query with id=" + queryID );
			}			
			mQueries.put(queryID, queryText);
		}
		
		// parse model elements
		NodeList models = d.getElementsByTagName("model");
		for( int i = 0; i < models.getLength(); i++ ) {
			// model XML node
			Node node = models.item( i );
			
			// get model id
			String modelID = XMLTools.getAttributeValue(node, "id", null);
			if( modelID == null ) {
				throw new Exception("Must specify a model id for every model!");
			}

			// parse parent nodes
			NodeList children = node.getChildNodes();
			for( int j = 0; j < children.getLength(); j++ ) {
				Node child = children.item( j );
				if( "expander".equals( child.getNodeName() ) ) {
					if( mExpanders.containsKey( modelID ) ) {
						throw new Exception("Only one expander allowed per model!");
					}
					mExpanders.put( modelID, child );
				}
			}
			
			// add model to lookup
			if( mModels.get(modelID) != null ) {
				throw new Exception("Duplicate model ids not allowed! Already parsed model with id=" + modelID );
			}
			mModels.put(modelID, node);
		}
		
		// parse relevance judgments
//		NodeList judgments = d.getElementsByTagName("judgments");
//		for( int i = 0; i < judgments.getLength(); i++ ) {
//			// relevance judgment node
//			Node node = judgments.item( i );
//						
//			// get relevance judgments
//			_relevanceJudgments = RelevanceJudgmentsFactory.getJudgments( _metadataReader, node );
//		}

		// parse stopwords
		NodeList stopwords = d.getElementsByTagName("stopword");
		for( int i = 0; i < stopwords.getLength(); i++ ) {
			// stopword node
			Node node = stopwords.item( i );
			
			// get stopword
			String stopword = node.getTextContent();
			
			// add stopword to lookup
			mStopwords.add( stopword );
		}

		// parse index
		NodeList index = d.getElementsByTagName("index");
		if( index.getLength() > 0 ) {
			if( mIndexPath != null ) {
				throw new Exception("Must specify only one index! There is no support for multiple indexes!");
			}
			mIndexPath = index.item(0).getTextContent();
		}
		
		// initialize metadata reader
//		NodeList metaReaders = d.getElementsByTagName("metareader");
//		for( int i = 0; i < metaReaders.getLength(); i++ ) {
//			// metadata reader node
//			Node node = metaReaders.item( i );
//			
//			if( _metadataReader != null ) {
//				throw new SMRFException("Only one MetadataReader can be specified!");
//			}
//			_metadataReader = MetadataReaderFactory.getMetadataReader( node );
//		}
		
	}
	
	public static void main(String[] args) throws Exception {
		RunQueryLocal se;
		try {
			// initialize runquery
			se = new RunQueryLocal(args);

			// run the queries
			se.runQueries();			
		}
		catch( Exception e ) {
			e.printStackTrace();
		}
		System.exit(0);
	}
}
