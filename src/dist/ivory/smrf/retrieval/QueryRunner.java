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

import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.expander.MRFExpander;

import java.util.List;

import org.apache.log4j.Logger;


/**
 * @author Don Metzler
 *
 */
public class QueryRunner {

	/**
	 * MRF builder
	 */
	private MRFBuilder mBuilder;

	/**
	 * MRF expander
	 */
	private MRFExpander mExpander;
		
	/**
	 * logger
	 */
	private static final Logger sLogger = Logger.getLogger(QueryRunner.class);

	public QueryRunner(MRFBuilder builder, MRFExpander expander) {
		mBuilder = builder;
		mExpander = expander;
	}

	/**
	 * @param query
	 */
	public Accumulator [] runQuery(String query) {
		long startTime;
		long endTime;
		
		try {
			startTime = System.currentTimeMillis();
			
			// build the MRF for this query
			MarkovRandomField mrf = mBuilder.buildMRF( query );
			endTime = System.currentTimeMillis();

			sLogger.info("MRF initialization time (ms): " + (endTime-startTime));
			
			// build the document set for this query
			//int [] docSet = _env.getDocSet( query );

			// add global evidence to MRF
			//mrf.setGlobalEvidence("candidateDocs", String.valueOf(docSet.length));
			
			// retrieve documents using this MRF
			startTime = System.currentTimeMillis();
			MRFDocumentRanker ranker = new MRFDocumentRanker( mrf, 1000 );
		
			// run initial query, if necessary
			Accumulator [] results = null;
			if( mExpander != null ) {
				results = ranker.rank();
			}
		
			// perform pseudo-relevance feedback, if requested
			if( mExpander != null ) {
				// get expanded MRF
				MarkovRandomField expandedMRF = mExpander.getExpandedMRF( mrf, results );

				// update doc set to reflect the expansion terms
				String queryTerms = "";
				List<ivory.smrf.model.Node> nodes = expandedMRF.getNodes();
				for( ivory.smrf.model.Node node : nodes ) {
					if( node instanceof TermNode ) {
						queryTerms += ((TermNode)node).getTerm() + " ";
					}
				}
				//docSet = _env.getDocSet( queryTerms );
				
				// add global evidence to MRF
				//expandedMRF.setGlobalEvidence("candidateDocs", String.valueOf(docSet.length));
				
				// re-rank documents according to expanded MRF
				ranker = new MRFDocumentRanker( expandedMRF, 1000 );
			}
			
			endTime = System.currentTimeMillis();
			sLogger.info("MRF document ranker initialization time (ms): " + (endTime-startTime));

			// rank the documents
			startTime = System.currentTimeMillis();
			results = ranker.rank();
			endTime = System.currentTimeMillis();
			sLogger.info("MRF document ranking time (ms): " + (endTime-startTime));
			
			return results;
		}
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}	
	
}
