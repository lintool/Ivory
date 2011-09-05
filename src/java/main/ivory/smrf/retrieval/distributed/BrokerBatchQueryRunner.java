/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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

package ivory.smrf.retrieval.distributed;


import ivory.core.util.ResultWriter;
import ivory.core.util.XMLTools;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.QueryRunner;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 * @author Tamer Elsayed
 */
public class BrokerBatchQueryRunner {
	private static final Logger sLogger = Logger.getLogger(BrokerBatchQueryRunner.class);

	private Map<String, String> mQueries = null;
	private FileSystem mFileSystem = null;
	private String mOutputFile;
	private String mBrokerAddress;
	private String mRuntag;
	private int mNumHits;

	public BrokerBatchQueryRunner(String queriesFilePath, String mid, String brokerAddr,
			String outputFile, int numHits) throws SAXException, IOException, ParserConfigurationException,
			Exception, NotBoundException {
		mQueries = new LinkedHashMap<String, String>();

		Configuration conf = new Configuration();
		mFileSystem = FileSystem.get(conf);
		String element = queriesFilePath;
		Document d = null;
		d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
				mFileSystem.open(new Path(element)));
		parseParameters(d);

		// make sure we have some queries to run
		if (mQueries.size() == 0) {
			throw new Exception("Must specify at least one query!");
		}

		mOutputFile = outputFile;
		mBrokerAddress = brokerAddr;
		mRuntag = mid;
		mNumHits = numHits;
	}

	public void runQueries() throws Exception {
		// initialize retrieval environment variables
		QueryRunner runner = null;
		try {
			runner = new BrokerQueryRunner(mBrokerAddress);
		} catch (Exception e) {
			e.printStackTrace();
		}

		for (String qid : mQueries.keySet()) {
			String rawQueryText = mQueries.get(qid);
			sLogger.info("query id:" + qid + ", query text:" + rawQueryText);
			runner.runQuery(qid, rawQueryText.split("\\s+"));
		}

		// where should we output these results?
		ResultWriter resultWriter = null;
		resultWriter = getResultWriter(mOutputFile, false);

		printResults(mRuntag, runner, resultWriter);
		// flush the result writer
		resultWriter.flush();
	}

	public ResultWriter getResultWriter(String fileName, boolean compress) throws Exception {
		return new ResultWriter(fileName, compress, mFileSystem);
	}

	private void parseParameters(Document d) throws Exception, RemoteException, NotBoundException {
		// parse query elements
		NodeList queries = d.getElementsByTagName("query");
		for (int i = 0; i < queries.getLength(); i++) {
			// query XML node
			Node node = queries.item(i);

			// get query id
			String queryID = XMLTools.getAttributeValue(node, "id", null);
			if (queryID == null) {
				throw new Exception("Must specify a query id attribute for every query!");
			}

			// get query text
			String queryText = node.getTextContent();

			// add query to lookup
			if (mQueries.get(queryID) != null) {
				throw new Exception(
						"Duplicate query ids not allowed! Already parsed query with id=" + queryID);
			}
			mQueries.put(queryID, queryText);
		}
	}

	protected void printResults(String runtag, QueryRunner runner, ResultWriter resultWriter)
			throws Exception {

		for (String qid : mQueries.keySet()) {
			// get the ranked list for this query
			Accumulator[] list = runner.getResults(qid);
			Map<Integer, String> queryDocnoMapping = ((BrokerQueryRunner) runner)
					.getDocnoMapping(qid);
			if (list == null) {
				sLogger.info("null results for: " + qid);
				throw new RuntimeException("null results for: \"+queryID");
			}
			if (queryDocnoMapping == null) {
				sLogger.info("null docno mapping for: " + qid);
				throw new RuntimeException("null docno mapping for: \"+queryID");
			}
			for (int i = 0; i < list.length && i < mNumHits; i++) {
				resultWriter.println(qid + " Q0 " + queryDocnoMapping.get(list[i].docno) + " "
						+ (i + 1) + " " + list[i].score + " " + runtag);
			}
		}
	}
}
