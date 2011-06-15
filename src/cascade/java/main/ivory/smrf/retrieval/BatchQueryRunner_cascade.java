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

package ivory.smrf.retrieval;

import ivory.exception.ConfigurationException;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.expander.MRFExpander;
import ivory.smrf.model.importance.ConceptImportanceModel;
import ivory.util.ResultWriter;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;
import ivory.eval.Qrels_new;
import ivory.eval.RankedListEvaluator_new;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.LinkedList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.collection.DocnoMapping;

/**
 * @author Lidan Wang
 */

public class BatchQueryRunner_cascade extends BatchQueryRunner{

	//For each model (as key), store the cascade costs for all queries (as value)
	private HashMap cascadeCosts = new HashMap();
	private HashMap cascadeCosts_lastStage = new HashMap();

	private String [] internalOutputFiles;
	private String [] internalInputFiles;

	public LinkedList ndcgValues = new LinkedList();
	public LinkedList costKeys = new LinkedList();
	private String dataCollection = null;
	private int K_val;
	private int kVal;

	public BatchQueryRunner_cascade(String[] args, FileSystem fs) throws ConfigurationException {
		super (args, fs);
		parseParameters(args);

	}

	public HashMap readInternalInputFile(String internalInputFile){
		HashMap savedResults= new HashMap();
		
		if (internalInputFile!=null){
			BufferedReader in;
			try{
				in = new BufferedReader(new InputStreamReader(mFs.open(new Path(internalInputFile))));
				String line;
				 //Docnos and scores for a given query
				LinkedList results = new LinkedList();
				String qid = "";
				float [] docno_score = new float[2];

				while ((line = in.readLine()) != null && line.trim().length()> 0) {
					String [] tokens = line.split("\\s+");
					
					//# qid internal_docno score
					if (!(qid.equals(tokens[0]))){
						if (!(qid.equals(""))){
							savedResults.put(qid, results);
						}
						qid = tokens[0];
						results = new LinkedList();
					}
					docno_score = new float[2];
					//docno_score[0] = Integer.parseInt(qid);
					docno_score[0] = (float)(Double.parseDouble(tokens[1]));
					docno_score[1] = (float)(Double.parseDouble(tokens[2]));
					results.add(docno_score);
				}

				 //put last group of query and results in
				savedResults.put(qid, results);
			}
			catch (Exception e){
                                System.out.println("Problem reading "+internalInputFile);
                                System.exit(-1);
                        }

			if (savedResults.size()<=1){
				System.out.println("Should have results for more queries.");
				System.exit(-1);
			}
		}

		HashMap savedResults_return = new HashMap();
		
		Set set = savedResults.entrySet();
		Iterator itr = set.iterator();

		while (itr.hasNext()){
			Map.Entry me = (Map.Entry) itr.next();

			String key = (String)(me.getKey());
			LinkedList val = (LinkedList) (me.getValue());

			float [][] results = new float[val.size()][2];
			float [] r;
			for (int i=0; i<val.size(); i++){
				r = (float[]) (val.get(i));
				results[i][0] = r[0];
				results[i][1] = r[1];
			}

			savedResults_return.put(key, results);
		}		
		return savedResults_return;

	}

	public void runQueries() {

		//for each model, store cascade costs for all queries
		cascadeCosts = new HashMap();
		cascadeCosts_lastStage = new HashMap();

		int modelCnt = 0;

		for (String modelID : mModels.keySet()) {

			String internalInputFile = internalInputFiles[modelCnt];

			//Initialize mDocSet for each query if there is internalInputFile
			HashMap savedResults_prevStage = readInternalInputFile(internalInputFile);

			Node modelNode = mModels.get(modelID);
			Node expanderNode = mExpanders.get(modelID);

   			//K value for cascade
                        K_val = XMLTools.getAttributeValue(modelNode, "K", 0);
			kVal = XMLTools.getAttributeValue(modelNode, "topK", 0);

			if (kVal == 0){
				System.out.println("Should not be 0!");
				System.exit(-1);
			}
			RetrievalEnvironment.topK = kVal;

			// Initialize retrieval environment variables.
			QueryRunner_cascade runner = null;
			MRFBuilder builder = null;
			MRFExpander expander = null;

			try {
				// Get the MRF builder.
				builder = MRFBuilder.get(mEnv, modelNode.cloneNode(true));

				// Get the MRF expander.
				expander = null;
				if (expanderNode != null) {
					expander = MRFExpander.getExpander(mEnv, expanderNode.cloneNode(true));
				}
				if (mStopwords != null && mStopwords.size() != 0) {
					expander.setStopwordList(mStopwords);
				}

				int numHits = XMLTools.getAttributeValue(modelNode, "hits", 1000);
				if (K_val!=0){
					numHits = K_val;
				}				
				LOGGER.info("number of hits: " + numHits);

				// Multi-threaded query evaluation still a bit unstable; setting
				// thread=1 for now.
				runner = new ThreadedQueryRunner_cascade(builder, expander, 1, numHits, savedResults_prevStage, K_val);

				mQueryRunners.put(modelID, (QueryRunner)runner);

			} catch (Exception e) {
				e.printStackTrace();
			}

			for (String queryID : mQueries.keySet()) {
				String rawQueryText = mQueries.get(queryID);
				String[] queryTokens = mEnv.tokenize(rawQueryText);

				LOGGER.info(String.format("query id: %s, query: \"%s\"", queryID, rawQueryText));

				// Execute the query.
				runner.runQuery(queryID, queryTokens);
			}

			// Where should we output these results?
			Node model = mModels.get(modelID);
			String fileName = XMLTools.getAttributeValue(model, "output", null);
			boolean compress = XMLTools.getAttributeValue(model, "compress", false);


			String internalOutputFile = internalOutputFiles[modelCnt];

			try {
				ResultWriter resultWriter = new ResultWriter(fileName, compress, mFs);

				//print out representation that uses internal docno for next cascade stage if doing boosting training
				if (internalOutputFile!=null){

					ResultWriter resultWriter2 = new ResultWriter(internalOutputFile, compress, mFs);
					printResults(modelID, runner, resultWriter2, true);
					resultWriter2.flush();
				}

				printResults(modelID, runner, resultWriter, false);
				resultWriter.flush();
			} catch (IOException e) {
				throw new RuntimeException("Error: Unable to write results!");
			}


			cascadeCosts.put(modelID, runner.getCascadeCostAllQueries());

			cascadeCosts_lastStage.put(modelID, runner.getCascadeCostAllQueries_lastStage());

			modelCnt++;
		}

		//Compute evaluation metric
		float totalNDCG = 0, totalCost = 0;
		for (int i=0; i<costKeys.size(); i++){
			String [] tokens = ((String) costKeys.get(i)).split("\\s+");
			float cost = getCascadeCost(tokens[0], tokens[1]);
			float ndcg = Float.parseFloat((String) (ndcgValues.get(i)));

			totalNDCG+=ndcg;
			totalCost+=cost;
		}

		if (costKeys.size()!=ndcgValues.size()){
			System.out.println("They should be equal "+costKeys.size()+" "+ndcgValues.size());
			System.exit(-1);
		}
		System.out.println("Evaluation results... NDCG Sum "+totalNDCG+" TotalCost "+totalCost+" # queries with results "+costKeys.size()+" dataCollection "+dataCollection+" kVal "+kVal);
	}

	//The cascade cost of the qid under model
	public float getCascadeCost(String model, String qid){
		float [] allQueryCosts = (float[]) (cascadeCosts.get(model));
		return allQueryCosts[Integer.parseInt(qid)];
		//return Float.parseFloat((String)(cascadeCosts.get(model+" "+qid)));
	}

	public float getCascadeCost_lastStage(String model, String qid){
		float [] allQueryCosts_lastStage = (float[]) (cascadeCosts_lastStage.get(model));
		return allQueryCosts_lastStage[Integer.parseInt(qid)];
	}

	private void printResults(String modelID, QueryRunner_cascade runner, ResultWriter resultWriter, boolean internalDocno)
			throws IOException {

		float ndcgSum = 0;
		String qrelsPath = null;

		//Set up qrelsPath. 
		if (dataCollection.indexOf("wt10g")!=-1){
			if (mFs.exists(new Path("/user/lidan/qrels/qrels.wt10g"))){
				qrelsPath = "/user/lidan/qrels/qrels.wt10g";
			}
			else if (mFs.exists(new Path("/umd-lin/lidan/qrels/qrels.wt10g"))){
				qrelsPath = "/umd-lin/lidan/qrels/qrels.wt10g";
			}
			else if (mFs.exists(new Path("/fs/clip-trec/trunk_new/docs/data/wt10g/qrels.wt10g"))){
				qrelsPath = "/fs/clip-trec/trunk_new/docs/data/wt10g/qrels.wt10g";
			}
		}
		else if (dataCollection.indexOf("gov2")!=-1){
			if (mFs.exists(new Path("/user/lidan/qrels/qrels.gov2.all"))){
				qrelsPath = "/user/lidan/qrels/qrels.gov2.all";
			}			
			else if (mFs.exists(new Path("/umd-lin/lidan/qrels/qrels.gov2.all"))){
				qrelsPath = "/umd-lin/lidan/qrels/qrels.gov2.all";
			}
			else if (mFs.exists(new Path("/fs/clip-trec/trunk_new/docs/data/gov2/qrels.gov2.all"))){
				qrelsPath = "/fs/clip-trec/trunk_new/docs/data/gov2/qrels.gov2.all";
			}
		}
		else if (dataCollection.indexOf("clue")!=-1){
			if (mFs.exists(new Path("/user/lidan/qrels/qrels.web09catB.txt"))){
				qrelsPath = "/user/lidan/qrels/qrels.web09catB.txt";
			}
			else if (mFs.exists(new Path("/umd-lin/lidan/qrels/qrels.web09catB.txt"))){
				qrelsPath = "/umd-lin/lidan/qrels/qrels.web09catB.txt";
			}
			else if (mFs.exists(new Path("/fs/clip-trec/trunk_new/docs/data/clue/qrels.web09catB.txt"))){
				qrelsPath = "/fs/clip-trec/trunk_new/docs/data/clue/qrels.web09catB.txt";
			}

		}

		if (qrelsPath == null){
			System.out.println("Should have set qrelsPath!");
			System.exit(-1);
		}

		Qrels_new qrels = new Qrels_new(qrelsPath);
                DocnoMapping mapping = getDocnoMapping();

		if (K_val==0){
			//System.out.println("K value should be set already.");
			//System.exit(-1);
		}
		for (String queryID : mQueries.keySet()) {
			// Get the ranked list for this query.
			Accumulator[] list = runner.getResults(queryID);
			if (list == null) {
				LOGGER.info("null results for: " + queryID);
				continue;
			}

			float ndcg = (float) RankedListEvaluator_new.computeNDCG(kVal, list, mapping, qrels.getReldocsForQid(queryID, true)); 
			ndcgSum += ndcg;

			if (!internalDocno){
				if (qrels.getReldocsForQid(queryID, true).size()>0){ //if have qrels for this query
					//System.out.println("Lidan: NDCG for query "+queryID+" is "+ndcg);
					ndcgValues.add(ndcg+"");
					costKeys.add(modelID+" "+queryID); //save keys for cost. Evaluation metric is computed by the end of runQueries()
				}
			}
			//Lidan: print out -- qid internal_docno score, for next cascade stage
			if (internalDocno){
				for (int i = 0; i < list.length; i++) {
					//System.out.println("Lidan: print internal results "+queryID + " "+list[i].docno + " " + list[i].score);
					resultWriter.println(queryID + " "+list[i].docno + " " + list[i].score);
				}
			}

			else if (mDocnoMapping == null) {
				// Print results with internal docnos if unable to translate to
				// external docids.
				for (int i = 0; i < list.length; i++) {
					resultWriter.println(queryID + " Q0 " + list[i].docno + " " + (i + 1) + " "
							+ list[i].score + " " + modelID);
				}
			} else {
				// Translate internal docnos to external docids.
				for (int i = 0; i < list.length; i++) {
					resultWriter.println(queryID + " Q0 " + mDocnoMapping.getDocid(list[i].docno)
							+ " " + (i + 1) + " " + list[i].score + " " + modelID);

				}
			}
		}

	}


        private void parseParameters(String[] args) throws ConfigurationException {
                for (int i = 0; i < args.length; i++) {
                        String element = args[i];
                        Document d = null;
                
                        try {
      
                          d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
                                                        mFs.open(new Path(element)));
                        } catch (SAXException e) {
                                throw new ConfigurationException(e.getMessage());
                        } catch (IOException e) {
                                throw new ConfigurationException(e.getMessage());
                        } catch (ParserConfigurationException e) {
                                throw new ConfigurationException(e.getMessage());
                        }
                
                        parseModels(d);                        
                        parseIndexLocation(d);
                }
                                
                // Make sure we have some queries to run.
                if (mQueries.isEmpty()) {
                        throw new ConfigurationException("Must specify at least one query!");
                }
                        
                // Make sure there are models that need evaluated.
                if (mModels.isEmpty()) {
                        throw new ConfigurationException("Must specify at least one model!");
                }
                                                
                // Make sure we have an index to run against.
                if (mIndexPath == null) {
                        throw new ConfigurationException("Must specify an index!");
                }
        }       
	
	private void parseModels(Document d) throws ConfigurationException {

		NodeList models = d.getElementsByTagName("model");

		if (models.getLength() > 0){
			internalInputFiles = new String[models.getLength()];
			internalOutputFiles = new String[models.getLength()];
		}
		for (int i = 0; i < models.getLength(); i++) {
			// Get model XML node.
			Node node = models.item(i);


			// Get model id
			String modelID = XMLTools.getAttributeValue(node, "id", null);

			//Lidan: need to save to String[] internalInputFiles, indexed by modelID, in case there are multiple different internalInputFile, one for each model. 
			
			String internalInputFile = XMLTools.getAttributeValue(node, "internalInputFile", null);

			if (internalInputFile!=null && internalInputFile.trim().length() == 0){
				internalInputFile = null;
			}

			internalInputFiles[i] = internalInputFile;

			String internalOutputFile = XMLTools.getAttributeValue(node, "internalOutputFile", null);

			if (internalOutputFile!=null && internalOutputFile.trim().length() == 0){
				internalOutputFile = null;
			}
			internalOutputFiles[i] = internalOutputFile;


			if (modelID == null) {
				throw new ConfigurationException("Must specify a model id for every model!");
			}

			// Parse parent nodes.
			NodeList children = node.getChildNodes();
			for (int j = 0; j < children.getLength(); j++) {
				Node child = children.item(j);
				if ("expander".equals(child.getNodeName())) {
					if (mExpanders.containsKey(modelID)) {
						throw new ConfigurationException("Only one expander allowed per model!");
					}
					mExpanders.put(modelID, child);
				}
			}

			// Add model to internal map.
			/*
			if (mModels.get(modelID) != null) {
				throw new ConfigurationException(
						"Duplicate model ids not allowed! Already parsed model with id=" + modelID);
			}
			mModels.put(modelID, node);
			*/
		}
	}


	private void parseIndexLocation(Document d) throws ConfigurationException {
		NodeList index = d.getElementsByTagName("index");

		if (index.getLength() > 0) {
			/*
			if (mIndexPath != null) {
				throw new ConfigurationException(
						"Must specify only one index! There is no support for multiple indexes!");
			}
			mIndexPath = index.item(0).getTextContent();
			*/

			if (mIndexPath!=null){
				//System.out.println("The name of the index is "+mIndexPath);
				if (mIndexPath.toLowerCase().indexOf("wt10g")!=-1){
					dataCollection = "wt10g";
					RetrievalEnvironment.dataCollection = "wt10g";
				}
				else if (mIndexPath.toLowerCase().indexOf("gov2")!=-1){
					dataCollection = "gov2";
					RetrievalEnvironment.dataCollection = "gov2";
				}
				else if (mIndexPath.toLowerCase().indexOf("clue")!=-1){
					dataCollection = "clue";
					RetrievalEnvironment.dataCollection = "clue";
				}
				else{
					System.out.println("Invalid data collection "+mIndexPath);
					System.exit(-1);
				}
			}
		}
	}

}
