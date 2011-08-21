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

package ivory.ltr;

import ivory.core.RetrievalEnvironment;
import ivory.core.exception.ConfigurationException;
import ivory.smrf.model.Clique;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.importance.ConceptImportanceModel;
import ivory.smrf.model.importance.LinearImportanceModel;
import ivory.smrf.model.importance.MetaFeature;
import ivory.smrf.retrieval.BatchQueryRunner;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;


import edu.umd.cloud9.collection.DocnoMapping;

/**
 * @author Don Metzler
 *
 */
public class ExtractFeatures {

	private static final double DEFAULT_FEATURE_VALUE = 0.0;     // default feature value

	private static final String QUERY_FEATURE_NAME = "qid";      // query id feature name
	private static final String DOC_FEATURE_NAME = "docid";      // document id feature name
	private static final String JUDGMENT_FEATURE_NAME = "grade"; // relevance grade feature name

	private BatchQueryRunner runner = null;     // batch query runner
	private RetrievalEnvironment env = null;    // retrieval environment
	private Map<String, String> queries = null; // query id -> query text mapping
	private DocnoMapping docnoMapping = null;   // docno mapping

	public ExtractFeatures(String [] args, FileSystem fs) throws SAXException, IOException, ParserConfigurationException, NotBoundException, Exception {
		loadQueryRunner(args, fs);
		env = runner.getRetrievalEnvironment();
		queries = runner.getQueries();
		docnoMapping = env.getDocnoMapping();
	}
	
	public void loadQueryRunner(String [] args, FileSystem fs) throws ConfigurationException{
		runner = new BatchQueryRunner(args, fs);
	}
	

	private void extract() throws Exception {
		// models specified in parameter files
		Set<String> modelNames = runner.getModels();

		// feature importance models
		Collection<ConceptImportanceModel> importanceModels = env.getImportanceModels();

		// we only know how to deal with linear importance models, so filter the rest out
		List<LinearImportanceModel> linearImportanceModels = new ArrayList<LinearImportanceModel>();
		for(ConceptImportanceModel model : importanceModels) {
			if(model instanceof LinearImportanceModel) {
				linearImportanceModels.add((LinearImportanceModel)model);
			}
		}

		SortedSet<String> featureNames = new TreeSet<String>();
		for(Entry<String, String> queryEntry : queries.entrySet()) {
			// query text
			String queryText = queryEntry.getValue();

			// compute features for each model
			for(String modelName : modelNames) {
				// build mrf from model node
				Node modelNode = runner.getModel(modelName);
				MRFBuilder builder = MRFBuilder.get(env, modelNode);			
				MarkovRandomField mrf = builder.buildMRF(env.tokenize(queryText));

				// get mrf cliques
				List<Clique> cliques = mrf.getCliques();

				// add parameter name to feature name set
				for(Clique c : cliques) {
					// parameter id
					String paramId = c.getParameter().getName();

					// handle linear importance model weights
					if(importanceModels.size() != 0) {
						for(LinearImportanceModel model : linearImportanceModels) {
							List<MetaFeature> metaFeatures = model.getMetaFeatures();

							for(MetaFeature metaFeat : metaFeatures) {
								// feature id = modelName-metaFeatId-paramId
								String featId = modelName + "-" + metaFeat.getName() + "-" + paramId;
								featureNames.add(featId);
							}
						}
					}

					// feature id = modelName-paramId
					String featId = modelName + "-" + paramId;

					featureNames.add(featId);
				}
			}			
		}

		// add judgment feature name
		featureNames.add(JUDGMENT_FEATURE_NAME);

		// print feature name header
		System.out.print(QUERY_FEATURE_NAME + "\t" + DOC_FEATURE_NAME);
		for(String featureName : featureNames) {
			System.out.print("\t" + featureName);
		}
		System.out.println();

		// extract features query-by-query
		for(Entry<String, String> queryEntry : queries.entrySet()) {
			// feature map (docname -> feature name -> feature value)
			SortedMap<String,SortedMap<String,Double>> featureValues = new TreeMap<String,SortedMap<String,Double>>();

			// query id and text
			String qid = queryEntry.getKey();
			String queryText = queryEntry.getValue();

			// compute features for each model
			for(String modelName : modelNames) {
				// build mrf from model node
				Node modelNode = runner.getModel(modelName);
				MRFBuilder builder = MRFBuilder.get(env, modelNode);			
				MarkovRandomField mrf = builder.buildMRF(env.tokenize(queryText));

				// initialize mrf
				mrf.initialize();

				// get mrf cliques
				List<Clique> cliques = mrf.getCliques();

				// get docnodes associated with mrf
				ArrayList<DocumentNode> docNodes = new ArrayList<DocumentNode>();
				List<GraphNode> nodes = mrf.getNodes();
				for (GraphNode node : nodes) {
					if (node instanceof DocumentNode) {
						docNodes.add((DocumentNode) node);
					}
				}

				// get document set to extract features for
				Map<String,Double> origJudgments = runner.getJudgmentSet(qid);
				if(origJudgments == null) {
					System.err.println("Warning: no judgments found for qid = " + qid + " -- skipping!");
					continue;
				}

				// convert to docid -> judgment mapping
				SortedMap<Integer,Double> judgments = new TreeMap<Integer,Double>();
				Map<Integer,String> docIdToNameMap = new HashMap<Integer,String>();
				for(Entry<String,Double> judgmentEntry : origJudgments.entrySet()) {
					// document name
					String docName = judgmentEntry.getKey();

					// judgment
					double judgment = judgmentEntry.getValue();

					// doc id
					int docid = docnoMapping.getDocno(docName);

					// update maps
					judgments.put(docid, judgment);
					docIdToNameMap.put(docid, docName);
				}


				for(Entry<Integer,Double> judgmentEntry : judgments.entrySet()) {
					// document id
					int docid = judgmentEntry.getKey();

					// document name
					String docName = docIdToNameMap.get(docid);

					// get feature map for this docname
					SortedMap<String,Double> docFeatures = featureValues.get(docName);
					if(docFeatures == null) {
						docFeatures = new TreeMap<String,Double>();
						featureValues.put(docName, docFeatures);
					}

					// document judgment
					double judgment = judgmentEntry.getValue();

					// set judgment feature
					docFeatures.put(JUDGMENT_FEATURE_NAME, judgment);

					// initialize doc nodes
					for(DocumentNode node : docNodes) {
						node.setDocno(docid);
					}

					// compute potentials for each clique
					for(Clique c : cliques) {
						// parameter id
						String paramId = c.getParameter().getName();

						// handle linear importance model weights (for everything except query-independent clique types)
						if(importanceModels.size() != 0 && c.getType() != Clique.Type.Document) {
							for(LinearImportanceModel model : linearImportanceModels) {
								List<MetaFeature> metaFeatures = model.getMetaFeatures();

								for(MetaFeature metaFeat : metaFeatures) {
									// feature id = modelName-metaFeatId-paramId
									String featId = modelName + "-" + metaFeat.getName() + "-" + paramId;

									// score = meta-feature weight * (raw) clique potential
									double score = model.computeFeatureVal(c.getConcept(), metaFeat) * c.getPotential();

									// update feature values
									Double curVal = docFeatures.get(featId);
									if(curVal == null) {
										docFeatures.put(featId, score);
									}
									else {
										docFeatures.put(featId, curVal + score);
									}
								}
							}
						}

						// feature id = modelName-paramId
						String featId = modelName + "-" + paramId;

						// score = (raw) clique potential
						double score = c.getPotential();

						// update feature values
						Double curVal = docFeatures.get(featId);
						if(curVal == null) {
							docFeatures.put(featId, score);
						}
						else {
							docFeatures.put(featId, curVal + score);
						}

					}
				}
			}

			// print feature values for current query
			for(Entry<String, SortedMap<String, Double>> featureEntry : featureValues.entrySet()) {
				String docName = featureEntry.getKey();
				System.out.print(qid + "\t" + docName);
				Map<String,Double> docFeatures = featureEntry.getValue();
				for(String featureName : featureNames) {
					Double featVal = docFeatures.get(featureName);
					if(featVal == null) {
						featVal = DEFAULT_FEATURE_VALUE;
					}
					System.out.print("\t" + featVal);
				}
				System.out.println();
			}
		}
	}

	public static void main(String[] args) throws SAXException, ParserConfigurationException, NotBoundException, Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.getLocal(conf);

		ExtractFeatures extractor = new ExtractFeatures(args, fs);
		extractor.extract();
	}
}
