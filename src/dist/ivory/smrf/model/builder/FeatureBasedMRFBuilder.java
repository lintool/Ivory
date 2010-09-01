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

package ivory.smrf.model.builder;

import ivory.smrf.model.Clique;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.MetaFeature;
import ivory.smrf.model.TermNode;

import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author Don Metzler
 * @author Lidan Wang
 */
public class FeatureBasedMRFBuilder extends MRFBuilder {

	private static final Logger sLogger = Logger.getLogger(FeatureBasedMRFBuilder.class);

	/* Some data structures necessary for WSD model */
	private static LinkedList<MetaFeature> metaFeatureSet = new LinkedList<MetaFeature>();

        //unigram and bigram stats from collection and external sources
        private static Map<String,String> ClueCF_external = new HashMap<String,String>();
        private static Map<String,String> EnWikiCF_external = new HashMap<String,String>();
        private static Map<String,String> collectionCF = new HashMap<String,String>();
        private static Map<String,String> collectionDF = new HashMap<String,String>();

	//Meta feature names
        private static String COLL_FREQ_FEATURE = "collection_freq";
        private static String DOC_FREQ_FEATURE = "document_freq";
        private static String CLUE_CF_FEATURE = "clue_cf";
        private static String ENWIKI_CF_FEATURE = "enwiki_cf";
                                        
        //Collection names
        private static String CLUE = "clue"; //external data
        private static String CLUE_COLLECTION = "clue_collection";
        private static String ENWIKI = "enwiki"; //external data
        private static String TREC45 = "trec45";
        private static String GOV2 = "gov2";
        private static String WT10G = "wt10g";


        private static String currentCollection;
           
	private static String modelType;

	private static long numDocsCollection;

	protected RetrievalEnvironment env= null;

	static {
		sLogger.setLevel(Level.WARN);
	}

	/**
	 * XML specification of features
	 */
	private Node mModel = null;

	/**
	 * @param env
	 * @param model
	 */
	public FeatureBasedMRFBuilder(RetrievalEnvironment env, Node model) throws Exception{
		super(env);
		mModel = model;
		this.env = env;

		numDocsCollection = env.documentCount();

		//name of the collection, e.g., clue_collection, wt10g, trec45.
		currentCollection = XMLTools.getAttributeValue(model, "collection", null);

		modelType = XMLTools.getAttributeValue(model, "type", null);

		if (!(modelType.equals("Feature")) && !(modelType.equals("WSD"))) {
			throw new Exception ("Exception: Invalid model type "+modelType);
		}

		if (modelType.equals("WSD")){
                        if (currentCollection == null){
                                throw new Exception ("Exception: Must specify the name of the collection.");
                        }
		}

		System.out.println("FeatureBasedMRFBuilder.java: current collection is "+currentCollection+"; model type is "+modelType);
	}

	@Override
	public MarkovRandomField buildMRF(String[] queryTerms) throws Exception {
		sLogger.info("Building MRF according to the following model: "
				+ XMLTools.format(mModel.getOwnerDocument()));

		// this is the MRF we're building
		MarkovRandomField mrf = new MarkovRandomField(queryTerms, mEnv);

		// construct MRF feature by feature
		NodeList children = mModel.getChildNodes();
		
		if (modelType.equals("WSD")){
			
			for (int i = 0; i < children.getLength(); i++) {
				Node child = children.item(i);

				if ("metaFeature".equals(child.getNodeName())){

					//collection_freq, document_freq, clue_cf, or enwiki_cf
					String metaFeatureName = XMLTools.getAttributeValue(child, "id", "");

					double metaFeatureWeight = XMLTools.getAttributeValue(child, "weight", -1.0);
				
					if (metaFeatureName=="" || metaFeatureWeight == -1){
						throw new Exception ("Exception: Must specify meta feature name and weight.");
					}

					MetaFeature mf = new MetaFeature (metaFeatureName, metaFeatureWeight);
					metaFeatureSet.add(mf);

					String file = XMLTools.getAttributeValue(child, "file", null);

					if (file==null){
						throw new Exception ("Exception: Must specify the location of the meta feature stats file.");
					}

					readDataStats (file, metaFeatureName);
				}			
			}


			if (metaFeatureSet.size() == 0){
				throw new Exception ("Exception: Must specify meta features for WSD model.");
			}

			//normalize meta feature weights
                	double sum = 0;
                	for (int i = 0; i < metaFeatureSet.size(); i++){
                        	MetaFeature mf = (MetaFeature) metaFeatureSet.get(i);
                        	sum += mf.getWeight();
                	}
                	double w;
                	for (int i = 0; i < metaFeatureSet.size(); i++){
                       		MetaFeature mf = (MetaFeature) metaFeatureSet.get(i);
                        	w = mf.getWeight() / sum;
                        	mf.setWeight(w);
                	}                       
		}


		for (int i = 0; i < children.getLength(); i++) {

			Node child = children.item(i);

			if ("feature".equals(child.getNodeName())) {

				// get the feature id
				String featureID = XMLTools.getAttributeValue(child, "id", "");
				if (featureID.equals("")) {
					throw new Exception("Exception: Each feature must specify an id attribute!");
				}

				// get feature weight (default = 1.0)
				double weight = XMLTools.getAttributeValue(child, "weight", 1.0);

				// get feature scale factor
				double scaleFactor = XMLTools.getAttributeValue(child, "scaleFactor", 1.0);

				String cliqueSetType = XMLTools.getAttributeValue(child, "cliqueSet", "");

				if (cliqueSetType.equals("")) {
					mrf.addClique(CliqueFactory.buildDocumentClique(mEnv, child));
				} else {
					// construct the clique set
					CliqueSet cliqueSet = CliqueSet.create(cliqueSetType, mEnv, queryTerms, child);

					// get cliques from clique set
					List<Clique> cliques = cliqueSet.getCliques();

					// add cliques to MRF
					for (Clique c : cliques) {
						c.setParameterID(featureID);
						c.setCliqueType(cliqueSet.getType());

						//c.setWeight(weight);

						if (modelType.equals("WSD")){

							// need clique terms in order to compute query-dependent weight
                                        		String cliqueTerms = "";

							List<ivory.smrf.model.GraphNode> mNodes = c.getNodes();

							//get clique terms
							for( Iterator<ivory.smrf.model.GraphNode> nodeIter = mNodes.iterator(); nodeIter.hasNext(); ) {
								try{
                                                        		TermNode tnode = (TermNode) (nodeIter.next());
                                                        		String term = tnode.getTerm();
                                                
                                                        		cliqueTerms += term+" ";
                                                
                                                        		//System.out.println("In FeatureBasedBuilder term is "+term+" ");
                                                		}
                                                		catch(Exception e){}
                                        		}
							cliqueTerms = cliqueTerms.trim();

							if (cliqueTerms == ""){
								throw new Exception ("Exception: Invalid clique terms.");
							}

							c.setCliqueTerms (cliqueTerms);
						
							//System.out.println("Clique terms: "+cliqueTerms+", clique type: " +cliqueType);

							//compute query-dependent clique weight
							double wgt = 0;
							for (int w = 0; w < metaFeatureSet.size(); w++){
								MetaFeature mf = (MetaFeature) (metaFeatureSet.get(w));
								
								//skip using clue external feature for clue data
								if (mf.getName().indexOf("clue")!=-1 && currentCollection.indexOf("clue")!=-1){}

								else{
									double metaWeight = mf.getWeight();
									double cliqueFeatureVal = computeFeatureVal (c, mf);
									//System.out.println("Meta wgt and met value "+metaWeight+" "+cliqueFeatureVal);
									wgt += metaWeight * cliqueFeatureVal;
								}
							}
							c.setCliqueWeight (wgt);
						}
						else{ 
							c.setCliqueWeight (weight);
						}

						c.setScaleFactor(scaleFactor);
						mrf.addClique(c);
					}
				}
			}
		}

		return mrf;
	}


	//read in the meta feature statistics
        public static void readDataStats (String file, String type){
                                                
                try {
                                         
                        FileInputStream  file_stream = new FileInputStream(file);
                                        
                        try {
                                BufferedReader infile = new BufferedReader(new InputStreamReader(file_stream));
                                        
                                String line = "";
                                 
                                while((line=infile.readLine())!=null){
                                        String [] tokens = (line.trim()).split("\\s+");
                                        String concept = "";
                                        for (int i=0; i<tokens.length-1; i++){
                                                concept += tokens[i]+" ";
                                        }
                                        concept = concept.trim();
                        
                                        String count = tokens[tokens.length-1];
                
                                        if (type.equals (COLL_FREQ_FEATURE)){
                                                collectionCF.put(concept, count);
                                        }
                                        else if (type.equals (DOC_FREQ_FEATURE)){
                                                collectionDF.put(concept, count);
                                        }
                                        else if (type.equals (CLUE_CF_FEATURE)){
                                                ClueCF_external.put(concept, count);
                                        }
                                        else if (type.equals (ENWIKI_CF_FEATURE)){
                                                EnWikiCF_external.put(concept, count);
                                        }
                                        else{
                                                System.out.println("Invalid type: "+type);
                                                System.exit(-1);
                                        }
        
                                }       
                
                        } catch(Exception e){
                                System.out.println("An error occurs: "+e);
                        }
                }
                catch(Exception e){
                        System.out.println("An error erros: "+e);   
                }
        }


	public static double computeFeatureVal (Clique clique, MetaFeature f) throws Exception {
   
                String cliqueTerms = clique.getCliqueTerms();
                String cliqueType = clique.getCliqueType();
                String featureName = f.getName();
        
		if (!(cliqueType.equals("Term")) && !(cliqueType.equals("Ordered")) && !(cliqueType.equals("Unordered"))){
			throw new Exception ("Exception: Invalid clique type "+cliqueType);
		}

                double v1 = 0, v2 = 0;
         
                if (featureName.equals(DOC_FREQ_FEATURE)){
                        v1 = getCount (cliqueTerms, DOC_FREQ_FEATURE);
                        v2 = Math.log (numDocsCollection/v1);
                }
                 
                else if (featureName.equals(COLL_FREQ_FEATURE)
                                || featureName.equals(CLUE_CF_FEATURE)
                                || featureName.equals(ENWIKI_CF_FEATURE)){
                        v1 = getCount (cliqueTerms, f.getName());
                        v2 = Math.log(v1);
                }
                else {
                        System.out.println("Invalid meta feature name: "+f.getName());
                        System.exit(-1);
                }
        
                //doing some normalizations
                if (cliqueType.equals("Term")){
                        if (featureName.equals (COLL_FREQ_FEATURE)) {
                                if (currentCollection.indexOf("clue")!=-1){
                                        v2 = 1-Math.exp(-0.005*v2);   
                                }
                                else{
                                        v2 = 1-Math.exp(-0.008*v2);
                                }
                        }
                        else if (featureName.equals(DOC_FREQ_FEATURE)){
                                v2 = 1-Math.exp(-0.005*v2);
                        }
                        //external stats
                        else if (featureName.equals(ENWIKI_CF_FEATURE)){
                                v2 = 1-Math.exp(-0.001*v2);
                        }
                        else if (featureName.equals(CLUE_CF_FEATURE)){
                                v2 = 1-Math.exp(-0.0008*v2);
                        }
                        else {
                                throw new Exception ("Exception: Feature not supported "+featureName);
                        }
                }
                else{
                        if (featureName.equals (COLL_FREQ_FEATURE)) {
                                v2 = 1-Math.exp(-0.0005*v2);
                        }
                        else if (featureName.equals(DOC_FREQ_FEATURE)){
                                v2 = 1-Math.exp(-0.0008*v2);
                        }
                        else if (featureName.equals(ENWIKI_CF_FEATURE)){
                                v2 = 1-Math.exp(-0.001*v2);
                        }
                        else if (featureName.equals(CLUE_CF_FEATURE)){
                                v2 = 1-Math.exp(-0.0001*v2);
                        }
                        else {
                                throw new Exception ("Exception: Feature not supported "+featureName);
                        }
                }
                                
                return v2;
        }


        // get concept stats
        public static double getCount (String concept, String type){
        
                String r = "";
                
                if (type.equals (COLL_FREQ_FEATURE)){
                        r = (String)(collectionCF.get(concept));
                }
                else if (type.equals (DOC_FREQ_FEATURE)){
                        r = (String)(collectionDF.get(concept));
                }
                else if (type.equals (CLUE_CF_FEATURE)){
                        r = (String)(ClueCF_external.get(concept));
                }
                else if (type.equals (ENWIKI_CF_FEATURE)){
                        r = (String)(EnWikiCF_external.get(concept));
                }
                else {
                        System.out.println("Invalid type: "+type);
                        System.exit(-1);
                }
                        
                double count = 1;
                
                if (r != null){
                        count = Double.parseDouble(r) + 1;
                }

                //System.out.println ("Returning "+count+" for concept "+concept+", type "+type);
 
                return count;
        }


	public static LinkedList<MetaFeature> getMetaFeatureList(){
		return metaFeatureSet;
	}

	public static String getModelType() {
		return modelType;
	}

	public static String getCurrentCollection(){
		return currentCollection;
	}
}
