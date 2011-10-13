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



import ivory.core.exception.ConfigurationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 * @author Don Metzler
 *
 */
public class GreedyLearn {

	private static final double TOLERANCE = 0.0001;

	public void train(String featFile, String modelOutputFile, int numModels, String metricClassName, boolean pruneCorrelated, double correlationThreshold, boolean logFeatures, boolean productFeatures, boolean quotientFeatures, int numThreads) throws IOException, InterruptedException, ExecutionException, ConfigurationException, InstantiationException, IllegalAccessException, ClassNotFoundException {		
		// read training instances
		Instances trainInstances = new Instances(featFile);

		// get feature map (mapping from feature names to feature number)
		Map<String,Integer> featureMap = trainInstances.getFeatureMap();

		// construct initial model
		Model initialModel = new Model();

		// initialize feature pools
		Map<Model,ArrayList<Feature>> featurePool = new HashMap<Model,ArrayList<Feature>>();
		featurePool.put(initialModel, new ArrayList<Feature>());

		// add simple features to feature pools
		for(String featureName : featureMap.keySet()) {
			featurePool.get(initialModel).add(new SimpleFeature(featureMap.get(featureName), featureName));
		}

		// eliminate document-independent features
		List<Feature> constantFeatures = new ArrayList<Feature>();
		for(int i = 0; i < featurePool.size(); i++) {
			Feature f = featurePool.get(initialModel).get(i);
			if(trainInstances.featureIsConstant(f)) {
				System.err.println("Feature " + f.getName() + " is constant -- removing from feature pool!");
				constantFeatures.add(f);
			}
		}
		featurePool.get(initialModel).removeAll(constantFeatures);

		// initialize score tables
		Map<Model,ScoreTable> scoreTable = new HashMap<Model,ScoreTable>();
		scoreTable.put(initialModel, new ScoreTable(trainInstances));

		// initialize model queue
		List<Model> models = new ArrayList<Model>();
		models.add(initialModel);

		// set up threading
		ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);

		Map<Model,ArrayList<ArrayList<Feature>>> featureBatches = new HashMap<Model,ArrayList<ArrayList<Feature>>>();
		featureBatches.put(initialModel, new ArrayList<ArrayList<Feature>>());

		for(int i = 0; i < numThreads; i++) {
			featureBatches.get(initialModel).add(new ArrayList<Feature>());
		}

		for(int i = 0; i < featurePool.get(initialModel).size(); i++) {
			featureBatches.get(initialModel).get(i % numThreads).add(featurePool.get(initialModel).get(i));
		}

		// greedily add features
		double curMetric = 0.0;
		double prevMetric = Double.NEGATIVE_INFINITY;
		int iter = 1;

		while(curMetric - prevMetric > TOLERANCE ) {

			Map<ModelFeaturePair,AlphaMeasurePair> modelFeaturePairMeasures = new HashMap<ModelFeaturePair,AlphaMeasurePair>();

			// update models
			for(Model model : models) {
				List<Future<Map<Feature,AlphaMeasurePair>>> futures = new ArrayList<Future<Map<Feature,AlphaMeasurePair>>>();
				for(int i = 0; i < numThreads; i++) {
					// construct measure
					Measure metric = (Measure)Class.forName(metricClassName).newInstance();

					// line searcher
					LineSearch search = new LineSearch(model, featureBatches.get(model).get(i), scoreTable.get(model), metric);
					
					Future<Map<Feature,AlphaMeasurePair>> future = threadPool.submit(search);
					futures.add(future);
				}

				for(int i = 0; i < numThreads; i++) {
					Map<Feature,AlphaMeasurePair> featAlphaMeasureMap = futures.get(i).get();
					for(Feature f : featAlphaMeasureMap.keySet()) {
						AlphaMeasurePair featAlphaMeasure = featAlphaMeasureMap.get(f);
						modelFeaturePairMeasures.put(new ModelFeaturePair(model, f), featAlphaMeasure);
					}
				}				
			}

			// sort model-feature pairs
			List<ModelFeaturePair> modelFeaturePairs = new ArrayList<ModelFeaturePair>(modelFeaturePairMeasures.keySet());
			Collections.sort(modelFeaturePairs, new ModelFeatureComparator(modelFeaturePairMeasures));

			// preserve current list of models
			List<Model> oldModels = new ArrayList<Model>(models);

			// add best model feature pairs to pool
			models = new ArrayList<Model>();

			//Lidan: here consider top-K features, rather than just the best one

			for(int i = 0; i < numModels; i++) {
				Model model = modelFeaturePairs.get(i).model;
				Feature feature = modelFeaturePairs.get(i).feature;
				String bestFeatureName = feature.getName();
				AlphaMeasurePair bestAlphaMeasure = modelFeaturePairMeasures.get(modelFeaturePairs.get(i));

				System.err.println("Model = " + model);
				System.err.println("Best feature: " + bestFeatureName);
				System.err.println("Best alpha: " + bestAlphaMeasure.alpha);
				System.err.println("Best measure: " + bestAlphaMeasure.measure);

				Model newModel = new Model(model);
				models.add(newModel);

				ArrayList<ArrayList<Feature>> newFeatureBatch = new ArrayList<ArrayList<Feature>>();
				for(ArrayList<Feature> fb : featureBatches.get(model)) {
					newFeatureBatch.add(new ArrayList<Feature>(fb));
				}
				featureBatches.put(newModel, newFeatureBatch);
				featurePool.put(newModel, new ArrayList<Feature>(featurePool.get(model)));

				// add auxiliary features (for atomic features only)
				if(featureMap.containsKey(bestFeatureName)) {
					int bestFeatureIndex = featureMap.get(bestFeatureName);

					// add log features, if requested
					if(logFeatures) {
						Feature logFeature = new LogFeature(bestFeatureIndex, "log(" + bestFeatureName + ")");
						featureBatches.get(newModel).get(bestFeatureIndex % numThreads).add(logFeature);
						featurePool.get(newModel).add(logFeature);
					}

					// add product features, if requested
					if(productFeatures) {
						for(String featureNameB : featureMap.keySet()) {
							int indexB = featureMap.get(featureNameB);
							Feature prodFeature = new ProductFeature(bestFeatureIndex, indexB, bestFeatureName + "*" + featureNameB);
							featureBatches.get(newModel).get(indexB % numThreads).add(prodFeature);
							featurePool.get(newModel).add(prodFeature);
						}
					}

					// add quotient features, if requested
					if(quotientFeatures) {
						for(String featureNameB : featureMap.keySet()) {
							int indexB = featureMap.get(featureNameB);
							Feature divFeature = new QuotientFeature(bestFeatureIndex, indexB, bestFeatureName + "/" + featureNameB);
							featureBatches.get(newModel).get(indexB % numThreads).add(divFeature);
							featurePool.get(newModel).add(divFeature);
						}
					}
				}

				// prune highly correlated features
				if(pruneCorrelated) {
					if(!newModel.containsFeature(feature)) {
						List<Feature> correlatedFeatures = new ArrayList<Feature>();

						for(Feature f : featurePool.get(newModel)) {
							if(f == feature) {
								continue;
							}
							double correl = trainInstances.getCorrelation(f, feature);
							if(correl > correlationThreshold) {
								System.err.println("Pruning highly correlated feature: " + f.getName());
								correlatedFeatures.add(f);
							}
						}

						for(ArrayList<Feature> batch : featureBatches.get(newModel)) {
							batch.removeAll(correlatedFeatures);
						}

						featurePool.get(newModel).removeAll(correlatedFeatures);
					}
				}

				// update score table
				if(iter == 0) {
					scoreTable.put(newModel, scoreTable.get(model).translate(feature, 1.0, 1.0));
					newModel.addFeature(feature, 1.0);
				}
				else {
					scoreTable.put(newModel, scoreTable.get(model).translate(feature, bestAlphaMeasure.alpha, 1.0 / (1.0 + bestAlphaMeasure.alpha)));
					newModel.addFeature(feature, bestAlphaMeasure.alpha);
				}
			}

			for(Model model : oldModels) {
				featurePool.remove(model);
				featureBatches.remove(model);
				scoreTable.remove(model);
			}

			// update metrics
			prevMetric = curMetric;
			curMetric = modelFeaturePairMeasures.get(modelFeaturePairs.get(0)).measure;

			iter++;
		}

		// serialize model
		System.out.println("Final Model: " + models.get(0));
		models.get(0).write(modelOutputFile);

		threadPool.shutdown();
	}

	public class ModelFeaturePair {
		public Model model;
		public Feature feature;

		public ModelFeaturePair(Model m, Feature f) {
			model = m;
			feature = f;
		}
	}

	public class ModelFeatureComparator implements Comparator<ModelFeaturePair> {

		private Map<ModelFeaturePair,AlphaMeasurePair> lookup = null;

		public ModelFeatureComparator(Map<ModelFeaturePair,AlphaMeasurePair> lookup) {
			this.lookup = lookup;
		}

		public int compare(ModelFeaturePair o1, ModelFeaturePair o2) {
			if(lookup.get(o1).measure > lookup.get(o2).measure) {
				return -1;
			}
			else if(lookup.get(o1).measure < lookup.get(o2).measure) {
				return 1;
			}
			return 0;
		}		
	}	

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Options options = new Options();

		options.addOption( OptionBuilder.withArgName("input").hasArg().withDescription("Input file that contains training instances.").isRequired().create("input") );
		options.addOption( OptionBuilder.withArgName("model").hasArg().withDescription("Model file to create.").isRequired().create("model") );		
		options.addOption( OptionBuilder.withArgName("numModels").hasArg().withDescription("Number of models to consider each iteration (default=1).").create("numModels") );
		options.addOption( OptionBuilder.withArgName("className").hasArg().withDescription("Java class name of metric to optimize for (default=ivory.ltr.NDCGMeasure)").create("metric") );
		options.addOption( OptionBuilder.withArgName("threshold").hasArg().withDescription("Feature correlation threshold for pruning (disabled by default).").create("pruneCorrelated") );
		options.addOption( OptionBuilder.withArgName("log").withDescription("Include log features (default=false).").create("log") );
		options.addOption( OptionBuilder.withArgName("product").withDescription("Include product features (default=false).").create("product") );
		options.addOption( OptionBuilder.withArgName("quotient").withDescription("Include quotient features (default=false).").create("quotient") );
		options.addOption( OptionBuilder.withArgName("numThreads").hasArg().withDescription("Number of threads to utilize (default=1).").create("numThreads") );

		HelpFormatter formatter = new HelpFormatter();
		CommandLineParser parser = new GnuParser();

		String trainFile = null;
		String modelOutputFile = null;

		int numModels = 1;

		String metricClassName = "ivory.ltr.NDCGMeasure";
		
		boolean pruneCorrelated = false;
		double correlationThreshold = 1.0;
		
		boolean logFeatures = false;
		boolean productFeatures = false;
		boolean quotientFeatures = false;

		int numThreads = 1;

		// parse the command-line arguments
		try {
			CommandLine line = parser.parse( options, args);

			if(line.hasOption("input")) {
				trainFile = line.getOptionValue("input");
			}

			if(line.hasOption("model")) {
				modelOutputFile = line.getOptionValue("model");
			}

			if(line.hasOption("numModels")) {
				numModels = Integer.parseInt(line.getOptionValue("numModels"));
			}

			if(line.hasOption("metric")) {
				metricClassName = line.getOptionValue("metric");
			}

			if(line.hasOption("pruneCorrelated")) {
				pruneCorrelated = true;
				correlationThreshold = Double.parseDouble(line.getOptionValue("pruneCorrelated"));
			}

			if(line.hasOption("numThreads")) {
				numThreads = Integer.parseInt(line.getOptionValue("numThreads"));
			}

			if(line.hasOption("log")) {
				logFeatures = true;
			}

			if(line.hasOption("product")) {
				productFeatures = true;
			}

			if(line.hasOption("quotient")) {
				quotientFeatures = true;
			}
		}
		catch(ParseException exp) {
			System.err.println(exp.getMessage());
		}		

		// were all of the required parameters specified?
		if(trainFile == null || modelOutputFile == null) {
			formatter.printHelp("GreedyLearn", options, true);
			System.exit(-1);
		}

		// learn the model
		try {
			GreedyLearn learn = new GreedyLearn();
			learn.train(trainFile, modelOutputFile, numModels, metricClassName, pruneCorrelated, correlationThreshold, logFeatures, productFeatures, quotientFeatures, numThreads);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
