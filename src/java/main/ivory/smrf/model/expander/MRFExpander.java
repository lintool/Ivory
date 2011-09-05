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

package ivory.smrf.model.expander;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.document.IntDocVector;
import ivory.core.data.document.IntDocVector.Reader;
import ivory.core.exception.ConfigurationException;
import ivory.core.exception.RetrievalException;
import ivory.core.util.XMLTools;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.Parameter;
import ivory.smrf.model.VocabFrequencyPair;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.importance.ConceptImportanceModel;
import ivory.smrf.retrieval.Accumulator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.umd.cloud9.util.map.HMapIV;

/**
 * @author Don Metzler
 */
public abstract class MRFExpander {
	protected RetrievalEnvironment env = null;  // Ivory retrieval environment.
	protected int numFeedbackDocs;              // Number of feedback documents.
	protected int numFeedbackTerms;             // Number of feedback terms.
	protected Set<String> stopwords = null;     // Stopwords list.

	// The expansion MRF cliques should be scaled according to this weight.
	protected float expanderWeight;
	
  // Maximum number of candidates to consider for expansion; non-positive numbers result in all
  // candidates being considered.
	protected int maxCandidates = 0;

	/**
	 * @param mrf
	 * @param results
	 */
	public abstract MarkovRandomField getExpandedMRF(MarkovRandomField mrf, Accumulator[] results)
			throws ConfigurationException;

	/**
	 * @param words
	 *            list of words to ignore when constructing expansion concepts
	 */
	public void setStopwordList(Set<String> words) {
		this.stopwords = Preconditions.checkNotNull(words);
	}

	public void setMaxCandidates(int maxCandidates) {
		this.maxCandidates = maxCandidates;
	}

	/**
	 * @param env
	 * @param model
	 * @throws Exception
	 */
  public static MRFExpander getExpander(RetrievalEnvironment env, Node model)
      throws ConfigurationException {
		Preconditions.checkNotNull(env);
		Preconditions.checkNotNull(model);

		// get model type
		String expanderType = XMLTools.getAttributeValueOrThrowException(model, "type",
		    "Expander type must be specified!");

		// get normalized model type
		String normExpanderType = expanderType.toLowerCase().trim();

		// build the expander
		MRFExpander expander = null;
		
		if ("unigramlatentconcept".equals(normExpanderType)) {
			int fbDocs = XMLTools.getAttributeValue(model, "fbDocs", 10);
			int fbTerms = XMLTools.getAttributeValue(model, "fbTerms", 10);
			float expanderWeight = XMLTools.getAttributeValue(model, "weight", 1.0f);

			List<Parameter> parameters = Lists.newArrayList();
			List<Node> scoreFunctionNodes = Lists.newArrayList();
			List<ConceptImportanceModel> importanceModels = Lists.newArrayList();

			// get the expandermodel, which describes how to actually
			// build the expanded MRF
			NodeList children = model.getChildNodes();
			for (int i = 0; i < children.getLength(); i++) {
				Node child = children.item(i);
				if ("conceptscore".equals(child.getNodeName())) {
					String paramID = XMLTools.getAttributeValueOrThrowException(child, "id",
					    "conceptscore node must specify an id attribute!");

					float weight = XMLTools.getAttributeValue(child, "weight", 1.0f);

					parameters.add(new Parameter(paramID, weight));
					scoreFunctionNodes.add(child);

					// get concept importance source (if applicable)
					ConceptImportanceModel importanceModel = null;
					String importanceSource = XMLTools.getAttributeValue(child, "importance", null);
          if (importanceSource != null) {
            importanceModel = env.getImportanceModel(importanceSource);
            if (importanceModel == null) {
              throw new RetrievalException("Error: importancemodel " + importanceSource + " not found!");
            }
          }
					importanceModels.add(importanceModel);
				}
			}

			// make sure there's at least one expansion model specified
			if (scoreFunctionNodes.size() == 0) {
				throw new ConfigurationException("No conceptscore specified!");
			}
			
			// create the expander
			expander = new UnigramLatentConceptExpander(env, fbDocs, fbTerms, expanderWeight, parameters,
					scoreFunctionNodes, importanceModels);

			// maximum number of candidate expansion terms to consider per query
			int maxCandidates = XMLTools.getAttributeValue(model, "maxCandidates", 0);
			if (maxCandidates > 0) {
				expander.setMaxCandidates(maxCandidates);
			}
		} else if ("latentconcept".equals(normExpanderType)) {
			int defaultFbDocs = XMLTools.getAttributeValue(model, "fbDocs", 10);
			int defaultFbTerms = XMLTools.getAttributeValue(model, "fbTerms", 10);

			List<Integer> gramList = new ArrayList<Integer>();
			List<MRFBuilder> builderList = new ArrayList<MRFBuilder>();
			List<Integer> fbDocsList = new ArrayList<Integer>();
			List<Integer> fbTermsList = new ArrayList<Integer>();

			// get the expandermodel, which describes how to actually
			// build the expanded MRF
			NodeList children = model.getChildNodes();
			for (int i = 0; i < children.getLength(); i++) {
				Node child = children.item(i);
				if ("expansionmodel".equals(child.getNodeName())) {
					int gramSize = XMLTools.getAttributeValue(child, "gramSize", 1);
					int fbDocs = XMLTools.getAttributeValue(child, "fbDocs", defaultFbDocs);
					int fbTerms = XMLTools.getAttributeValue(child, "fbTerms", defaultFbTerms);

					// set MRF builder parameters
					gramList.add(gramSize);
					builderList.add(MRFBuilder.get(env, child));
					fbDocsList.add(fbDocs);
					fbTermsList.add(fbTerms);
				}
			}

			// make sure there's at least one expansion model specified
			if (builderList.size() == 0) {
				throw new ConfigurationException("No expansionmodel specified!");
			}

			// create the expander
			expander = new NGramLatentConceptExpander(env, gramList, builderList, fbDocsList,
					fbTermsList);

			// maximum number of candidate expansion terms to consider per query
			int maxCandidates = XMLTools.getAttributeValue(model, "maxCandidates", 0);
			if (maxCandidates > 0) {
				expander.setMaxCandidates(maxCandidates);
			}
		} else {
			throw new ConfigurationException("Unrecognized expander type -- " + expanderType);
		}

		return expander;
	}

	@SuppressWarnings("unchecked")
	protected TFDoclenStatistics getTFDoclenStatistics(IntDocVector[] docVecs) throws IOException {
		Preconditions.checkNotNull(docVecs);
		
		Map<String, Integer> vocab = Maps.newHashMap();
		Map<String, Short>[] tfs = new HashMap[docVecs.length];
		int[] doclens = new int[docVecs.length];

		for (int i = 0; i < docVecs.length; i++) {
			IntDocVector doc = docVecs[i];

			Map<String, Short> docTfs = new HashMap<String, Short>();
			int doclen = 0;

			Reader dvReader = doc.getReader();
			while (dvReader.hasMoreTerms()) {
				int termid = dvReader.nextTerm();
				String stem = env.getTermFromId(termid);
				short tf = dvReader.getTf();

				doclen += tf;

				if (stem != null && (stopwords == null || !stopwords.contains(stem))) {
					Integer df = vocab.get(stem);
					if (df != null) {
						vocab.put(stem, df + 1);
					} else {
						vocab.put(stem, 1);
					}
				}

				docTfs.put(stem, tf);
			}

			tfs[i] = docTfs;
			doclens[i] = doclen;
		}

		// sort the vocab hashmap according to tf
		VocabFrequencyPair[] entries = new VocabFrequencyPair[vocab.size()];
		int entryNum = 0;
		for (Entry<String, Integer> entry : vocab.entrySet()) {
			entries[entryNum++] = new VocabFrequencyPair(entry.getKey(), entry.getValue());
		}
		Arrays.sort(entries);

		return new TFDoclenStatistics(entries, tfs, doclens);
	}

	/**
	 * @param docVecs
	 * @param gramSize
	 * @throws IOException
	 */
	protected VocabFrequencyPair[] getVocabulary(IntDocVector[] docVecs, int gramSize)
			throws IOException {
		Map<String, Integer> vocab = new HashMap<String, Integer>();

		for (IntDocVector doc : docVecs) {
			HMapIV<String> termMap = new HMapIV<String>();
			int maxPos = Integer.MIN_VALUE;

			Reader dvReader = doc.getReader();
			while (dvReader.hasMoreTerms()) {
				int termid = dvReader.nextTerm();
				String stem = env.getTermFromId(termid);
				int[] pos = dvReader.getPositions();
				for (int i = 0; i < pos.length; i++) {
					termMap.put(pos[i], stem);
					if (pos[i] > maxPos) {
						maxPos = pos[i];
					}
				}
			}

			// grab all grams of size gramSize that do not contain
			// any out of vocabulary terms
			for (int pos = 0; pos <= maxPos + 1 - gramSize; pos++) {
				String concept = new String();
				boolean toAdd = true;
				for (int offset = 0; offset < gramSize; offset++) {
					String stem = termMap.get(pos + offset);
					
					if (stem == null || (stopwords != null && stopwords.contains(stem))) {
						toAdd = false;
						break;
					}
					
					if (offset == gramSize - 1) {
						concept += stem;
					} else {
						concept += stem + " ";
					}
				}
				
				if (toAdd) {
					Integer tf = vocab.get(concept);
					if (tf != null) {
						vocab.put(concept, tf + 1);
					} else {
						vocab.put(concept, 1);
					}
				}
			}
		}

		// sort the vocab hashmap according to tf
		VocabFrequencyPair[] entries = new VocabFrequencyPair[vocab.size()];
		int entryNum = 0;
		for (Entry<String, Integer> entry : vocab.entrySet()) {
			entries[entryNum++] = new VocabFrequencyPair(entry.getKey(), entry.getValue());
		}

		Arrays.sort(entries);

		return entries;
	}

	protected class TFDoclenStatistics {
		private VocabFrequencyPair[] vocab = null;
		private Map<String, Short>[] tfs = null;
		private int[] doclengths = null;

		public TFDoclenStatistics(VocabFrequencyPair[] entries, Map<String, Short>[] tfs,
				int[] doclengths) {
			this.vocab = Preconditions.checkNotNull(entries);
			this.tfs = Preconditions.checkNotNull(tfs);
			this.doclengths = Preconditions.checkNotNull(doclengths);
		}

		public VocabFrequencyPair[] getVocab() {
			return vocab;
		}

		public Map<String, Short>[] getTfs() {
			return tfs;
		}

		public int[] getDoclens() {
			return doclengths;
		}
	}
}
