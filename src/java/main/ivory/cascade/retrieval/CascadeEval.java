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

package ivory.cascade.retrieval;

import ivory.cascade.model.CascadeClique;
import ivory.exception.ConfigurationException;
import ivory.exception.RetrievalException;
import ivory.smrf.model.Clique;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.score.ScoringFunction;
import ivory.smrf.retrieval.Accumulator;
import ivory.util.RetrievalEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author Lidan Wang
 */
public class CascadeEval {
  private static final Logger LOG = Logger.getLogger(CascadeEval.class);

  static int INITIAL_STAGE_NUM_RESULTS = 20000;

  /**
   * Pool of accumulators.
   */
  private CascadeAccumulator[] mAccumulators = null;

  /**
   * Sorted list of accumulators.
   */
  private final PriorityQueue<CascadeAccumulator> mSortedAccumulators = new PriorityQueue<CascadeAccumulator>();

  /**
   * Comparator used to sort cliques by their max score.
   */
  private final Comparator<Clique> maxScoreComparator = new Clique.MaxScoreComparator();

  /**
   * Markov Random Field that we are using to generate the ranking.
   */
  private MarkovRandomField mMRF = null;

  /**
   * If defined, only documents within this set will be scored.
   */
  private int[] mDocSet = null;
  float[] accumulated_scores = null;

  // Declare these so don't have to repeatedly declaring them in the methods
  double[] mDocSet_tmp;
  float[] accumulated_scores_tmp;
  int[] order;

  /**
   * MRF document nodes.
   */
  private List<DocumentNode> mDocNodes = null;

  /**
   * Maximum number of results to return.
   */
  private int mNumResults;

  // saved results from internalInputFile
  private float[][] mSavedResults;

  // K value used in cascade model
  private int mK;

  // Cost of this cascade model = # documents * sum of unit per document cost over the cliques
  float cascadeCost = 0;


  // docs that will be passed around
  int[][][] keptDocs;
  int[] keptDocLengths;

  // single terms in cliques used in first stage, which clique number they correspond to, keyed by
  // the concept, value is the cliqueNumber or termCollectionFrequency
  Map<String, Integer> termToCliqueNumber = Maps.newHashMap();
  Map<String, Long> cf = Maps.newHashMap();
  Map<String, Integer> df = Maps.newHashMap();

  // for pruning use
  float meanScore = 0;
  float stddev = 0;

  int numQueryTerms;

  public static int defaultNumDocs = 9999999;

  public CascadeEval(MarkovRandomField mrf, int numResults, String qid, float[][] savedResults,
      int K) {
    this(mrf, null, numResults, qid, savedResults, K);
  }

  public CascadeEval(MarkovRandomField mrf, int[] docSet, int numResults, String qid,
      float[][] savedResults, int K) {
    mMRF = mrf;
    mDocSet = docSet;
    mNumResults = numResults;
    mDocNodes = getDocNodes();
    mSavedResults = savedResults;
    mK = K;

    // Lidan: get # query terms
    numQueryTerms = mMRF.getQueryTerms().length;

    keptDocs = new int[INITIAL_STAGE_NUM_RESULTS + 1][numQueryTerms][];

    keptDocLengths = new int[INITIAL_STAGE_NUM_RESULTS + 1];
  }

  // Lidan: assuming mDocSet[] & accumulated_scores[] sorted by descending order of scores!
  // Lidan: this method modifies mDocSet[] & accumulated_scores[] (class variables)
  public void pruneDocuments(String pruner, float pruner_param) {

    // After pruning, make sure have max(RetrievalEnvironment.mCascade_K, |retained docs|)
    // documents!

    int[] mDocSet_tmp = new int[mDocSet.length];
    float[] accumulated_scores_tmp = new float[accumulated_scores.length];

    int retainSize = 0;

    if (pruner.equals("score")) {
      float max_score = accumulated_scores[0];
      float min_score = accumulated_scores[accumulated_scores.length - 1];

      float score_threshold = (max_score - min_score) * pruner_param + min_score;

      for (int i = 0; i < accumulated_scores.length; i++) {
        if (score_threshold <= accumulated_scores[i]) {
          retainSize++;
        } else {
          break;
        }
      }
    } else if (pruner.equals("mean-max")) {
      float max_score = accumulated_scores[0];
      float mean_score = 0;
      for (int j = 0; j < accumulated_scores.length; j++) {
        mean_score += accumulated_scores[j];
      }
      mean_score = mean_score / (float) accumulated_scores.length;
      float score_threshold = pruner_param * max_score + (1.0f - pruner_param) * mean_score;

      for (int i = 0; i < accumulated_scores.length; i++) {
        if (score_threshold <= accumulated_scores[i]) {
          retainSize++;
        } else {
          break;
        }
      }
    } else if (pruner.equals("rank")) {
      // if pruner_param = 0.3 --> remove bottom 30% of the docs!
      retainSize = (int) ((1.0 - pruner_param) * ((double) (mDocSet.length)));
    } else if (pruner.equals("z-score")) {
      // compute mean
      float avgScores = 0.0f;

      for (int i = 0; i < accumulated_scores.length; i++) {
        avgScores += accumulated_scores[i];
      }
      avgScores = avgScores / (float) accumulated_scores.length;

      // compute variance
      float variance = 0.0f;
      for (int i = 0; i < accumulated_scores.length; i++) {
        variance += (accumulated_scores[i] - avgScores) * (accumulated_scores[i] - avgScores);
      }
      float stddev = (float) Math.sqrt(variance);

      float[] z_scores = new float[accumulated_scores.length];
      for (int i = 0; i < z_scores.length; i++) {
        z_scores[i] = (accumulated_scores[i] - avgScores) / stddev;
      }
    } else {
      throw new RetrievalException("PruningFunction " + pruner + " is not supported!");
    }

    if (retainSize < mK) {
      if (mDocSet.length >= mK) {
        retainSize = mK;
      } else if (mK != defaultNumDocs) {
        // When training the model, set the # output docs large on purpose so that output size =
        // retained docs size

        retainSize = mDocSet.length;
      }
    }

    if (retainSize > mDocSet.length) {
      retainSize = mDocSet.length;
    }

    for (int i = 0; i < retainSize; i++) {
      mDocSet_tmp[i] = mDocSet[i];
      accumulated_scores_tmp[i] = accumulated_scores[i];
    }
    mDocSet = new int[retainSize];
    accumulated_scores = new float[retainSize];

    for (int i = 0; i < retainSize; i++) {
      mDocSet[i] = mDocSet_tmp[i];
      accumulated_scores[i] = accumulated_scores_tmp[i];
    }

  }

  // Lidan: operate on class vars mDocSet[] & accumulated_scores
  public void sortDocumentsByDocnos() {
    order = new int[mDocSet.length];
    mDocSet_tmp = new double[mDocSet.length];
    accumulated_scores_tmp = new float[mDocSet.length];

    for (int i = 0; i < order.length; i++) {
      order[i] = i;
      mDocSet_tmp[i] = mDocSet[i];
      accumulated_scores_tmp[i] = accumulated_scores[i];
    }

    ivory.smrf.model.constrained.ConstraintModel.Quicksort(mDocSet_tmp, order, 0, order.length - 1);

    for (int i = 0; i < order.length; i++) {
      mDocSet[i] = (int) mDocSet_tmp[i];
      accumulated_scores[i] = accumulated_scores_tmp[order[i]];
    }
  }

  // Total cost of the cascade model: # documents * sum of unit per document cost over each clique
  public float getCascadeCost() {
    // Lidan: should cast it to [0, 1]
    float normalizedCost = 1.0f - (float) (Math.exp(-0.01 * cascadeCost / 50000));
    return normalizedCost;
  }

  public Accumulator[] rank() {
    if (mSavedResults != null) {
      mDocSet = new int[mSavedResults.length];
      accumulated_scores = new float[mSavedResults.length];

      for (int i = 0; i < mSavedResults.length; i++) {
        mDocSet[i] = (int) mSavedResults[i][0];
        accumulated_scores[i] = mSavedResults[i][1];
      }

      keptDocs = new int[mDocSet.length + 1][numQueryTerms][];
      keptDocLengths = new int[mDocSet.length + 1];
    }

    // Initialize the MRF ==> this will clear out postings readers cache!
    try {
      mMRF.initialize();
    } catch (ConfigurationException e) {
      LOG.error("Error initializing MRF. Aborting ranking!");
      return null;
    }

    int totalCnt = mMRF.getCliques().size();
    Map<Integer, Set<CascadeClique>> cascadeStages = Maps.newHashMap();
    for (Clique c : mMRF.getCliques()) {
      CascadeClique cc = (CascadeClique) c;
      int stage = cc.getCascadeStage();
      if ( cascadeStages.containsKey(stage)) {
        cascadeStages.get(stage).add(cc);
      } else {
        cascadeStages.put(stage, Sets.newHashSet(cc));
      }
    }

    CascadeAccumulator[] results = null;
    // Cascade stage starts at 0
    int cascadeStage = 0;
    int cnt = 0;

    String pruningFunction = null;
    float pruningParameter = -1;
    int termMatches = 0;

    while (cnt != totalCnt) { // if not have gone thru all cascade stages
      float subTotal_cascadeCost = 0;

      if (cascadeStage < 1) { // only call once, then use keptDocs[][][]
        mMRF.removeAllCliques();

        for (CascadeClique c : cascadeStages.get(cascadeStage)) {
          mMRF.addClique(c);
          cnt++;

          pruningFunction = c.getPruningFunction();
          pruningParameter = c.getPruningParameter();

          int numDocs = Integer.MAX_VALUE;

          if (mDocSet == null) {
            numDocs = c.getNumberOfPostings();
            // (not) ignore cost of first stage from the cost model
            subTotal_cascadeCost += c.cost * numDocs;
          } else {
            subTotal_cascadeCost += c.cost;
          }
        }

        if (mDocSet != null) {
          // Lidan: mDocSet[] & accumulated_scores[] should be sorted by doc scores!
          // Lidan: this method opereates on mDocSet[] & accumulated_scores[]!
          pruneDocuments(pruningFunction, pruningParameter);

          // Lidan: will score all documents in the retained documenet set
          mNumResults = mDocSet.length;

          sortDocumentsByDocnos();

          // Cost = cost of applying the feature on the retained documents after pruning
          subTotal_cascadeCost = subTotal_cascadeCost * mNumResults;
        } else {
          // Lidan: first cascade stage, just output 20000 documents
          mNumResults = INITIAL_STAGE_NUM_RESULTS;

          if (cascadeStage != 0) {
            System.out.println("Should be the first stage here!");
            System.exit(-1);
          }
        }

        // Create single pool of reusable accumulators.
        mAccumulators = new CascadeAccumulator[mNumResults + 1];
        for (int i = 0; i < mNumResults + 1; i++) {
          mAccumulators[i] = new CascadeAccumulator(0, 0.0f);
        }

        results = executeInitialStage();

        cascadeStage++;
      } else {
        String featureID = null;
        ScoringFunction scoringFunction = null;

        int mSize = -1;
        String[][] concepts_this_stage = new String[totalCnt][];
        float[] clique_wgts = new float[concepts_this_stage.length];

        int cntConcepts = 0;

        for (CascadeClique c : cascadeStages.get(cascadeStage)) {
          cnt++;
          pruningFunction = c.getPruningFunction();
          pruningParameter = c.getPruningParameter();

          featureID = c.getParamID().trim(); // termWt, orderedWt, unorderedWt
          scoringFunction = c.getScoringFunction();

          mSize = c.getWindowSize(); // window width
          if (mSize == -1 && !(featureID.equals("termWt"))) {
            throw new RetrievalException("Only term features don't support getWindowSize()! " + featureID);
          }
          concepts_this_stage[cntConcepts] = c.getSingleTerms();
          clique_wgts[cntConcepts] = c.getWeight();

          cntConcepts++;
          subTotal_cascadeCost += c.cost;
        }

        // for use in pruning

        // score-based
        float max_score = results[0].score;
        float min_score = results[results.length - 1].score;
        float score_threshold = (max_score - min_score) * pruningParameter + min_score;
        float mean_max_score_threshold = pruningParameter * max_score + (1.0f - pruningParameter) * meanScore;

        // rank-based
        int retainSize = (int) ((1.0 - pruningParameter) * ((double) (results.length)));
        int size = 0;

        // Clear priority queue.
        mSortedAccumulators.clear();

        float[] termCollectionFreqs = new float[cntConcepts];
        float[] termDFs = new float[cntConcepts];
        int[][] termIndexes = new int[cntConcepts][];

        float sumScore = 0;

        for (int j = 0; j < cntConcepts; j++) {
          String[] singleTerms = concepts_this_stage[j];

          int termIndex1 = termToCliqueNumber.get(singleTerms[0]);

          if (featureID.indexOf("termWt") != -1) {
            float termCollectionFreq = cf.get(singleTerms[0]);
            termCollectionFreqs[j] = termCollectionFreq;

            float termDF = df.get(singleTerms[0]);
            termDFs[j] = termDF;

            termIndexes[j] = new int[1];
            termIndexes[j][0] = termIndex1;

            if (singleTerms.length != 1) {
              System.out.println("Should have length 1 " + singleTerms.length);
              System.exit(-1);
            }
          } else {
            int termIndex2 = termToCliqueNumber.get(singleTerms[1]);

            termIndexes[j] = new int[2];
            termIndexes[j][0] = termIndex1;
            termIndexes[j][1] = termIndex2;

            if (singleTerms.length != 2) {
              System.out.println("Should have length 2 " + singleTerms.length);
              System.exit(-1);
            }
          }
        }

        // iterate over results documents, which are sorted in scores
        for (int i = 0; i < results.length; i++) {
          // pruning, if okay, scoring, update pruning stats for next cascade stage

          boolean passedPruning = false;
          if (pruningFunction.equals("rank")) {
            if (i < retainSize) {
              passedPruning = true;
            } else {
              if (size < mK && mK != defaultNumDocs) {
                passedPruning = true;
              } else {
                break;
              }
            }
          } else if (pruningFunction.equals("score")) {
            if (results[i].score > score_threshold) {
              passedPruning = true;
            } else {
              if (size < mK && mK != defaultNumDocs) {
                passedPruning = true;
              } else {
                break;
              }
            }
          } else if (pruningFunction.equals("mean-max")) {
            if (results[i].score > mean_max_score_threshold) {
              passedPruning = true;
            } else {
              if (size < mK && mK != defaultNumDocs) {
                passedPruning = true;
              } else {
                break;
              }
            }
          } else {
            throw new RetrievalException("Not supported pruner! "+pruningFunction);
          }

          if (passedPruning) {
            size++;

            int docIndex = results[i].index_into_keptDocs;
            int docLen = keptDocLengths[docIndex];
            float docScore_cascade = 0;

            for (int j = 0; j < cntConcepts; j++) {
              if (featureID.equals("termWt")) {
                int termIndex1 = termIndexes[j][0];
                int[] positions1 = keptDocs[docIndex][termIndex1];

                int tf = 0;
                if (positions1 != null) {
                  tf = positions1.length;
                }

                docScore_cascade += clique_wgts[j] * scoringFunction.getScore(tf, docLen);

              } else { // term proximity

                // merge into a single stream and compute matches. Assume there are only two
                // terms!!!

                int termIndex1 = termIndexes[j][0];
                int termIndex2 = termIndexes[j][1];

                int[] positions1 = keptDocs[docIndex][termIndex1];
                int[] positions2 = keptDocs[docIndex][termIndex2];

                int matches = 0;

                if (positions1 != null && positions2 != null) { // both query terms are in the doc

                  termMatches++;
                  int[] ids = new int[positions1.length];
                  Arrays.fill(ids, 0);
                  int length = positions1.length;

                  int length2 = positions2.length;

                  int[] newPositions = new int[length + length2];
                  int[] newIds = new int[length + length2];

                  int posA = 0;
                  int posB = 0;

                  int ii = 0;
                  while (ii < length + length2) {
                    if (posB == length2 || posA < length && positions1[posA] <= positions2[posB]) {
                      newPositions[ii] = positions1[posA];
                      newIds[ii] = ids[posA];
                      posA++;
                    } else {
                      newPositions[ii] = positions2[posB];
                      newIds[ii] = 1;
                      posB++;
                    }
                    ii++;
                  }

                  int[] positions = newPositions;
                  ids = newIds;

                  BitSet mMatchedIds = new BitSet(2); // Assume there are only two terms!!!

                  if (featureID.equals("orderedWt")) {

                    for (ii = 0; ii < positions.length; ii++) {
                      mMatchedIds.clear();
                      int maxGap = 0;
                      boolean ordered = true;
                      mMatchedIds.set(ids[ii]);
                      int matchedIDCounts = 1;
                      int lastMatchedID = ids[ii];
                      int lastMatchedPos = positions[ii];

                      for (int jj = ii + 1; jj < positions.length; jj++) {
                        int curID = ids[jj];
                        int curPos = positions[jj];
                        if (!mMatchedIds.get(curID)) {
                          mMatchedIds.set(curID);
                          matchedIDCounts++;
                          if (curID < lastMatchedID) {
                            ordered = false;
                          }
                          if (curPos - lastMatchedPos > maxGap) {
                            maxGap = curPos - lastMatchedPos;
                          }
                        }
                        // stop looking if the maximum gap is too large
                        // or the terms appear out of order
                        if (maxGap > mSize || !ordered) {
                          break;
                        }
                        // did we match all the terms, and in order?
                        if (matchedIDCounts == 2 && ordered) {
                          matches++;
                          break;
                        }
                      }
                    }
                  } else if (featureID.equals("unorderedWt")) {

                    for (ii = 0; ii < positions.length; ii++) {
                      mMatchedIds.clear();

                      mMatchedIds.set(ids[ii]);
                      int matchedIDCounts = 1;
                      int startPos = positions[ii];

                      for (int jj = ii + 1; jj < positions.length; jj++) {
                        int curID = ids[jj];
                        int curPos = positions[jj];
                        int windowSize = curPos - startPos + 1;

                        if (!mMatchedIds.get(curID)) {
                          mMatchedIds.set(curID);
                          matchedIDCounts++;
                        }
                        // stop looking if we've exceeded the maximum window size
                        if (windowSize > mSize) {
                          break;
                        }
                        // did we match all the terms?
                        if (matchedIDCounts == 2) {
                          matches++;
                          break;
                        }
                      }
                    }
                  } else {
                    System.out.println("Invalid featureID " + featureID);
                    System.exit(-1);
                  }
                } // end if this is a match, i.e., both query terms are in the doc

//                float s = getScore(matches, docLen, RetrievalEnvironment.defaultCf,
//                    (float) RetrievalEnvironment.defaultDf, scoringFunctionName);
//                docScore_cascade += clique_wgts[j] * s;
                
                GlobalTermEvidence termEvidence = scoringFunction.getGlobalTermEvidence();
                termEvidence.cf = RetrievalEnvironment.defaultCf;
                termEvidence.df = RetrievalEnvironment.defaultDf;

                scoringFunction.initialize(termEvidence, scoringFunction.getGlobalEvidence());
                docScore_cascade += clique_wgts[j] * scoringFunction.getScore(matches, docLen);

              } // end else it's proximity feature
            } // end for (each concept)

            // accumulate doc score in results[i] across cascade stages
            results[i].score += docScore_cascade;

            mSortedAccumulators.add(results[i]);

            sumScore += results[i].score;

          } // end if passed pruning
        } // end iterating over docs

        // order based on new scores in results[], put into priority queue
        if (size != mSortedAccumulators.size()) {
          throw new RetrievalException("They should be equal right here " + size + " "
              + mSortedAccumulators.size());
        }

        CascadeAccumulator[] results_tmp = new CascadeAccumulator[size];

        meanScore = sumScore / (float) size; // update stats for use in pruning in next cascade stage
        stddev = 0;

        for (int i = 0; i < results_tmp.length; i++) {
          results_tmp[results_tmp.length - 1 - i] = mSortedAccumulators.poll();

          stddev += (results_tmp[results_tmp.length - 1 - i].score - meanScore)
              * (results_tmp[results_tmp.length - 1 - i].score - meanScore);
        }
        results = results_tmp;

        stddev = (float) Math.sqrt(stddev);

        // Create single pool of reusable accumulators.
        // Use mNumResults from prev iteration, since we don't know how many docs are kept until
        // we're done iterating through the documents

        cascadeStage++;

        subTotal_cascadeCost = subTotal_cascadeCost * size;

      } // end if not first stage

      cascadeCost += subTotal_cascadeCost;

    } // end while

    CascadeAccumulator[] results_return = results;

    if (results.length > mK) {
      results_return = new CascadeAccumulator[mK];

      for (int i = 0; i < mK; i++) {
        results_return[i] = new CascadeAccumulator(results[i].docno, results[i].score);
      }
    }

    return results_return;
  }

  public CascadeAccumulator[] executeInitialStage() {

    // point to next position in keptDocs array that hasn't been filled
    int indexCntKeptDocs = 0;

    // Clear priority queue.
    mSortedAccumulators.clear();

    // Cliques associated with the MRF.
    List<Clique> cliques = mMRF.getCliques();

    if (cliques.size() == 0) {
      throw new RetrievalException("Shouldn't have size 0!");
    }

    // Current accumulator.
    CascadeAccumulator a = mAccumulators[0];

    // Maximum possible score that this MRF can achieve.
    float mrfMaxScore = 0.0f;
    for (Clique c : cliques) {
      if (!((((CascadeClique) c).getParamID()).equals("termWt"))) {
        System.out
            .println("In this faster cascade implementation, first stage must be term in order to get positions[] values! "
                + ((CascadeClique) c).getParamID());
        System.exit(-1);
      }
      mrfMaxScore += c.getMaxScore();
    }

    // Sort cliques according to their max scores.
    Collections.sort(cliques, maxScoreComparator);

    // Score that must be achieved to enter result set.
    double scoreThreshold = Double.NEGATIVE_INFINITY;

    // Offset into document set we're currently at (if applicable).
    int docsetOffset = 0;

    int docno = 0;
    if (mDocSet != null) {
      docno = docsetOffset < mDocSet.length ? mDocSet[docsetOffset++] : Integer.MAX_VALUE;
    } else {
      docno = mMRF.getNextCandidate();
    }

    boolean firstTime = true;

    while (docno < Integer.MAX_VALUE) {
      for (DocumentNode documentNode : mDocNodes) {
        documentNode.setDocno(docno);
      }

      // Document-at-a-time scoring.
      float docMaxScore = mrfMaxScore;
      boolean skipped = false;

      float score = 0.0f;

      // Lidan: accumulate document scores across the cascade stages
//      if (mDocSet != null && cascadeStage != 0) {
//        score = accumulated_scores[docsetOffset - 1];
//      }

      // for each query term, its position in a document
      int[][] termPositions = new int[cliques.size()][];
      int doclen = -1;

      for (int i = 0; i < cliques.size(); i++) {
        // Current clique that we're scoring.
        CascadeClique c = (CascadeClique) cliques.get(i);

        if (firstTime) {
          termToCliqueNumber.put(c.getConcept().trim().toLowerCase(), i);
          cf.put(c.getConcept().trim().toLowerCase(), c.termCollectionCF());
          df.put(c.getConcept().trim().toLowerCase(), c.termCollectionDF());
        }

        if (score + docMaxScore <= scoreThreshold) {
          // Advance postings readers (but don't score).
          for (int j = i; j < cliques.size(); j++) {
            cliques.get(j).setNextCandidate(docno + 1);
          }
          skipped = true;

          break;
        }

        // Document independent cliques do not affect the ranking.
        if (!c.isDocDependent()) {
          continue;
        }

        // Update document score.
        float cliqueScore = c.getPotential();
        score += c.getWeight() * cliqueScore;

        // Update the max score for the rest of the cliques.
        docMaxScore -= c.getMaxScore();

        // stuff needed for document evaluation in the next stage
        int[] p = c.getPositions();

        if (p != null) {
          termPositions[i] = Arrays.copyOf(p, p.length);
          doclen = c.getDocLen();
        }
      }

      firstTime = false;

      // Keep track of mNumResults best accumulators.
      if (!skipped && score > scoreThreshold) {
        a.docno = docno;
        a.score = score;
        a.index_into_keptDocs = indexCntKeptDocs;
        keptDocLengths[indexCntKeptDocs] = doclen;

        mSortedAccumulators.add(a);

        // save positional information for each query term in the document
        for (int j = 0; j < termPositions.length; j++) {

          if (termPositions[j] != null) {
            keptDocs[indexCntKeptDocs][j] = Arrays.copyOf(termPositions[j], termPositions[j].length);
          }
        }

        if (mSortedAccumulators.size() == mNumResults + 1) {
          a = mSortedAccumulators.poll(); // Re-use the accumulator of the removed document

          // After maximum # docs been put into queue, each time a new document is added, an old
          // document will be ejected, use the spot freed by the ejected document to store the new
          // document positional info in keptDocs

          indexCntKeptDocs = a.index_into_keptDocs;
          keptDocs[indexCntKeptDocs] = new int[numQueryTerms][];

          scoreThreshold = mSortedAccumulators.peek().score;

        } else {
          a = mAccumulators[mSortedAccumulators.size()]; // Next non-used accumulator in the
                                                         // accumulator pool
          indexCntKeptDocs++;
        }

      }

      if (mDocSet != null) {
        docno = docsetOffset < mDocSet.length ? mDocSet[docsetOffset++] : Integer.MAX_VALUE;
      } else {
        docno = mMRF.getNextCandidate();
      }
    }

    // Grab the accumulators off the stack, in (reverse) order.
    CascadeAccumulator[] results_tmp = new CascadeAccumulator[Math.min(mNumResults,
        mSortedAccumulators.size())];

    for (int i = 0; i < results_tmp.length; i++) {
      results_tmp[results_tmp.length - 1 - i] = mSortedAccumulators.poll();
      meanScore += results_tmp[results_tmp.length - 1 - i].score;
    }

    meanScore /= results_tmp.length;

    CascadeAccumulator[] results = results_tmp;

    return results;
  }

  /**
   * Returns the Markov Random Field associated with this ranker.
   */
  public MarkovRandomField getMRF() {
    return mMRF;
  }

  /**
   * Sets the number of results to return.
   */
  public void setNumResults(int numResults) {
    mNumResults = numResults;
  }

  private List<DocumentNode> getDocNodes() {
    ArrayList<DocumentNode> docNodes = new ArrayList<DocumentNode>();

    // Check which of the nodes are DocumentNodes.
    List<GraphNode> nodes = mMRF.getNodes();
    for (GraphNode node : nodes) {
      if (node.getType() == GraphNode.Type.DOCUMENT) {
        docNodes.add((DocumentNode) node);
      }
    }
    return docNodes;
  }
}
