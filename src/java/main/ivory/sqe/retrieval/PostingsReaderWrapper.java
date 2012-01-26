package ivory.sqe.retrieval;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsReader;
import ivory.core.data.index.ProximityPostingsReaderOrderedWindow;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.score.BM25ScoringFunction;
import ivory.smrf.model.score.ScoringFunction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Preconditions;

public class PostingsReaderWrapper {
  // Default score for potentials with no postings.
  protected static final float DEFAULT_SCORE = 0.0f;
  private static final Logger LOG = Logger.getLogger(PostingsReaderWrapper.class);

  protected final Posting curPosting = new Posting();
  protected RetrievalEnvironment env;
  protected ScoringFunction scoringFunction;
  protected PostingsReader postingsReader = null;
  protected GlobalTermEvidence gte;
  protected GlobalEvidence ge;

  protected boolean endOfList = true; // Whether or not we're at the end of
  // the postings list.
  protected int lastScoredDocno = 0, iterStart = 0, iterStep = 1;

  protected String operator, terms[];
  protected JSONArray values;
  protected List<PostingsReaderWrapper> children;

  protected boolean isOOV = false;
  protected float weights[];// , normalizationFactor;

  public PostingsReaderWrapper(JSONObject query, RetrievalEnvironment env,
      ScoringFunction scoringFunction, GlobalEvidence ge)
  throws JSONException {
    this.operator = query.keys().next();
    this.values = query.getJSONArray(operator);

    // LOG.info("operator= "+operator);
    // LOG.info("values= "+values.toString());

    if (operator.equals("#weight") || operator.equals("#pweight")) {
      iterStart = 1;
      iterStep = 2;
      weights = new float[values.length() / 2];

      // in #weight or #pweight structure, even-numbered indices corr. to
      // weights, odd-numbered indices corr. to terms/phrases
      // handle weight normalization here
      // normalizationFactor = 0.0f;
      for (int i = 0; i < values.length(); i = i + iterStep) {
        weights[i / 2] = (float) values.getDouble(i);
        // normalizationFactor += weights[i/2];
      }
    }

    this.env = Preconditions.checkNotNull(env);
    this.scoringFunction = Preconditions.checkNotNull(scoringFunction);

    // Read first posting.
    endOfList = false;

    // If this is not a leaf node, create children
    children = new ArrayList<PostingsReaderWrapper>();
    // LOG.info("non-leaf node with "+values.length()+" children");
    for (int i = iterStart; i < values.length(); i = i + iterStep) {
      // LOG.info("child "+i+":");
      JSONObject child = values.optJSONObject(i);
      // //LOG.info(child);
      if (child != null) {
        // If child is an object (non-leaf), call nonleaf-constructor
        children.add(new PostingsReaderWrapper(values.getJSONObject(i),
            env, scoringFunction, ge));
      } else {
        // If child is leaf, call leaf-constructor
        children.add(new PostingsReaderWrapper(values.getString(i),
            env, scoringFunction, ge));
      }
    }

    lastScoredDocno = 0;
    // LOG.info("non-leaf done.");
  }

  public PostingsReaderWrapper(String termOrPhrase, RetrievalEnvironment env,
      ScoringFunction scoringFunction, GlobalEvidence ge)
  throws JSONException {
    this.env = Preconditions.checkNotNull(env);
    this.scoringFunction = Preconditions.checkNotNull(scoringFunction);

    // Read first posting.
    endOfList = false;

    // If this is a leaf node (i.e., single term), create postings list
    // LOG.info("leaf node");
    terms = termOrPhrase.split("\\s+");
    if (terms.length > 1) {
      operator = "phrase";
      List<PostingsReader> prs = new ArrayList<PostingsReader>();
      int i = 0;
      for (String term : terms) {
        PostingsList pl = env.getPostingsList(term);
        // if any of the tokens is OOV, then the phrase is considerd OOV
        if (pl == null) {
          isOOV = true;
          endOfList = true;
          return;
        }
        prs.add(pl.getPostingsReader());	
      }
      postingsReader = new ProximityPostingsReaderOrderedWindow(prs.toArray(new PostingsReader[0]), 2);
      postingsReader.nextPosting(curPosting);
      //      ProximityPostingsReaderOrderedWindow postingsReader2 = new ProximityPostingsReaderOrderedWindow(prs, 2);
      //      while (postingsReader2.hasMorePostings()){
      //			  if(postingsReader2.nextPosting(curPosting)){
      //			    if(postingsReader2.getTf()>0){
      //			      LOG.info(termOrPhrase+ " docno-->"+postingsReader2.getDocno());
      //			      LOG.info(termOrPhrase+" tf-->"+postingsReader2.getTf());
      //			    }
      //			  }
      //			}
      gte = new GlobalTermEvidence(env.getDefaultDf(), env.getDefaultCf());
      this.ge = ge;
      lastScoredDocno = 0;
    } else {
      operator = "term";
      PostingsList pl = env.getPostingsList(termOrPhrase);
      if (pl == null) {
        isOOV = true;
        endOfList = true;
      } else {
        postingsReader = pl.getPostingsReader();
        gte = new GlobalTermEvidence(pl.getDf(), pl.getCf());
        this.ge = ge;
        lastScoredDocno = 0;
      }
    }
    // LOG.info("leaf done.");
  }

  public float computeScore(int curDocno) {
    // LOG.info("Scoring... docno="+curDocno+" node="+this.toString());
    float score = 0;
    if (isOOV) {

    } else if (!isLeaf()) {
      // If this is not a leaf node, compute scores from children and
      // combine them w.r.t operator
      float[] scores = new float[children.size()];
      // LOG.info(children.size()+" children");
      for (int i = 0; i < children.size(); i++) {
        // LOG.info("Scoring child "+ children.get(i).toString() +
        // "...");
        scores[i] = children.get(i).computeScore(curDocno);
        // LOG.info("Child score: "+ scores[i]);
      }
      score = runOperator(scores);
      lastScoredDocno = curDocno;
    } else { // leaf node
      // System.out.println("leaf node");
      // Advance postings reader. Invariant: curPosting will always point
      // to
      // the next posting that has not yet been scored.
      while (!endOfList && postingsReader.getDocno() < curDocno) {
        if (!postingsReader.nextPosting(curPosting)) {
          endOfList = true;
        }
      }

      // Compute term frequency if postings list contains this docno,
      // otherwise tf=0
      int tf = 0;
      if (curDocno == postingsReader.getDocno()) {
        tf = postingsReader.getTf();
      }
      ((BM25ScoringFunction) scoringFunction).setB(0.3f);
      ((BM25ScoringFunction) scoringFunction).setK1(0.5f);

      int docLen = env.getDocumentLength(curDocno);
      this.scoringFunction.initialize(gte, ge);
      score = scoringFunction.getScore(tf, docLen);
      // LOG.info(tf + "," + gte.getDf()+ "," + ge.queryLength+"," +
      // ge.numDocs+"," + ge.collectionLength+"," + docLen);

      lastScoredDocno = curDocno;
    }
    return score;
  }

  private float runOperator(float[] scores) {
    if (operator.equals("#combine")) {
      float finalScore = 0;
      for (int i = 0; i < scores.length; i++) {
        finalScore += scores[i];
      }
      return finalScore;
    } else if (operator.equals("#or")) {
      return 0;
    } else if (operator.equals("#weight")) {
      float finalScore = 0;
      for (int i = 0; i < scores.length; i++) {
        // LOG.info("normalized weight = " +
        // (weights[i]/normalizationFactor));
        finalScore += scores[i] * (weights[i]);// /normalizationFactor);
      }
      return finalScore;
    } else if (operator.equals("#pweight")) {
      float finalScore = 0;
      for (int i = 0; i < scores.length; i++) {
        // LOG.info("normalized weight = " +
        // (weights[i]/normalizationFactor));
        finalScore += scores[i] * (weights[i]);// /normalizationFactor);
      }
      return finalScore;
    } else {
      return 0;
    }
  }

  /**
   * @param curDocno
   * @return next smallest docno from posting lists of leaf nodes
   */
  public int getNextCandidate(int docno) {
//    LOG.info("Looking for candidates less than "+ docno +" at node="+this.toString());
    if (isOOV) {
      return docno;
    } else if (!isLeaf()) { // not a leaf node
      for (int i = 0; i < children.size(); i++) {
        int nextDocno = children.get(i).getNextCandidate(docno);
        if (nextDocno != lastScoredDocno && nextDocno < docno) {
          docno = nextDocno;
        }else {
//          LOG.info("ignored "+nextDocno);
        }
      }
      return docno;
    } else { // leaf node
      if (endOfList) {
//        LOG.info("End of list");
        return Integer.MAX_VALUE;
      }
      int nextDocno = findNextDocnoWithPositiveTF(operator);
      if (nextDocno == Integer.MAX_VALUE) {
//        LOG.info("End of list");
        endOfList = true;
      } else {
//        LOG.info("Found "+nextDocno);
      }
      return nextDocno;
    }
  }

  private int findNextDocnoWithPositiveTF(String operator) {
    boolean t = true;
    while (t && (postingsReader.getTf() == 0 || postingsReader.getDocno() == lastScoredDocno)) {
      t = postingsReader.nextPosting(curPosting);
    }
    if (t) {
      return postingsReader.getDocno();
    } else {
      return Integer.MAX_VALUE;
    }
  }

  public void reset() {
    endOfList = false;
    lastScoredDocno = -1;
  }

  public float getMinScore() {
    return scoringFunction.getMinScore();
  }

  public float getMaxScore() {
    return scoringFunction.getMaxScore();
  }

  public void setNextCandidate(int docno) {
    // Advance postings reader. Invariant: curPosting will always point to
    // the next posting that has not yet been scored.
    while (!endOfList && postingsReader.getDocno() < docno) {
      if (!postingsReader.nextPosting(curPosting)) {
        endOfList = true;
      }
    }
  }

  public ScoringFunction getScoringFunction() {
    return this.scoringFunction;
  }

  public String toString() {
    if (isOOV) {
      return "OOV";
    } else if (!isLeaf()) { // not a leaf node
      return operator + "::" + values.toString();
    } else {
      return operator + "::" + Arrays.asList(terms).toString();
    }
  }

  private boolean isLeaf() {
    return postingsReader != null;
  }
}
