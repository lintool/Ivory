package ivory.sqe.retrieval;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsReader;
import ivory.core.data.index.ProximityPostingsReaderOrderedWindow;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.score.ScoringFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mortbay.log.Log;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class PostingsReaderWrapper {
  // Default score for potentials with no postings.
  protected static final float DEFAULT_SCORE = 0.0f;

  protected final Posting curPosting = new Posting();
  protected RetrievalEnvironment env;
  protected ScoringFunction scoringFunction;
  protected PostingsReader postingsReader = null;
  protected GlobalTermEvidence gte;
  protected GlobalEvidence ge;

  protected boolean endOfList = true; // Whether or not we're at the end of
  // the postings list.
  protected int lastScoredDocno = 0, iterStart = 0, iterStep = 1;

  protected String operator, termOrPhrase, terms[];
  protected JsonArray values;
  protected List<PostingsReaderWrapper> children;

  protected boolean isOOV = false;
  protected float weights[];// , normalizationFactor;

  public PostingsReaderWrapper(JsonObject query, RetrievalEnvironment env,
      ScoringFunction scoringFunction, GlobalEvidence ge) {
    this.operator = query.entrySet().iterator().next().getKey();
    this.values = query.getAsJsonArray(operator);

    // //LOG.info("operator= "+operator);
    // //LOG.info("values= "+values.toString());

    if (operator.equals("#weight") || operator.equals("#combweight")) {
      iterStart = 1;
      iterStep = 2;
      weights = new float[values.size() / 2];

      // in #weight or #combweight structure, even-numbered indices corr. to
      // weights, odd-numbered indices corr. to terms/phrases
      for (int i = 0; i < values.size(); i = i + iterStep) {
        weights[i / 2] = (float) values.get(i).getAsDouble();
      }
    }

    this.env = Preconditions.checkNotNull(env);
    this.scoringFunction = Preconditions.checkNotNull(scoringFunction);

    // Read first posting.
    endOfList = false;

    // If this is not a leaf node, create children
    children = new ArrayList<PostingsReaderWrapper>();
    // //LOG.info("non-leaf node with "+values.length()+" children");
    for (int i = iterStart; i < values.size(); i = i + iterStep) {
      // //LOG.info("child "+i+":");
      //JsonObject child = values.get(i).getAsJsonObject();
      // ////LOG.info(child);
      if (!values.get(i).isJsonPrimitive()) {
        // If child is an object (non-leaf), call nonleaf-constructor
        children.add(new PostingsReaderWrapper(values.get(i).getAsJsonObject(),
            env, scoringFunction, ge));
      } else {
        // If child is leaf, call leaf-constructor
        children.add(new PostingsReaderWrapper(values.get(i).getAsString(),
            env, scoringFunction, ge));
      }
    }

    lastScoredDocno = 0;
    // //LOG.info("non-leaf done.");
  }

  public PostingsReaderWrapper(String termOrPhrase, RetrievalEnvironment env,
      ScoringFunction scoringFunction, GlobalEvidence ge) {
    this.env = Preconditions.checkNotNull(env);
    this.scoringFunction = Preconditions.checkNotNull(scoringFunction);

    // Read first posting.
    endOfList = false;

    // If this is a leaf node (i.e., single term), create postings list
    // //LOG.info("leaf node");
    this.termOrPhrase = termOrPhrase;
    terms = termOrPhrase.split("\\s+");
    if (terms.length > 1) {
      operator = "phrase";
      List<PostingsReader> prs = new ArrayList<PostingsReader>();
      for (String term : terms) {
        PostingsList pl = env.getPostingsList(term);
//        LOG.info(term+"->"+pl.getDf());
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
      //			      //LOG.info(termOrPhrase+ " docno-->"+postingsReader2.getDocno());
      //			      //LOG.info(termOrPhrase+" tf-->"+postingsReader2.getTf());
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
    // //LOG.info("leaf done.");
  }

  public NodeWeight computeScore(int curDocno, int depth) {
    //LOG.info("@"+depth+" Scoring... docno="+curDocno+" node="+this.toString());
    NodeWeight score;
    if (isOOV) {
      int docLen = env.getDocumentLength(curDocno);
      int numDocs = (int) env.getDocumentCount();
      float avgDocLen = env.getCollectionSize() / numDocs;

      score = new TfDfWeight(0,0, numDocs, docLen, avgDocLen);
    } else if (!isLeaf()) {
      // If this is not a leaf node, compute scores from children and
      // combine them w.r.t operator
      NodeWeight[] scores = new NodeWeight[children.size()];
      //LOG.info(children.size()+" children");
      for (int i = 0; i < children.size(); i++) {
        //LOG.info("@"+depth+" Scoring child "+ children.get(i).toString() +"...");
        scores[i] = children.get(i).computeScore(curDocno, depth+1);
        //LOG.info("@"+depth+" Child score: "+ scores[i]);
      }
      int docLen = env.getDocumentLength(curDocno);
      int numDocs = (int) env.getDocumentCount();
      float avgDocLen = env.getCollectionSize() / numDocs;

      score = runOperator(scores, (int) numDocs, docLen, avgDocLen);
      //LOG.info("@"+depth+" Node score: "+ score);
      lastScoredDocno = curDocno;
    } else { // leaf node
      // System.out.println("leaf node");
      // Advance postings reader. Invariant: curPosting will always point
      // to the next posting that has not yet been scored.
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
 
      int docLen = env.getDocumentLength(curDocno);
      int numDocs = (int) env.getDocumentCount();
      float avgDocLen = env.getCollectionSize() / numDocs;

      score = new TfDfWeight(tf, gte.getDf(), numDocs, docLen, avgDocLen);
      //LOG.info(tf + "," + gte.getDf());//+ "," + ge.queryLength+"," + ge.numDocs+"," + ge.collectionLength);

      lastScoredDocno = curDocno;
    }
    return score;
  }

  private NodeWeight runOperator(NodeWeight[] scores, int numDocs, int docLen, float avgDocLen) {
    NodeWeight resultScore;
    if (operator.equals("#combine")) {
      // sum bm25 scores
      float score = 0f;
      for (int i = 0; i < scores.length; i++) {
        float bm25 = scores[i].getScore();
        //LOG.info("Child #"+i+" bm25 = "+bm25);
        score += bm25;
      }
      resultScore = new FloatWeight(score);
    } else if (operator.equals("#weight")) {
      if (scores.length == 0) {
        resultScore = new FloatWeight();
      } else {
        if (scores[0] instanceof TfDfWeight) {
          resultScore = new TfDfWeight(0, 0, numDocs, docLen, avgDocLen);
        } else {
          resultScore = new FloatWeight();
        }
        // tf,df = sum{weight_i * (tf_i,df_i)}
        for (int i = 0; i < scores.length; i++) {
          resultScore.add(scores[i].multiply(weights[i]));
        }
      }
    } else if (operator.equals("#combweight")) {
      // sum bm25 scores
      float score = 0f;
      for (int i = 0; i < scores.length; i++) {
        float bm25 = scores[i].getScore();
        //LOG.info("Child #"+i+" bm25 = "+bm25);
        score += bm25 * weights[i];
      }
      resultScore = new FloatWeight(score); 
    } else {
      throw new RuntimeException("Unknown operator: "+operator);
    }
    return resultScore;
  }

  /**
   * @param docno
   * @return next smallest docno from posting lists of leaf nodes
   */
  public int getNextCandidate(int docno) {
    //LOG.info("Looking for candidates less than "+ docno +" at node="+this.toString());
    if (isOOV) {
      return docno;
    } else if (!isLeaf()) { // not a leaf node
      for (int i = 0; i < children.size(); i++) {
        int nextDocno = children.get(i).getNextCandidate(docno);
        if (nextDocno != lastScoredDocno && nextDocno < docno) {
          docno = nextDocno;
        }else {
          //LOG.info("ignored "+nextDocno);
        }
      }
      return docno;
    } else { // leaf node
      if (endOfList) {
        //LOG.info("End of list");
        return Integer.MAX_VALUE;
      }
      int nextDocno = findNextDocnoWithPositiveTF(operator);
      if (nextDocno == Integer.MAX_VALUE) {
        //LOG.info("End of list");
        endOfList = true;
      } else {
        //LOG.info("Found "+nextDocno);
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
