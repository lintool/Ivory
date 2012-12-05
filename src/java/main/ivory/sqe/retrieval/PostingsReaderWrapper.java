package ivory.sqe.retrieval;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsReader;
import ivory.core.data.index.ProximityPostingsReaderOrderedWindow;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class PostingsReaderWrapper {
  // Default score for potentials with no postings.
  protected static final float DEFAULT_SCORE = 0.0f;

  protected final Posting curPosting = new Posting();
  protected RetrievalEnvironment env;
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
  protected float weights[];

  private final int numDocs;
  private final float avgDocLen;

  public PostingsReaderWrapper(JsonObject query, RetrievalEnvironment env, GlobalEvidence ge) {
    this.operator = query.entrySet().iterator().next().getKey();
    this.values = query.getAsJsonArray(operator);

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
    this.numDocs = (int) env.getDocumentCount();
    this.avgDocLen = env.getCollectionSize() / numDocs;

    // Read first posting.
    endOfList = false;

    // If this is not a leaf node, create children
    children = new ArrayList<PostingsReaderWrapper>();
    // //LOG.info("non-leaf node with "+values.length()+" children");
    for (int i = iterStart; i < values.size(); i = i + iterStep) {
      if (!values.get(i).isJsonPrimitive()) {
        // If child is an object (non-leaf), call nonleaf-constructor
        children.add(new PostingsReaderWrapper(values.get(i).getAsJsonObject(), env, ge));
      } else {
        // If child is leaf, call leaf-constructor
        children.add(new PostingsReaderWrapper(values.get(i).getAsString(), env, ge));
      }
    }

    lastScoredDocno = 0;
  }

  public PostingsReaderWrapper(String termOrPhrase, RetrievalEnvironment env, GlobalEvidence ge) {
    this.env = Preconditions.checkNotNull(env);
    this.numDocs = (int) env.getDocumentCount();
    this.avgDocLen = env.getCollectionSize() / numDocs;

    // Read first posting.
    endOfList = false;

    // If this is a leaf node (i.e., single term), create postings list
    this.termOrPhrase = termOrPhrase;
    terms = termOrPhrase.split("\\s+");
    if (terms.length > 1) {
      operator = "phrase";
      List<PostingsReader> prs = new ArrayList<PostingsReader>();
      for (String term : terms) {
        PostingsList pl = env.getPostingsList(term);
        // if any of the tokens is OOV, then the phrase is considered OOV
        if (pl == null) {
          isOOV = true;
          endOfList = true;
          return;
        }
        prs.add(pl.getPostingsReader());
      }
      postingsReader = new ProximityPostingsReaderOrderedWindow(prs.toArray(new PostingsReader[0]),
          2);
      postingsReader.nextPosting(curPosting);
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
  }

  public NodeWeight computeScore(int curDocno) {
    NodeWeight score;
    if (isOOV) {
      int docLen = env.getDocumentLength(curDocno);
      score = new TfDfWeight(0, 0, docLen, numDocs, avgDocLen);
    } else if (!isLeaf()) {
      score = runOperator(curDocno);
      lastScoredDocno = curDocno;
    } else { // leaf node
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
      score = new TfDfWeight(tf, gte.getDf(), docLen, numDocs, avgDocLen);

      lastScoredDocno = curDocno;
    }
    return score;
  }

  private NodeWeight runOperator(int curDocno) {
    // If this is not a leaf node, compute scores from children and
    // combine them w.r.t operator
    NodeWeight[] scores = new NodeWeight[children.size()];
    for (int i = 0; i < children.size(); i++) {
      scores[i] = children.get(i).computeScore(curDocno);
    }
    int docLen = env.getDocumentLength(curDocno);

    NodeWeight resultScore;
    if (operator.equals("#combine")) {
      // sum bm25 scores
      float score = 0f;
      for (int i = 0; i < scores.length; i++) {
        score += scores[i].getScore();
      }
      resultScore = new FloatWeight(score);
    } else if (operator.equals("#weight")) {
      if (scores.length == 0) {
        resultScore = new FloatWeight();
      } else {
        if (scores[0] instanceof TfDfWeight) {
          resultScore = new TfDfWeight(0, 0, docLen, numDocs, avgDocLen);
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
        score += scores[i].getScore() * weights[i];
      }
      resultScore = new FloatWeight(score);
    } else {
      throw new RuntimeException("Unknown operator: " + operator);
    }
    return resultScore;
  }

  /**
   * @param docno
   * @return next smallest docno from posting lists of leaf nodes
   */
  public int getNextCandidate(int docno) {
    if (isOOV) {
      return docno;
    } else if (!isLeaf()) { // not a leaf node
      for (int i = 0; i < children.size(); i++) {
        int nextDocno = children.get(i).getNextCandidate(docno);
        if (nextDocno != lastScoredDocno && nextDocno < docno) {
          docno = nextDocno;
        }
      }
      return docno;
    } else { // leaf node
      if (endOfList) {
        return Integer.MAX_VALUE;
      }
      int nextDocno = findNextDocnoWithPositiveTF(operator);
      if (nextDocno == Integer.MAX_VALUE) {
        endOfList = true;
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

  public void setNextCandidate(int docno) {
    // Advance postings reader. Invariant: curPosting will always point to
    // the next posting that has not yet been scored.
    while (!endOfList && postingsReader.getDocno() < docno) {
      if (!postingsReader.nextPosting(curPosting)) {
        endOfList = true;
      }
    }
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
