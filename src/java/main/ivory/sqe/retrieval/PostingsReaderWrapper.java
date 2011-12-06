package ivory.sqe.retrieval;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsReader;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.score.BM25ScoringFunction;
import ivory.smrf.model.score.ScoringFunction;
import java.util.ArrayList;
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

  protected boolean endOfList = true; // Whether or not we're at the end of the postings list.
  protected int lastScoredDocno = 0;

  protected String operator, term;
  protected JSONArray values;
  protected List<PostingsReaderWrapper> children;
  protected GlobalTermEvidence gte;
  protected GlobalEvidence ge;
    
  public PostingsReaderWrapper(JSONObject query, RetrievalEnvironment env, ScoringFunction scoringFunction, GlobalEvidence ge) throws JSONException {
	  this.operator = query.keys().next();
	  this.values = query.getJSONArray(operator);

	  //LOG.info("operator= "+operator);
	  //LOG.info("values= "+values.toString());


	  this.env = Preconditions.checkNotNull(env);
	  this.scoringFunction = Preconditions.checkNotNull(scoringFunction);

	  // Read first posting.
	  endOfList = false;

	  // If this is not a leaf node, create children
	  children = new ArrayList<PostingsReaderWrapper>();
	  //LOG.info("non-leaf node with "+values.length()+" children");
	  for(int i=0; i<values.length(); i++){
		  //LOG.info("child "+i+":");
		  JSONObject child = values.optJSONObject(i);
		  //LOG.info(child);
		  if(child != null){
			  children.add(new PostingsReaderWrapper(values.getJSONObject(i), env, scoringFunction, ge));
		  }else{
			  children.add(new PostingsReaderWrapper(values.getString(i), env, scoringFunction, ge));
		  }
	  }

	  lastScoredDocno = 0;
	  //LOG.info("non-leaf done.");
  }
  
  public PostingsReaderWrapper(String term, RetrievalEnvironment env, ScoringFunction scoringFunction, GlobalEvidence ge) throws JSONException {
	  this.env = Preconditions.checkNotNull(env);
	  this.scoringFunction = Preconditions.checkNotNull(scoringFunction);

	  // Read first posting.
	  endOfList = false;

	  // If this is a leaf node (i.e., single term), create postings list
	  ////LOG.info("leaf node");
	  
	  operator = "term";
	  this.term = term;
	  
	  PostingsList pl = env.getPostingsList(term);

	  
//----------BEGIN LOCAL_USAGE

//	  PostingsListDocSortedNonPositional pl = new PostingsListDocSortedNonPositional();
//	  int docno = 0;
//	  int df = ((int) (Math.random()*19))+1;
//	  int docnos[] = new int[df];
//	  short tfs[] = new short[df];
//	  int cf = 0;
//	  for(int i=0;i<df;i++){
//		  if(docno == 20){
//			  df = i;
//			  break;
//		  }
//		  docno = ((int) (Math.random()*(19-docno-1)))+docno+1;  //prevdocno+1....20
//		  docnos[i] = docno;
//		  tfs[i] = (short) (Math.random()*5+1);		//1...6
//		  cf += tfs[i];
//	  }
//	  pl.setDf(df);
//	  pl.setCf(cf);
//	  pl.setCollectionDocumentCount(20);
//	  for(int i=0;i<df;i++){
//		  pl.add(docnos[i], tfs[i]);
//	  }
	  
//---------END LOCAL_USAGE
	  postingsReader = pl.getPostingsReader();
	  gte = new GlobalTermEvidence(pl.getDf(), pl.getCf());
	  this.ge = ge;

	  lastScoredDocno = 0;
	  ////LOG.info("leaf done.");
  }


  public float computeScore(int curDocno) {
	//LOG.info("Scoring...");
	float score = 0;

    // If this is not a leaf node, compute scores from children and combine them w.r.t operator
    if (!operator.equals("term")) {
    	//LOG.info("non-leaf node");
    	float[] scores = new float[children.size()];
    	for (int i = 0; i < children.size(); i++) {
    		scores[i] = children.get(i).computeScore(curDocno);
    	    //LOG.info("Child "+ i + " score = " + scores[i]);
    	}
    	score = runOperator(scores);
	    //System.out.println("non-leaf score = " + score);
    }else{
    	//System.out.println("leaf node");
	    // Advance postings reader. Invariant: curPosting will always point to
	    // the next posting that has not yet been scored.
	    while (!endOfList && postingsReader.getDocno() < curDocno) {
	      if (!postingsReader.nextPosting(curPosting)) {
	        endOfList = true;
	      }
	    }
	
	    // Compute term frequency if postings list contains this docno, otherwise tf=0
	    int tf = 0;
	    if (curDocno == postingsReader.getDocno()) {
	      tf = postingsReader.getTf();
	    }
	    ((BM25ScoringFunction) scoringFunction).setB(0.3f);
	    ((BM25ScoringFunction) scoringFunction).setK1(0.5f);
	
	    int docLen = env.getDocumentLength(curDocno);
		this.scoringFunction.initialize(gte, ge);
		score = scoringFunction.getScore(tf, docLen);
	    lastScoredDocno = curDocno;
	    
	    //LOG.info("leaf score = " + score);
    }
    return score;
}

  private float runOperator(float[] scores) {
	if (this.operator.equals("#combine")) {
		float finalScore = 0;
		for (int i = 0; i < scores.length; i++){
			finalScore += scores[i];
		}
		return finalScore;
	}else if (this.operator.equals("#or")) {
		return 0;
	}else{
		return 0;		
	}
  }


  /**
   * @param curDocno
   * @return next smallest docno from posting lists of leaf nodes
   */
  public int getNextCandidate(int docno) {
	if (postingsReader == null) { // not a leaf node
    	for (int i = 0; i < children.size(); i++) {
    		int nextDocno = children.get(i).getNextCandidate(docno);
    		if (nextDocno < docno) {
    			docno = nextDocno;
    		}
    	}
    	return docno;
    }else{	// leaf node
    	if (endOfList) {
            return Integer.MAX_VALUE;
    	}
    	int nextDocno = postingsReader.getDocno();
        if (nextDocno == lastScoredDocno) {
        	boolean t = postingsReader.nextPosting(curPosting);
          if (!t) { // Advance reader.
            endOfList = true;
            return Integer.MAX_VALUE;
          } else {
            return postingsReader.getDocno();
          }
        }
        return nextDocno;
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
}
