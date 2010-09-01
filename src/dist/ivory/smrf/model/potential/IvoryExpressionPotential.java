/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
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

package ivory.smrf.model.potential;

import ivory.data.Posting;
import ivory.data.PostingsReader;
import ivory.data.ProximityPostingsReader;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.Node;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.builder.ExpressionGenerator;
import ivory.smrf.model.score.ScoringFunction;
import ivory.util.RetrievalEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Don Metzler
 *
 */
public class IvoryExpressionPotential extends PotentialFunction {

	/**
	 * default score for potentials with no postings
	 */
	private static final double DEFAULT_SCORE = 0.0;
	
	/**
	 * Indri query environment associated with this potential store 
	 */
	private RetrievalEnvironment mEnv = null;

	/**
	 * Class used to score terms/general expressions 
	 */
	private ScoringFunction mScoringFunction = null;
	
	/**
	 * Class used to generate Indri expressions from clique settings 
	 */
	private ExpressionGenerator mExpressionGenerator = null;
		
	/**
	 * Document node associated with this potential
	 */
	private DocumentNode mDocNode = null;
	
	/**
	 * Term nodes associated with this potential
	 */
	private List<TermNode> mTermNodes = null;
	
	/**
	 * Current postings reader
	 */
	private PostingsReader mPostingsReader = null;
	
	/**
	 * Current posting
	 */
	private Posting mCurPosting = null;
	
	/**
	 * document frequency of _curExpression
	 */
	private int mDf;
	
	/**
	 * collection frequency of _curExpression
	 */
	private long mCf;
		
	/**
	 * whether we're at the end of the postings list or not
	 */
	private boolean mEndOfList;
	
	/**
	 * @param env
	 * @throws SMRFException 
	 */
	public IvoryExpressionPotential(RetrievalEnvironment env, ScoringFunction scoringFunction, ExpressionGenerator expressionGenerator) {
		mEnv = env;
		mScoringFunction = scoringFunction;
		mExpressionGenerator = expressionGenerator;
		mCurPosting = new Posting();
	}
	
	/* (non-Javadoc)
	 * @see ivory.smrf.model.potential.PotentialFunction#initialize(java.util.List, ivory.smrf.model.GlobalEvidence)
	 */
	public void initialize(List<Node> nodes, GlobalEvidence globalEvidence) throws Exception {
		mDocNode = null;
		mTermNodes = new ArrayList<TermNode>();
		
		for (Node node : nodes) {
			if( node instanceof DocumentNode && mDocNode != null ) {
				throw new Exception("Only one document node allowed in cliques associated with IndriExpressionPotential!");
			} else if( node instanceof DocumentNode ) {
				mDocNode = (DocumentNode)node;
			} else if( node instanceof TermNode ) {
				mTermNodes.add((TermNode)node);
			} else {
				throw new Exception("Unrecognized node type in clique associated with IndriExpressionPotential!");
			}
		}
		
		StringBuffer buffer = new StringBuffer();
		for (TermNode termNode : mTermNodes) {
			buffer.append(termNode.getTerm());
			buffer.append(' ');
		}	
		
		String expression = mExpressionGenerator.getExpression( buffer.toString().trim() );

		// get inverted list for this expression
		mPostingsReader = mEnv.getPostingsReader(expression);

		// get collection statistics for the expression
		if(mPostingsReader == null) {
			mDf = 0;
			mCf = 0;
		}
		else if(mPostingsReader instanceof ProximityPostingsReader) {
			mDf = mEnv.getDefaultDF();
			mCf = mEnv.getDefaultCF();
		}
		else {
			mDf = mPostingsReader.getPostingsList().getDf();
			mCf = mPostingsReader.getPostingsList().getCf();
		}

		// set global term evidence in scorer
		GlobalTermEvidence termEvidence = new GlobalTermEvidence(mDf, mCf);
		mScoringFunction.initialize(termEvidence, globalEvidence);

		// read first posting
		if(mPostingsReader == null || (mPostingsReader != null && !mPostingsReader.nextPosting(mCurPosting))) {
			mEndOfList = true;
		}
	}
	
	/* (non-Javadoc)
	 * @see edu.umass.cs.SMRF.model.PotentialFunction#getPotential()
	 */
	@Override
	public double computePotential() {
		// if there are no postings associated with this potential
		// then just return the default score
		if( mPostingsReader == null ) {
			return DEFAULT_SCORE;
		}
		
		// advance postings reader
		// invariant: mCurPosting will always point to the next
		//            posting that has not yet been scored
		while(mCurPosting.getDocno() < mDocNode.getDocumentID() && !mEndOfList) {
			if(!mPostingsReader.nextPosting(mCurPosting)) {
				mEndOfList = true;
			}
		}
		
		// compute term frequency
		double tf = 0.0;
		if( mDocNode.getDocumentID() == mCurPosting.getDocno() ) {
			tf = mCurPosting.getScore();
			if(!mPostingsReader.nextPosting(mCurPosting)) { // advance reader
				mEndOfList = true;
			}
		}
		
		int docLen = mEnv.documentLength(mDocNode.getDocumentID());
		
		double score = mScoringFunction.getScore(tf, docLen);
		
		return score;
	}

	/* (non-Javadoc)
	 * @see edu.umass.cs.SMRF.model.potential.PotentialFunction#getNextCandidate()
	 */
	public int getNextCandidate() {
		if( mPostingsReader == null || mEndOfList ) { // just getting started
			return Integer.MAX_VALUE;
		}
		return mCurPosting.getDocno();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String ret = new String();
		ret = "<potential type=\"IvoryExpression\">\n";
		ret += mScoringFunction;
		ret += mExpressionGenerator;
		ret += "<nodes>\n";
		ret += mDocNode;
		for( Node n : mTermNodes ) {
			ret += n;
		}
		ret += 
		ret += "</nodes>\n";
		return ret + "</potential>\n";
	}

	/* (non-Javadoc)
	 * @see edu.umass.cs.SMRF.model.potential.PotentialFunction#reset()
	 */
	@Override
	public void reset() {
		mEndOfList = false;
		mDf = 0;
		mCf = 0;
	}

	/* (non-Javadoc)
	 * @see ivory.smrf.model.potential.PotentialFunction#getMaxScore()
	 */
	@Override
	public double getMaxScore() {
		return mScoringFunction.getMaxScore();
	}

	/* (non-Javadoc)
	 * @see ivory.smrf.model.potential.PotentialFunction#setNextCandidate(int)
	 */
	@Override
	public void setNextCandidate(int docid) {
		// advance postings reader
		// invariant: mCurPosting will always point to the next
		//            posting that has not yet been scored
		while(mCurPosting.getDocno() < docid && !mEndOfList) {
			if(!mPostingsReader.nextPosting(mCurPosting)) {
				mEndOfList = true;
			}
		}
	}
}
