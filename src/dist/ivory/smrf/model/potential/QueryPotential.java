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

package ivory.smrf.model.potential;

import ivory.data.ProximityPostingsReader;
import ivory.data.Posting;
import ivory.data.PostingsReader;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.builder.ExpressionGenerator;
import ivory.smrf.model.score.ScoringFunction;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 * 
 */
public class QueryPotential extends PotentialFunction {

	/**
	 * default score for potentials with no postings
	 */
	private static final double DEFAULT_SCORE = 0.0;

	/**
	 * query environment associated with this potential
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
	 * document frequency of current expression
	 */
	private int mDf = 0;

	/**
	 * collection frequency of current expression
	 */
	private long mCf = 0;

	/**
	 * whether we're at the end of the postings list or not
	 */
	private boolean mEndOfList = true;

	/**
	 * docno of last document scored
	 */
	private int mLastScoredDocno = -1;

	public QueryPotential() {
	}
	
	/**
	 * @param env
	 * @param generator
	 * @param scoringFunction
	 */
	public QueryPotential(RetrievalEnvironment env, ExpressionGenerator generator, ScoringFunction scoringFunction) {
		mEnv = env;
		mExpressionGenerator = generator;
		mScoringFunction = scoringFunction;
		mCurPosting = new Posting();
	}
	
	public void configure(RetrievalEnvironment env, Node domNode) throws Exception {
		// get expression generator type
		String generatorType = XMLTools.getAttributeValue(domNode, "generator", null);
		if (generatorType == null) {
			throw new Exception(
					"A generator attribute must be specified in order to generate a potential function!");
		}
		mExpressionGenerator = ExpressionGenerator.create(generatorType, domNode);

		// get score function
		String scoreFunctionType = XMLTools.getAttributeValue(domNode, "scoreFunction", null);
		if (scoreFunctionType == null) {
			throw new Exception(
					"A scoreFunction attribute must be specified in order to generate a potential function!");
		}
		mScoringFunction = ScoringFunction.create(scoreFunctionType, domNode);

		mEnv = env;
		mCurPosting = new Posting();
	}

	public void initialize(List<GraphNode> nodes, GlobalEvidence globalEvidence) throws Exception {
		mDocNode = null;
		mTermNodes = new ArrayList<TermNode>();

		for (GraphNode node : nodes) {
			if (node.getType() == GraphNode.DOCUMENT && mDocNode != null) {
				throw new Exception(
						"Only one document node allowed in cliques associated with QueryPotential!");
			} else if (node.getType() == GraphNode.DOCUMENT) {
				mDocNode = (DocumentNode) node;
			} else if (node.getType() == GraphNode.TERM) {
				mTermNodes.add((TermNode) node);
			} else {
				throw new Exception(
						"Unrecognized node type in clique associated with QueryPotential!");
			}
		}

		String[] terms = new String[mTermNodes.size()];
		for (int i = 0; i < mTermNodes.size(); i++) {
			terms[i] = mTermNodes.get(i).getTerm();
		}

		String expression = mExpressionGenerator.getExpression(terms);

		// get inverted list for this expression
		mPostingsReader = mEnv.getPostingsReader(expression);

		// get collection statistics for the expression
		if (mPostingsReader == null) {
			mDf = 0;
			mCf = 0;
		} else if (mPostingsReader instanceof ProximityPostingsReader) {
			mDf = mEnv.getDefaultDf();
			mCf = mEnv.getDefaultCf();
		} else {
			mDf = mPostingsReader.getPostingsList().getDf();
			mCf = mPostingsReader.getPostingsList().getCf();
		}

		// set global term evidence in scorer
		GlobalTermEvidence termEvidence = new GlobalTermEvidence(mDf, mCf);
		mScoringFunction.initialize(termEvidence, globalEvidence);

		// read first posting
		mEndOfList = false;
		if (mPostingsReader == null) {
				//|| (mPostingsReader != null && !mPostingsReader.nextPosting(mCurPosting))) {
			mEndOfList = true;
		}
		
		mLastScoredDocno = 0;
	}

	@Override
	public double computePotential() {
		// if there are no postings associated with this potential
		// then just return the default score
		if (mPostingsReader == null) {
			return DEFAULT_SCORE;
		}

		// advance postings reader
		// invariant: mCurPosting will always point to the next
		// posting that has not yet been scored
		while (!mEndOfList && mPostingsReader.getDocno() < mDocNode.getDocno()) {
			if (!mPostingsReader.nextPosting(mCurPosting)) {
				mEndOfList = true;
			}
		}

		// compute term frequency
		double tf = 0.0;
		if (mDocNode.getDocno() == mPostingsReader.getDocno()) {
			tf = mPostingsReader.getScore(); //mCurPosting.getScore();
		}

		int docLen = mEnv.documentLength(mDocNode.getDocno());

		double score = mScoringFunction.getScore(tf, docLen);
		
		mLastScoredDocno = mDocNode.getDocno();
		
		return score;
	}

	public int getNextCandidate() {
		if (mPostingsReader == null || mEndOfList) { // just getting started
			return Integer.MAX_VALUE;
		}
		int nextDocno = mPostingsReader.getDocno();
		if(nextDocno == mLastScoredDocno) {
			if (!mPostingsReader.nextPosting(mCurPosting)) { // advance reader
				mEndOfList = true;
				return Integer.MAX_VALUE;
			}
			else {
				return mPostingsReader.getDocno();
			}
		}
		return nextDocno;
	}

	@Override
	public String toString() {
		String ret = new String();
		ret = "<potential type=\"QueryPotential\">\n";
		ret += mScoringFunction;
		ret += mExpressionGenerator;
		ret += "<nodes>\n";
		ret += mDocNode;
		for (GraphNode n : mTermNodes) {
			ret += n;
		}

		ret += "</nodes>\n";
		return ret + "</potential>\n";
	}

	@Override
	public void reset() {
		mEndOfList = false;
		mDf = 0;
		mCf = 0;
		mLastScoredDocno = -1;
	}

	@Override
	public double getMaxScore() {
		return mScoringFunction.getMaxScore();
	}

	@Override
	public void setNextCandidate(int docno) {
		// advance postings reader
		// invariant: mCurPosting will always point to the next
		// posting that has not yet been scored
		while (!mEndOfList && mPostingsReader.getDocno() < docno) {
			if (!mPostingsReader.nextPosting(mCurPosting)) {
				mEndOfList = true;
			}
		}
	}
}
