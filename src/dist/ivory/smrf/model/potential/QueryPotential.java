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

import ivory.data.Posting;
import ivory.data.PostingsReader;
import ivory.data.ProximityPostingsReader;
import ivory.exception.ConfigurationException;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.builder.Expression;
import ivory.smrf.model.builder.ExpressionGenerator;
import ivory.smrf.model.score.ScoringFunction;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 * 
 */
public class QueryPotential extends PotentialFunction {

	// Default score for potentials with no postings.
	private static final float DEFAULT_SCORE = 0.0f;

	private RetrievalEnvironment mEnv = null;
	private ScoringFunction mScoringFunction = null;
	private ExpressionGenerator mExpressionGenerator = null;

	private DocumentNode mDocNode = null;
	private final List<TermNode> mTermNodes = new ArrayList<TermNode>();
	private final GlobalTermEvidence mTermEvidence = new GlobalTermEvidence();
	
	private PostingsReader mPostingsReader = null;
	private Posting mCurPosting = null;

	// Whether we're at the end of the postings list or not.
	private boolean mEndOfList = true;

	// Docno of last document scored.
	private int mLastScoredDocno = 0;

	public QueryPotential() {
	}

	public QueryPotential(RetrievalEnvironment env, ExpressionGenerator generator,
			ScoringFunction scoringFunction) {
		Preconditions.checkNotNull(env);
		Preconditions.checkNotNull(generator);
		Preconditions.checkNotNull(scoringFunction);

		mEnv = env;
		mExpressionGenerator = generator;
		mScoringFunction = scoringFunction;

		mCurPosting = new Posting();
	}

	public void configure(RetrievalEnvironment env, Node domNode) throws ConfigurationException {
		Preconditions.checkNotNull(env);
		Preconditions.checkNotNull(domNode);

		String generatorType = XMLTools.getAttributeValue(domNode, "generator", null);
		if (generatorType == null) {
			throw new ConfigurationException(
					"A generator attribute must be specified in order to generate a potential function!");
		}
		mExpressionGenerator = ExpressionGenerator.create(generatorType, domNode);

		String scoreFunctionType = XMLTools.getAttributeValue(domNode, "scoreFunction", null);
		if (scoreFunctionType == null) {
			throw new ConfigurationException(
					"A scoreFunction attribute must be specified in order to generate a potential function!");
		}
		mScoringFunction = ScoringFunction.create(scoreFunctionType, domNode);

		mEnv = env;
		mCurPosting = new Posting();
	}

	public void initialize(List<GraphNode> nodes, GlobalEvidence globalEvidence)
			throws ConfigurationException {
		Preconditions.checkNotNull(nodes);
		Preconditions.checkNotNull(globalEvidence);

		mDocNode = null;
		mTermNodes.clear();

		for (GraphNode node : nodes) {
			if (node.getType() == GraphNode.Type.DOCUMENT && mDocNode != null) {
				throw new ConfigurationException("Only one document node allowed in QueryPotential!");
			} else if (node.getType() == GraphNode.Type.DOCUMENT) {
				mDocNode = (DocumentNode) node;
			} else if (node.getType() == GraphNode.Type.TERM) {
				mTermNodes.add((TermNode) node);
			} else {
				throw new ConfigurationException("Unrecognized node type in clique associated with QueryPotential!");
			}
		}

		String[] terms = new String[mTermNodes.size()];
		for (int i = 0; i < mTermNodes.size(); i++) {
			terms[i] = mTermNodes.get(i).getTerm();
		}

		Expression expression = mExpressionGenerator.getExpression(terms);

		// Get inverted list for this expression.
		mPostingsReader = mEnv.getPostingsReader(expression);

		// Get collection statistics for the expression.
		if (mPostingsReader == null) {
			mTermEvidence.df = 0;
			mTermEvidence.cf = 0;
		} else if (mPostingsReader instanceof ProximityPostingsReader) {
			mTermEvidence.df = mEnv.getDefaultDf();
			mTermEvidence.cf = mEnv.getDefaultCf();
		} else {
			mTermEvidence.df = mPostingsReader.getPostingsList().getDf();
			mTermEvidence.cf = mPostingsReader.getPostingsList().getCf();
		}

		// Set global term evidence in scoring function.
		mScoringFunction.initialize(mTermEvidence, globalEvidence);

		// Read first posting.
		mEndOfList = false;
		if (mPostingsReader == null) {
			mEndOfList = true;
		}

		mLastScoredDocno = 0;
	}

	@Override
	public float computePotential() {
		// If there are no postings associated with this potential then just
		// return the default score.
		if (mPostingsReader == null) {
			return DEFAULT_SCORE;
		}

		// Advance postings reader. Invariant: mCurPosting will always point to
		// the next posting that has not yet been scored.
		while (!mEndOfList && mPostingsReader.getDocno() < mDocNode.getDocno()) {
			if (!mPostingsReader.nextPosting(mCurPosting)) {
				mEndOfList = true;
			}
		}

		// Compute term frequency.
		int tf = 0;
		if (mDocNode.getDocno() == mPostingsReader.getDocno()) {
			tf = mPostingsReader.getScore();
		}

		int docLen = mEnv.documentLength(mDocNode.getDocno());
		float score = mScoringFunction.getScore(tf, docLen);
		mLastScoredDocno = mDocNode.getDocno();

		return score;
	}

	@Override
	public int getNextCandidate() {
		if (mPostingsReader == null || mEndOfList) { // Just getting started.
			return Integer.MAX_VALUE;
		}

		int nextDocno = mPostingsReader.getDocno();

		if (nextDocno == mLastScoredDocno) {
			if (!mPostingsReader.nextPosting(mCurPosting)) { // Advance reader.
				mEndOfList = true;
				return Integer.MAX_VALUE;
			} else {
				return mPostingsReader.getDocno();
			}
		}

		return nextDocno;
	}

	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();

		ret.append("<potential type=\"QueryPotential\">\n");
		ret.append(mScoringFunction);
		ret.append(mExpressionGenerator);
		ret.append("<nodes>\n");
		ret.append(mDocNode);

		for (GraphNode n : mTermNodes) {
			ret.append(n);
		}

		ret.append("</nodes>\n");
		ret.append("</potential>\n");

		return ret.toString();
	}

	@Override
	public void reset() {
		mEndOfList = false;
		mLastScoredDocno = -1;
	}

	@Override
	public float getMaxScore() {
		return mScoringFunction.getMaxScore();
	}

	@Override
	public void setNextCandidate(int docno) {
		// Advance postings reader. Invariant: mCurPosting will always point to
		// the next posting that has not yet been scored.
		while (!mEndOfList && mPostingsReader.getDocno() < docno) {
			if (!mPostingsReader.nextPosting(mCurPosting)) {
				mEndOfList = true;
			}
		}
	}
}
