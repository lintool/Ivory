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

import ivory.core.ConfigurationException;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsReader;
import ivory.core.data.index.ProximityPostingsReader;
import ivory.core.util.XMLTools;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.builder.Expression;
import ivory.smrf.model.builder.ExpressionGenerator;
import ivory.smrf.model.score.ScoringFunction;

import java.util.List;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Query potential.
 *
 * @author Don Metzler
 */
public class QueryPotential extends PotentialFunction {
  // Default score for potentials with no postings.
  protected static final float DEFAULT_SCORE = 0.0f;

  private ExpressionGenerator expressionGenerator;

  protected final List<TermNode> termNodes = Lists.newArrayList();
  protected final GlobalTermEvidence termEvidence = new GlobalTermEvidence();
  protected final Posting curPosting = new Posting();

  protected RetrievalEnvironment env;
  protected ScoringFunction scoringFunction;
  protected DocumentNode docNode = null;
  protected PostingsReader postingsReader = null;

  protected boolean endOfList = true; // Whether or not we're at the end of the postings list.
  protected int lastScoredDocno = 0;

  public QueryPotential() {}
  // Note, must have zero-arg constructor for creation by factory method in PotentialFunction

  public QueryPotential(RetrievalEnvironment env, ExpressionGenerator generator,
      ScoringFunction scoringFunction) {
    this.env = Preconditions.checkNotNull(env);
    this.expressionGenerator = Preconditions.checkNotNull(generator);
    this.scoringFunction = Preconditions.checkNotNull(scoringFunction);
  }

  @Override
  public void configure(RetrievalEnvironment env, Node domNode) throws ConfigurationException {
    this.env = Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(domNode);

    String generatorType = XMLTools.getAttributeValueOrThrowException(domNode, "generator",
        "A generator attribute must be specified in order to generate a potential function!");
    expressionGenerator = ExpressionGenerator.create(generatorType, domNode);

    String scoreFunctionType = XMLTools.getAttributeValue(domNode, "scoreFunction",
        "A scoreFunction attribute must be specified in order to generate a potential function!");
    scoringFunction = ScoringFunction.create(scoreFunctionType, domNode);
  }

  @Override
  public void initialize(List<GraphNode> nodes, GlobalEvidence globalEvidence)
      throws ConfigurationException {
    Preconditions.checkNotNull(nodes);
    Preconditions.checkNotNull(globalEvidence);

    docNode = null;
    termNodes.clear();

    for (GraphNode node : nodes) {
      if (node.getType() == GraphNode.Type.DOCUMENT && docNode != null) {
        throw new ConfigurationException("Only one document node allowed in QueryPotential!");
      } else if (node.getType() == GraphNode.Type.DOCUMENT) {
        docNode = (DocumentNode) node;
      } else if (node.getType() == GraphNode.Type.TERM) {
        termNodes.add((TermNode) node);
      } else {
        throw new ConfigurationException(
            "Unrecognized node type in clique associated with QueryPotential!");
      }
    }

    String[] terms = new String[termNodes.size()];
    for (int i = 0; i < termNodes.size(); i++) {
      terms[i] = termNodes.get(i).getTerm();
    }

    Expression expression = expressionGenerator.getExpression(terms);

    // Get inverted list for this expression.
    postingsReader = env.getPostingsReader(expression);

    // Get collection statistics for the expression.
    if (postingsReader == null) {
      termEvidence.set(0, 0L);
    } else if (postingsReader instanceof ProximityPostingsReader) {
      termEvidence.set(env.getDefaultDf(), env.getDefaultCf());
    } else {
      termEvidence.set(postingsReader.getPostingsList().getDf(),
          postingsReader.getPostingsList().getCf());
    }

    // Set global term evidence in scoring function.
    scoringFunction.initialize(termEvidence, globalEvidence);

    // Read first posting.
    endOfList = false;
    if (postingsReader == null) {
      endOfList = true;
    }

    lastScoredDocno = 0;
  }

  @Override
  public float computePotential() {
    // If there are no postings associated with this potential then just
    // return the default score.
    if (postingsReader == null) {
      return DEFAULT_SCORE;
    }

    // Advance postings reader. Invariant: curPosting will always point to
    // the next posting that has not yet been scored.
    while (!endOfList && postingsReader.getDocno() < docNode.getDocno()) {
      if (!postingsReader.nextPosting(curPosting)) {
        endOfList = true;
      }
    }

    // Compute term frequency.
    int tf = 0;
    if (docNode.getDocno() == postingsReader.getDocno()) {
      tf = postingsReader.getTf();
    }

    int docLen = env.getDocumentLength(docNode.getDocno());
    float score = scoringFunction.getScore(tf, docLen);
    lastScoredDocno = docNode.getDocno();

    return score;
  }

  @Override
  public int getNextCandidate() {
    if (postingsReader == null || endOfList) { // Just getting started.
      return Integer.MAX_VALUE;
    }

    int nextDocno = postingsReader.getDocno();
    if (nextDocno == lastScoredDocno) {
      if (!postingsReader.nextPosting(curPosting)) { // Advance reader.
        endOfList = true;
        return Integer.MAX_VALUE;
      } else {
        return postingsReader.getDocno();
      }
    }

    return nextDocno;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("<potential type=\"QueryPotential\">\n");
    sb.append(scoringFunction);
    sb.append(expressionGenerator);
    sb.append("<nodes>\n");
    sb.append(docNode);

    for (GraphNode n : termNodes) {
      sb.append(n);
    }

    sb.append("</nodes>\n");
    sb.append("</potential>\n");

    return sb.toString();
  }

  @Override
  public void reset() {
    endOfList = false;
    lastScoredDocno = -1;
  }

  @Override
  public float getMinScore() {
    return scoringFunction.getMinScore();
  }

  @Override
  public float getMaxScore() {
    return scoringFunction.getMaxScore();
  }

  @Override
  public void setNextCandidate(int docno) {
    // Advance postings reader. Invariant: curPosting will always point to
    // the next posting that has not yet been scored.
    while (!endOfList && postingsReader.getDocno() < docno) {
      if (!postingsReader.nextPosting(curPosting)) {
        endOfList = true;
      }
    }
  }

  /**
   * Returns the scoring function associated with this potential.
   *
   * @return scoring function associated with this potential
   */
  public ScoringFunction getScoringFunction() {
    return this.scoringFunction;
  }
}
