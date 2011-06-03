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

import ivory.smrf.model.builder.ExpressionGenerator;
import ivory.smrf.model.score.ScoringFunction;
import ivory.util.RetrievalEnvironment;

import java.util.Arrays;

/**
 * @author Lidan Wang
 * 
 */
public class CascadeQueryPotential extends QueryPotential {

  //If it's a term feature, store its positions at current document in the postings list
  private int [] positions; 
  private int document_length; 

  public CascadeQueryPotential() {
  }

  public CascadeQueryPotential(RetrievalEnvironment env, ExpressionGenerator generator,
      ScoringFunction scoringFunction) {

    super(env, generator, scoringFunction);
  }


  public float termCollectionCF(){
    return termEvidence.getCf();
  }

  public float termCollectionDF(){
    return termEvidence.getDf();
  }


  public int getDocno(){
    return postingsReader.getDocno();
  }


  @Override
  public float computePotential() {

    // If there are no postings associated with this potential then just
    // return the default score.
    if (postingsReader == null) {
      return DEFAULT_SCORE;
    }

    // Advance postings reader. Invariant: mCurPosting will always point to
    // the next posting that has not yet been scored.

    while (!endOfList && postingsReader.getDocno() < docNode.getDocno()) {

      if (!postingsReader.nextPosting(curPosting)) {
        endOfList = true;
      }
    }

    // Compute term frequency.
    int tf = 0;

    int docLen = env.getDocumentLength(docNode.getDocno());
    document_length = -1;

    positions = null;

    if (docNode.getDocno() == postingsReader.getDocno()) {

      document_length = docLen;

      //getPositions() only defined for term features
      if (termNodes.size()==1){

        int [] p = postingsReader.getPositions();
          
        positions = Arrays.copyOf(p, p.length);

      }
      tf = postingsReader.getScore();  //even if two terms match, tf can be 0, i.e., if they aren't within the window size in the doc
    }

    float score = scoringFunction.getScore(tf, docLen);

    lastScoredDocno = docNode.getDocno();


    return score;
  }


  public void resetPostingsListReader(){
    try{
      postingsReader.reset();
    }
    catch(Exception e){
      System.out.println("Postings for this query doesn't exist!");
      System.exit(-1);
    }

    endOfList = false;
                lastScoredDocno = -1;

  }

  public int getNumberOfPostings(){
    return postingsReader.getNumberOfPostings();
  }

  public int getWindowSize(){
    return postingsReader.getWindowSize();
  }

  public String getScoringFunctionName(){
    if (scoringFunction.toString().indexOf("Dirichlet")!=-1){
      return "dirichlet";
    }
    else if (scoringFunction.toString().indexOf("BM25")!=-1){
      return "bm25";
    }
    else{
      return null;
    }
  }

  public ScoringFunction getScoringFunction() {
    return scoringFunction;
  }
        public int [] getPositions(){
    return positions;
  }

        public int getDocLen(){
    return document_length;
  }
}
