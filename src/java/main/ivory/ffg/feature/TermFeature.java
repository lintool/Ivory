package ivory.ffg.feature;

import ivory.ffg.score.ScoringFunction;
import ivory.ffg.stats.GlobalStats;

/**
 * Implementation of a term feature.
 *
 * @author Nima Asadi
 */

public class TermFeature implements Feature {
  private ScoringFunction scoringFunction;

  @Override public void initialize(ScoringFunction scoringFunction) {
    this.scoringFunction = scoringFunction;
  }

  @Override public float computeScoreWithSlidingWindow(int[] document, int[] query, int[] hashedQuery, GlobalStats stats) {
    int[] tf = countTerms(document, hashedQuery);

    float score = 0;
    for(int i = 0; i < query.length; i++) {
      score += scoringFunction.computeTermScore(query[i], document.length, tf[i], stats);
    }
    return score;
  }

  @Override public float computeScoreWithMiniIndexes(int[][] positions, int[] query, int dl, GlobalStats stats) {
    float score = 0;
    for(int i = 0; i < query.length; i++) {
      score += scoringFunction.computeTermScore(query[i], dl, positions[i].length, stats);
    }
    return score;
  }

  public static int[] countTerms(int[] document, int[] query) {
    int[] tf = new int[query.length];
    for(int q = 0; q < query.length; q++) {
      for(int i = 0; i < document.length; i++) {
        if(document[i] == query[q]) {
          tf[q]++;
        }
      }
    }
    return tf;
  }

  @Override public String toString() {
    return "featureClass=\"" + TermFeature.class.getName() + "\" " +
      scoringFunction.toString();
  }
}
