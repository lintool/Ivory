package ivory.ffg.feature;

import ivory.ffg.score.ScoringFunction;
import ivory.ffg.stats.GlobalStats;

/**
 * Implementation of a phrase feature (ordered-window, sequential-dependence model)
 *
 * @author Nima Asadi
 */
public class OrderedWindowSequentialDependenceFeature implements Feature {
  private int gap;
  private ScoringFunction scoringFunction;

  /**
   * @param gap Gap size
   */
  public OrderedWindowSequentialDependenceFeature(int gap) {
    this.gap = gap;
  }

  @Override public void initialize(ScoringFunction scoringFunction) {
    this.scoringFunction = scoringFunction;
  }

  @Override public float computeScoreWithSlidingWindow(int[] document, int[] query, int[] hashedQuery, GlobalStats stats) {
    if(query.length == 1) {
      return 0f;
    }

    int[] tf = countTerms(document, hashedQuery, gap);

    float score = 0;
    for(int i = 0; i < tf.length; i++) {
      score += scoringFunction.computePhraseScore(document.length, tf[i], stats);
    }
    return score;
  }

  @Override public float computeScoreWithMiniIndexes(int[][] positions, int[] query, int dl, GlobalStats stats) {
    if(query.length == 1) {
      return 0f;
    }

    int[] tf = countTerms(positions, gap);

    float score = 0;
    for(int i = 0; i < tf.length; i++) {
      score += scoringFunction.computePhraseScore(dl, tf[i], stats);
    }
    return score;
  }

  public static int[] countTerms(int[][] positions, int gap) {
    int[] tf = new int[positions.length - 1];

    for(int i = 0; i < positions.length - 1; i++) {
      int[] p = positions[i];
      int[] pn = positions[i + 1];

      for(int j = 0; j < p.length; j++) {
        for(int k = 0; k < pn.length; k++) {
          if(pn[k] > p[j] && (pn[k] - p[j] - 1) <= gap) {
            tf[i]++;
            break;
          }
        }
      }
    }
    return tf;
  }

  public static int[] countTerms(int[] document, int[] query, int gap) {
    int[] tf = new int[query.length - 1];

    for(int q = 0; q < query.length - 1; q++) {
      for(int i = 0; i < document.length; i++) {
        if(document[i] != query[q]) {
          continue;
        }
        for(int j = i + 1; j < i + gap + 2 && j < document.length; j++) {
          if(document[j] == query[q + 1]) {
            tf[q]++;
            break;
          }
        }
      }
    }
    return tf;
  }

  @Override public String toString() {
    return "featureClass=\"" + OrderedWindowSequentialDependenceFeature.class.getName() +
      "\" width=\"" + gap + "\" " +
      scoringFunction.toString();
  }
}
