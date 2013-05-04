package ivory.ffg.feature;

import ivory.ffg.score.ScoringFunction;
import ivory.ffg.stats.GlobalStats;

/**
 * Implementation of a phrase feature (unordered-window, sequential-dependence model)
 *
 * @author Nima Asadi
 */
public class UnorderedWindowSequentialDependenceFeature implements Feature {
  private int window;
  private ScoringFunction scoringFunction;

  /**
   * @param window Window size
   */
  public UnorderedWindowSequentialDependenceFeature(int window) {
    this.window = window * 2;
  }

  @Override public void initialize(ScoringFunction scoringFunction) {
    this.scoringFunction = scoringFunction;
  }

  @Override public float computeScoreWithSlidingWindow(int[] document, int[] query, int[] hashedQuery, GlobalStats stats) {
    if(query.length == 1) {
      return 0f;
    }

    int[] tf = countTerms(document, hashedQuery, window);

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

    int[] tf = countTerms(positions, window);

    float score = 0;
    for(int i = 0; i < tf.length; i++) {
      score += scoringFunction.computePhraseScore(dl, tf[i], stats);
    }
    return score;
  }

  public static int[] countTerms(int[][] positions, int window) {
    int[] tf = new int[positions.length - 1];

    for(int i = 0; i < positions.length - 1; i++) {
      int[] p = positions[i];
      int[] pn = positions[i + 1];

      for(int j = 0; j < p.length; j++) {
        for(int k = 0; k < pn.length; k++) {
          if(pn[k] > p[j] && (pn[k] - p[j] + 1) <= window) {
            tf[i]++;
            break;
          } else if(pn[k] < p[j] && (p[j] - pn[k] + 1) <= window) {
            if(j > 0) {
              if(p[j - 1] < pn[k]) {
                tf[i]++;
              }
            } else {
              tf[i]++;
            }
          }
        }
      }
    }
    return tf;
  }

  public static int[] countTerms(int[] document, int[] query, int window) {
    int[] tf = new int[query.length - 1];

    for(int i = 0; i < document.length; i++) {
      if(document[i] != query[0]) {
        continue;
      }

      for(int j = i + 1; j < i + window && j < document.length; j++) {
        if(document[j] == query[1]) {
          tf[0]++;
          break;
        }
      }
    }

    for(int q = 1; q < query.length - 1; q++) {
      for(int i = 0; i < document.length; i++) {
        if(document[i] != query[q]) {
          continue;
        }

        for(int j = i + 1; j < i + window && j < document.length; j++) {
          if(document[j] == query[q + 1]) {
            tf[q]++;
            break;
          }
        }

        for(int j = i + 1; j < i + window && j < document.length; j++) {
          if(document[j] == query[q - 1]) {
            tf[q - 1]++;
            break;
          }
        }
      }
    }

    int e = query.length - 1;
    for(int i = 0; i < document.length; i++) {
      if(document[i] != query[e]) {
        continue;
      }

      for(int j = i + 1; j < i + window && j < document.length; j++) {
        if(document[j] == query[e - 1]) {
          tf[e - 1]++;
          break;
        }
      }
    }
    return tf;
  }

  @Override public String toString() {
    return "featureClass=\"" + UnorderedWindowSequentialDependenceFeature.class.getName() +
      "\" width=\"" + window + "\" " +
      scoringFunction.toString();
  }
}
