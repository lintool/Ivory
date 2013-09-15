package ivory.ffg.feature;

import ivory.ffg.score.ScoringFunction;
import ivory.ffg.stats.GlobalStats;

/**
 * Abstract feature definition. Feature values are computed
 * using a sliding window or an indexed document.
 *
 * @author Nima Asadi
 */

public interface Feature {
  /**
   * Initializes this feature.
   *
   * @param score scoring function
   */
  public void initialize(ScoringFunction score);

  /**
   * Computes the feature value using a sliding window
   * given a document, query and a scoring function
   *
   * @param document Flat array representation of a document vector.
   * @param query Original query terms (used to retrieve global statistics)
   * @param hashedQuery Hashed query terms (the same as query for techniques that don't use hashing)
   * @param stats Global statistics
   * @return Feature value
   */
  public float computeScoreWithSlidingWindow(int[] document, int[] query, int[] hashedQuery, GlobalStats stats);

  /**
   * Computes the feature value using a (mini-)indexed document.
   *
   * @param positions Positions of each term in the query
   * @param query Original query terms
   * @param dl Document length
   * @param stats Global statistics
   * @return Feature value
   */
  public float computeScoreWithMiniIndexes(int[][] positions, int[] query, int dl, GlobalStats stats);
}
