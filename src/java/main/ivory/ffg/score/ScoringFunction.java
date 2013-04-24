package ivory.ffg.score;

import ivory.ffg.stats.GlobalStats;

/**
 * Abstract scoring function.
 *
 * @author Nima Asadi
 */
public interface ScoringFunction {
  /**
   * Computes term feature value.
   *
   * @param dl Document length
   * @param tf Term frequency
   * @param stats Global statistics
   * @return Feature value
   */
  public float computeTermScore(int term, int dl, int tf, GlobalStats stats);

  /**
   * Computes phrase feature value.
   *
   * @param dl Document length
   * @param tf Phrase frequency
   * @param stats Global statistics
   * @return Feature value
   */
  public float computePhraseScore(int dl, int tf, GlobalStats stats);
}
