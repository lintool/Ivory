package ivory.ffg.score;

import ivory.ffg.stats.GlobalStats;

/**
 * Returns the raw Term Frequency value
 *
 * @author Nima Asadi
 */
public class TfScoringFunction implements ScoringFunction {
  @Override public float computeTermScore(int term, int dl, int tf, GlobalStats stat) {
    return tf;
  }

  @Override public float computePhraseScore(int dl, int tf, GlobalStats stat) {
    return tf;
  }

  @Override public String toString() {
    return "scoringFunction=\"" + TfScoringFunction.class.getName() + "\"";
  }
}
