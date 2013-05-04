package ivory.ffg.score;

import ivory.ffg.stats.GlobalStats;

/**
 * Returns the Tf-Idf scores
 *
 * @author Nima Asadi
 */
public class TfIdfScoringFunction implements ScoringFunction {
  @Override public float computeTermScore(int term, int dl, int tf, GlobalStats stat) {
    return tf * stat.getIdf(term);
  }

  @Override public float computePhraseScore(int dl, int tf, GlobalStats stat) {
    return tf * stat.getDefaultIdf();
  }

  @Override public String toString() {
    return "scoringFunction=\"" + TfIdfScoringFunction.class.getName() + "\"";
  }
}
