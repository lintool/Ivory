package ivory.ffg.score;

import ivory.ffg.stats.GlobalStats;

/**
 * Implementation of the BM25 scoring function.
 *
 * @author Nima Asadi
 */
public class BM25ScoringFunction implements ScoringFunction {
  private float k1;
  private float b;

  /**
   * @param k1 BM25 parameter k1
   * @param b BM25 parameter b
   */
  public BM25ScoringFunction(float k1, float b) {
    this.k1 = k1;
    this.b = b;
  }

  @Override public float computeTermScore(int term, int dl, int tf, GlobalStats stat) {
    return stat.getIdf(term) * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl/stat.getAvgDocumentLength())));
  }

  @Override public float computePhraseScore(int dl, int tf, GlobalStats stat) {
    return stat.getDefaultIdf() * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl/stat.getAvgDocumentLength())));
  }

  @Override public String toString() {
    return "scoringFunction=\"" + BM25ScoringFunction.class.getName() +
      "\" k1=\"" + k1 + "\" b=\"" + b + "\"";
  }
}
