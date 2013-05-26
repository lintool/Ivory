package ivory.ffg.score;

import ivory.ffg.stats.GlobalStats;

/**
 * Implementation of the Dirichlet scoring function.
 *
 * @author Nima Asadi
 */
public class DirichletScoringFunction implements ScoringFunction {
  private float mu;

  /**
   * @param mu Dirichlet parameter mu
   */
  public DirichletScoringFunction(float mu) {
    this.mu = mu;
  }

  @Override public float computeTermScore(int term, int dl, int tf, GlobalStats stat) {
    return (float) Math.log(((float) tf + mu * termBackgroundProb(term, stat)) / (dl + mu));
  }

  @Override public float computePhraseScore(int dl, int tf, GlobalStats stat) {
    return (float) Math.log(((float) tf + mu * phraseBackgroundProb(stat)) / (dl + mu));
  }

  private float termBackgroundProb(int term, GlobalStats stat) {
    return (float) (stat.getCf(term) / stat.getCollectionLength());
  }

  private float phraseBackgroundProb(GlobalStats stat) {
    return (float) (stat.getDefaultCf() / stat.getCollectionLength());
  }

  @Override public String toString() {
    return "scoringFunction=\"" + DirichletScoringFunction.class.getName() +
      "\" mu=\"" + mu +"\"";
  }
}
