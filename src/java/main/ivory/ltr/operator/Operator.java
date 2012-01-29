package ivory.ltr.operator;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * Holds and computes the final value of a feature
 *
 * @author Nima Asadi
 */
public abstract class Operator {
  protected List<Double> scores;

  protected Operator() {
    scores = Lists.newArrayList();
  }

  /**
   * Adds a new score
   *
   * @param score Score
   */
  public void addScore(double score) {
    scores.add(score);
  }

  /**
   * Clears the scores
   */
  public void clear() {
    scores.clear();
  }

  /**
   * Computes the final feature value
   *
   * @return Feature value
   */
  public abstract double getFinalScore();

  /**
   * @return New instance
   */
  public abstract Operator newInstance();
}
