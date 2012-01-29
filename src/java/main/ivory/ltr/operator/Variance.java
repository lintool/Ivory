package ivory.ltr.operator;

public class Variance extends Operator {
  @Override public double getFinalScore() {
    if(scores.size() == 0) {
      return 0;
    }

    double mean = 0;
    for(double f: scores) {
      mean += f;
    }
    mean /= scores.size();

    double var = 0;
    for(double f: scores) {
      var += Math.pow((f - mean), 2);
    }

    return var / scores.size();
  }

  @Override public Operator newInstance() {
    return new Variance();
  }
}
