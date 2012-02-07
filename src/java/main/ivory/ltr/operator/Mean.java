package ivory.ltr.operator;

public class Mean extends Operator {
  @Override public double getFinalScore() {
    if(scores.size() == 0) {
      return 0;
    }

    double s = 0;
    for(double f: scores) {
      s += f;
    }

    return (s / scores.size());
  }

  @Override public Operator newInstance() {
    return new Mean();
  }
}
