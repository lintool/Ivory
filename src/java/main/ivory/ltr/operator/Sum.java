package ivory.ltr.operator;

public class Sum extends Operator {
  @Override public double getFinalScore() {
    double s = 0;
    for(double f: scores) {
      s += f;
    }
    return s;
  }

  @Override public Operator newInstance() {
    return new Sum();
  }
}
