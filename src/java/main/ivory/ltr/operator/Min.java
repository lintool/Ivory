package ivory.ltr.operator;

public class Min extends Operator {
  @Override public double getFinalScore() {
    double s = Double.POSITIVE_INFINITY;
    for(double f: scores) {
      if(f < s) {
        s = f;
      }
    }
    return s;
  }

  @Override public Operator newInstance() {
    return new Min();
  }
}
