package ivory.ltr.operator;

public class Max extends Operator {
  @Override public double getFinalScore() {
    double s = Float.MIN_VALUE;
    for(double f: scores) {
      if(f > s) {
        s = f;
      }
    }
    return s;
  }

  @Override public Operator newInstance() {
    return new Max();
  }
}
