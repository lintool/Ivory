package ivory.ltr.operator;

public class Max extends Operator {
  @Override public double getFinalScore() {
    double s = Double.NEGATIVE_INFINITY;
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
