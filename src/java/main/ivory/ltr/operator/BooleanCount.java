package ivory.ltr.operator;

public class BooleanCount extends Operator {
  @Override public double getFinalScore() {
    int s = 0;
    for(double f: scores) {
      if(f > 0) {
        s++;
      }
    }
    return ((double) s);
  }

  @Override public Operator newInstance() {
    return new BooleanCount();
  }
}
