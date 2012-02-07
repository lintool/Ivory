package ivory.ltr.operator;

public class BooleanRatio extends Operator {
  @Override public double getFinalScore() {
    if(scores.size() == 0) {
      return 0;
    }

    int s = 0;
    for(double f: scores) {
      if(f > 0) {
        s++;
      }
    }
    return ((double) s) / scores.size();
  }

  @Override public Operator newInstance() {
    return new BooleanRatio();
  }
}
