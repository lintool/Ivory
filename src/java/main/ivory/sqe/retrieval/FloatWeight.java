package ivory.sqe.retrieval;

public class FloatWeight implements NodeWeight {
  private float score;

  public FloatWeight() {
    score = 0;
  }

  public FloatWeight(float s) {
    score = s;
  }

  public float getScore() {
    return score;
  }

  public void add(NodeWeight o) {
    if (o instanceof FloatWeight){
      FloatWeight other = (FloatWeight) o;
      this.score += other.score;
    }
  }

  public FloatWeight multiply(float weight) {
    return new FloatWeight(score * weight);
  }

  public String toString() {
    return "score(" + score + ")";
  }
}
