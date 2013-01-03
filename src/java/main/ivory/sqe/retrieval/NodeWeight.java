package ivory.sqe.retrieval;

public interface NodeWeight {
  public float getScore();
  
  public void add(NodeWeight other);

  public NodeWeight multiply(float weight);
}
