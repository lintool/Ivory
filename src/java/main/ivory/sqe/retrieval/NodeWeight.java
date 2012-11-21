package ivory.sqe.retrieval;

public interface NodeWeight {
  public float getBM25(int numDocs, int docLen, float avgDocLen);
  
  public void add(NodeWeight other);

  public NodeWeight multiply(float weight);


}
