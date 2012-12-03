package ivory.sqe.retrieval;

public class TfDfWeight implements NodeWeight {
  private float tf, df;
  private int docLen;
  private int numDocs;
  private float avgDocLen;

  public TfDfWeight() {
    tf = 0;
    df = 0;
  }

  public TfDfWeight(float tf, float df, int docLen, int numDocs, float avgDocLen) {
    this.tf = tf;
    this.df = df;
    this.numDocs = numDocs;
    this.docLen = docLen;
    this.avgDocLen = avgDocLen;
  }

  public float getScore() {
    float b = 0.3f;
    float k1 = 0.5f;

    float idf = (float) Math.log((numDocs - df + 0.5f) / (df+ 0.5f));
    float bm25 = ((k1 + 1.0f) * tf) / (k1 * ((1.0f - b) + b * docLen / avgDocLen) + tf) * idf;
    return bm25;
  }

  public void add(NodeWeight o) {
    if (o instanceof TfDfWeight) {
      TfDfWeight other = (TfDfWeight) o;
      tf += other.tf;
      df += other.df;
    }
  }

  public TfDfWeight multiply(float weight) {
    return new TfDfWeight(tf * weight, df * weight, docLen, numDocs, avgDocLen);
  }

  public String toString() {
    return "tfdf(" + tf + "," + df + "," + numDocs + "," + docLen + "," + avgDocLen + ")" ;
  }
}
