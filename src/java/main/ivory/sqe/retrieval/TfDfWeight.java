package ivory.sqe.retrieval;

public class TfDfWeight implements NodeWeight {
  //  Map<String,PairOfFloats> term2TfIDf;
  float tf, df;

  public TfDfWeight() {
    //    term2TfIDf = new HashMap<String,PairOfFloats>();
    tf = 0;
    df = 0;
  }

  public TfDfWeight(float t, float d) {
    tf = t;
    df = d;
  }

  public float getBM25(int numDocs, int docLen, float avgDocLen) {
    float b = 0.3f;
    float k1 = 0.5f;
    //    float totalBM25 = 0f;
    //    for (java.util.Map.Entry<String, PairOfFloats> entry : term2TfIDf.entrySet()) {
    //      float tf = entry.getValue().getLeftElement();
    //      float df = entry.getValue().getRightElement();
    float idf = (float) Math.log((numDocs - df + 0.5f) / (df+ 0.5f));
    float bm25 = ((k1 + 1.0f) * tf) / (k1 * ((1.0f - b) + b * docLen / avgDocLen) + tf) * idf;
    //      totalBM25 += bm25;
    //    }
    //    return totalBM25;
    return bm25;
  }

  public void add(NodeWeight o) {
    if (o instanceof TfDfWeight){
      TfDfWeight other = (TfDfWeight) o;

      //    if (term2TfIDf.containsKey(term)) {
      //      PairOfFloats pair = term2TfIDf.get(term);
      //      pair.set(pair.getLeftElement() + tf, pair.getRightElement() + df);
      //      term2TfIDf.put(term, pair);
      //    } else {
      //      term2TfIDf.put(term, new PairOfFloats(tf, df));
      //    }
      tf += other.tf;
      df += other.df;
    }
  }

  //  public void put(String term, int tf, int df) {
  //    term2TfIDf.put(term, new PairOfFloats(tf, df));  
  //  }

  //  public void addAll(TfDfPair tfIDfMap) {
  //    for (java.util.Map.Entry<String, PairOfFloats> entry : tfIDfMap.entrySet()) {
  //      add(entry.getKey(), entry.getValue().getLeftElement(), entry.getValue().getRightElement());
  //    }
  //  }

  public TfDfWeight multiply(float weight) {
    return new TfDfWeight(tf * weight, df * weight);
    //    for (java.util.Map.Entry<String, PairOfFloats> entry : term2TfIDf.entrySet()) {
    //      PairOfFloats pair = entry.getValue();
    //      pair.set(pair.getLeftElement() * weight, pair.getRightElement() * weight);
    //      term2TfIDf.put(entry.getKey(), pair);
    //    }
    //    return this;
  }

  //  private Set<java.util.Map.Entry<String, PairOfFloats>> entrySet() {
  //    return term2TfIDf.entrySet();
  //  }

  public String toString() {
    String s = "";
    //    for (java.util.Map.Entry<String, PairOfFloats> entry : term2TfIDf.entrySet()) {
    //      s += entry.getKey() + " --> " + entry.getValue() + "\n";
    //    }
    s += "tfdf(" + tf + "," + df + ")";
    return s;
  }

}
