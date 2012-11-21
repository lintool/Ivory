package ivory.sqe.retrieval;

public class FloatWeight implements NodeWeight {
  float bm25;

  public FloatWeight() {
    bm25 = 0;
  }

  public FloatWeight(float s) {
    bm25 = s;
  }

  public float getBM25(int numDocs, int docLen, float avgDocLen) {
    return bm25;
  }

  public void add(NodeWeight o) {
    if (o instanceof FloatWeight){
      FloatWeight other = (FloatWeight) o;
      //    if (term2TfIDf.containsKey(term)) {
      //      PairOfFloats pair = term2TfIDf.get(term);
      //      pair.set(pair.getLeftElement() + tf, pair.getRightElement() + df);
      //      term2TfIDf.put(term, pair);
      //    } else {
      //      term2TfIDf.put(term, new PairOfFloats(tf, df));
      //    }
      this.bm25 += other.bm25;
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

  public FloatWeight multiply(float weight) {
//    throw new RuntimeException("Unsupported method");
        return new FloatWeight(bm25 * weight);
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
    return "bm25(" + bm25 + ")";
  }

}
