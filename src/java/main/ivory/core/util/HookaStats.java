package ivory.core.util;

public class HookaStats {
  private int cntLongTail, cntShortTail, sumShortTail, numTopTrans;
  private float sumCumProbs, cumProbThreshold;
  
  
  public HookaStats(int n, float c) {
    cntLongTail = 0;
    cntShortTail = 0;
    sumShortTail = 0;
    sumCumProbs = 0f;
    numTopTrans = n;
    cumProbThreshold = c;
  }

  public int getCntLongTail() {
    return cntLongTail;
  }

  public void incCntLongTail(int n) {
    this.cntLongTail += n;
  }

  public int getCntShortTail() {
    return cntShortTail;
  }

  public void incCntShortTail(int n) {
    this.cntShortTail += n;
  }

  public int getSumShortTail() {
    return sumShortTail;
  }

  public void incSumShortTail(int n) {
    this.sumShortTail += n;
  }

  public float getSumCumProbs() {
    return sumCumProbs;
  }

  public void incSumCumProbs(float n) {
    this.sumCumProbs += n;
  }
  
  public void update (int numEntries, float sumOfProbs) {
    if (numEntries < numTopTrans) {
      incCntShortTail(1);
      incSumShortTail(numEntries);
    }else {
      incCntLongTail(1);
      incSumCumProbs(sumOfProbs);
    }
  }
  
  public String toString () {
    String s = "";
    s += "# source terms with > "+cumProbThreshold+" probability covered: "+cntShortTail+" and average translations per term: "+(sumShortTail/(cntShortTail+0.0f))+"\n";
    float averageCoverageLongTail = sumCumProbs/cntLongTail;
    s += "# source terms with <= "+cumProbThreshold+" probability covered: "+cntLongTail+" (each have "+ numTopTrans +" translations). Average coverage is: "+averageCoverageLongTail+"\n";
    float averageCoverageOverall = (cntShortTail*cumProbThreshold+cntLongTail*averageCoverageLongTail)/(cntShortTail+cntLongTail);
    s += "Size (total number of dictionary entries) = "+(sumShortTail + cntLongTail*numTopTrans)+" TTable average coverage >= "+averageCoverageOverall;
    
    return s;
  }
  
  
}