package ivory.lsh.data;

import java.util.Comparator;

import tl.lin.data.pair.PairOfFloatInt;

public class PairComparator implements Comparator<PairOfFloatInt>{

  public int compare(PairOfFloatInt p1, PairOfFloatInt p2) {
    if(p1.getLeftElement()<p2.getLeftElement()){
      return -1;
    }else if(p1.getLeftElement()>p2.getLeftElement()){
      return 1;
    }else{
      return 0;
    }
  }

}