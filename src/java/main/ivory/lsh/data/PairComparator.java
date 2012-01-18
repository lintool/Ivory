package ivory.lsh.data;

import java.util.Comparator;

import edu.umd.cloud9.io.pair.PairOfFloatInt;

public class PairComparator implements Comparator<PairOfFloatInt> {

  public int compare(PairOfFloatInt p1, PairOfFloatInt p2) {
    if (p1.getLeftElement() < p2.getLeftElement()) {
      return -1;
    } else if (p1.getLeftElement() > p2.getLeftElement()) {
      return 1;
    } else {
      return 0;
    }
  }

}