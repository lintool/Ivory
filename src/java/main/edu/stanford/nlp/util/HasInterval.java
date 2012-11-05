package edu.stanford.nlp.util;

import java.util.Comparator;

/**
 * HasInterval interface
 *
 * @author Angel Chang
 */
public interface HasInterval<E extends Comparable<E>> {
  public Interval<E> getInterval();
  
  public final static Comparator<HasInterval> OFFSET_COMPARATOR =
    new Comparator<HasInterval>() {
      public int compare(HasInterval e1, HasInterval e2) {
        return (e1.getInterval().compareTo(e2.getInterval()));
      }
    };

}
