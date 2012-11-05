package edu.stanford.nlp.util;

/**
 * Represents a interval
 *
 * @author Angel Chang
 */
public class Interval<E extends Comparable<E>> extends Pair<E,E> implements HasInterval<E> {
  public static int INTERVAL_OPEN_BEGIN = 0x01;
  public static int INTERVAL_OPEN_END = 0x02;

  private int flags;                                                                                              
  public enum RelType { BEFORE, AFTER, EQUAL, BEGIN_MEET_END, END_MEET_BEGIN, CONTAIN, INSIDE, OVERLAP, UNKNOWN, NONE }

  protected final static int REL_FLAGS_SAME = 0x0001;
  protected final static int REL_FLAGS_BEFORE = 0x0002;
  protected final static int REL_FLAGS_AFTER = 0x0004;
  protected final static int REL_FLAGS_UNKNOWN = 0x0007;
  protected final static int REL_FLAGS_SS_SHIFT = 0;
  protected final static int REL_FLAGS_SE_SHIFT = 1*4;
  protected final static int REL_FLAGS_ES_SHIFT = 2*4;
  protected final static int REL_FLAGS_EE_SHIFT = 3*4;
  public final static int REL_FLAGS_SS_SAME = 0x0001;
  public final static int REL_FLAGS_SS_BEFORE = 0x0002;
  public final static int REL_FLAGS_SS_AFTER = 0x0004;
  public final static int REL_FLAGS_SS_UNKNOWN = 0x0007;
  public final static int REL_FLAGS_SE_SAME = 0x0010;
  public final static int REL_FLAGS_SE_BEFORE = 0x0020;
  public final static int REL_FLAGS_SE_AFTER = 0x0040;
  public final static int REL_FLAGS_SE_UNKNOWN = 0x0070;
  public final static int REL_FLAGS_ES_SAME = 0x0100;
  public final static int REL_FLAGS_ES_BEFORE = 0x0200;
  public final static int REL_FLAGS_ES_AFTER = 0x0400;
  public final static int REL_FLAGS_ES_UNKNOWN = 0x0700;
  public final static int REL_FLAGS_EE_SAME = 0x1000;
  public final static int REL_FLAGS_EE_BEFORE = 0x2000;
  public final static int REL_FLAGS_EE_AFTER = 0x4000;
  public final static int REL_FLAGS_EE_UNKNOWN = 0x7000;

  public final static int REL_FLAGS_INTERVAL_SAME = 0x00010000;    // SS,EE  SAME
                                                                   // Can be set with OVERLAP, INSIDE, CONTAIN
  public final static int REL_FLAGS_INTERVAL_BEFORE = 0x00020000;  // ES BEFORE => SS, SE, EE BEFORE
  public final static int REL_FLAGS_INTERVAL_AFTER = 0x00040000;   // SE AFTER => SS, ES, EE AFTER

  // flags can be set together along with SAME
  public final static int REL_FLAGS_INTERVAL_OVERLAP = 0x00100000; // SS SAME or AFTER, SE SAME or BEFORE
                                                                   // SS SAME or BEFORE, ES SAME or AFTER
  public final static int REL_FLAGS_INTERVAL_INSIDE = 0x00200000;  // SS SAME or AFTER, EE SAME or BEFORE
  public final static int REL_FLAGS_INTERVAL_CONTAIN = 0x00400000; // SS SAME or BEFORE, EE SAME or AFTER
  public final static int REL_FLAGS_INTERVAL_UNKNOWN = 0x00770000;

  public final static int REL_FLAGS_INTERVAL_ALMOST_SAME = 0x01000000;
  public final static int REL_FLAGS_INTERVAL_ALMOST_BEFORE = 0x01000000;
  public final static int REL_FLAGS_INTERVAL_ALMOST_AFTER = 0x01000000;

//  public final static int REL_FLAGS_INTERVAL_ALMOST_OVERLAP = 0x10000000;
//  public final static int REL_FLAGS_INTERVAL_ALMOST_INSIDE = 0x20000000;
//  public final static int REL_FLAGS_INTERVAL_ALMOST_CONTAIN = 0x40000000;

  public final static int REL_FLAGS_INTERVAL_FUZZY = 0x80000000;

  protected Interval(E a, E b, int flags) {
    super(a,b);
    this.flags = flags;
    int comp = a.compareTo(b);
    if (comp > 0) {
      throw new IllegalArgumentException("Invalid interval: " + a + "," + b);
    }
  }

  public static <E extends Comparable<E>> Interval<E> toInterval(E a, E b) {
    return toInterval(a,b,0);
  }

  public static <E extends Comparable<E>> Interval<E> toInterval(E a, E b, int flags) {
    int comp = a.compareTo(b);
    if (comp <= 0) {
      return new Interval(a,b, flags);
    } else {
      return null;
    }
  }

  public static <E extends Comparable<E>> Interval<E> toValidInterval(E a, E b) {
    return toValidInterval(a,b,0);
  }

  public static <E extends Comparable<E>> Interval<E> toValidInterval(E a, E b, int flags) {
    int comp = a.compareTo(b);
    if (comp <= 0) {
      return new Interval(a,b,flags);
    } else {
      return new Interval(b,a,flags);
    }
  }

  public Interval<E> getInterval() {
    return this;
  }

  public E getBegin()
  {
    return first;
  }

  public E getEnd()
  {
    return second;
  }

  protected static <E extends Comparable<E>> E max(E a, E b)
  {
    int comp = a.compareTo(b);
    return (comp > 0)? a:b;
  }

  protected static <E extends Comparable<E>> E min(E a, E b)
  {
    int comp = a.compareTo(b);
    return (comp < 0)? a:b;
  }

  public boolean contains(E p)
  {
    return (first.compareTo(p) <= 0 && second.compareTo(p) >= 0);
  }


  // Returns (smallest) interval that contains both this and other
  public Interval expand(Interval<E> other)
  {
    if (other == null) return this;
    E a = min(this.first, other.first);
    E b = max(this.second, other.second);
    return toInterval(a,b);
  }

  // Returns interval that is the intersection of this and the other
  // Returns null if intersect is null
  public Interval intersect(Interval<E> other)
  {
    if (other == null) return null;
    E a = max(this.first, other.first);
    E b = min(this.second, other.second);
    return toInterval(a,b);
  }

  // Returns true if this interval overlaps the other
  // (i.e. the intersect would not be null
  public boolean overlaps(Interval<E> other)
  {
    if (other == null) return false;
    int comp12 = this.first.compareTo(other.second());
    int comp21 = this.second.compareTo(other.first());
    if (comp12 > 0 || comp21 < 0) {
      return false;
    } else {
      if (comp12 == 0) {
        if (!this.includesBegin() || !other.includesEnd()) {
          return false;
        }
      }
      if (comp21 == 0) {
        if (!this.includesEnd() || !other.includesBegin()) {
          return false;
        }
      }
      return true;
    }
  }

  public boolean includesBegin()
  {
    return ((flags & INTERVAL_OPEN_BEGIN) == 0);
  }

  public boolean includesEnd()
  {
    return ((flags & INTERVAL_OPEN_END) == 0);
  }

/*  // Returns true if end before (start of other)
  public boolean isEndBeforeBegin(Interval<E> other)
  {
    if (other == null) return false;
    int comp21 = this.second.compareTo(other.first());
    return (comp21 < 0);
  }

  // Returns true if end before or eq (start of other)
  public boolean isEndBeforeEqBegin(Interval<E> other)
  {
    if (other == null) return false;
    int comp21 = this.second.compareTo(other.first());
    return (comp21 <= 0);
  }

  // Returns true if end before or eq (start of other)
  public boolean isEndEqBegin(Interval<E> other)
  {
    if (other == null) return false;
    int comp21 = this.second.compareTo(other.first());
    return (comp21 == 0);
  }

  // Returns true if start after (end of other)
  public boolean isBeginAfterEnd(Interval<E> other)
  {
    if (other == null) return false;
    int comp12 = this.first.compareTo(other.second());
    return (comp12 > 0);
  }

  // Returns true if start eq(end of other)
  public boolean isBeginAfterEqEnd(Interval<E> other)
  {
    if (other == null) return false;
    int comp12 = this.first.compareTo(other.second());  
    return (comp12 >= 0);
  }

  // Returns true if start eq(end of other)
  public boolean isBeginEqEnd(Interval<E> other)
  {
    if (other == null) return false;
    int comp12 = this.first.compareTo(other.second());
    return (comp12 >= 0);
  }

  // Returns true if start is the same
  public boolean isBeginSame(Interval<E> other)
  {
    if (other == null) return false;
    int comp11 = this.first.compareTo(other.first());
    return (comp11 == 0);
  }

  // Returns true if end is the same
  public boolean isEndSame(Interval<E> other)
  {
    if (other == null) return false;
    int comp22 = this.second.compareTo(other.second());
    return (comp22 == 0);
  } */

  protected int toRelFlags(int comp, int shift)
  {
    int flags = 0;
    if (comp == 0) {
      flags = REL_FLAGS_SAME;
    } else if (comp > 0) {
      flags = REL_FLAGS_AFTER;
    } else {
      flags = REL_FLAGS_BEFORE;
    }
    flags = flags << shift;
    return flags;
  }

  public int getRelationFlags(Interval<E> other)
  {
    if (other == null) return 0;
    int flags = 0;
    int comp11 = this.first.compareTo(other.first());   // 3 choices
    flags |= toRelFlags(comp11, REL_FLAGS_SS_SHIFT);
    int comp22 = this.second.compareTo(other.second());   // 3 choices
    flags |= toRelFlags(comp22, REL_FLAGS_EE_SHIFT);
    int comp12 = this.first.compareTo(other.second());   // 3 choices
    flags |= toRelFlags(comp12, REL_FLAGS_SE_SHIFT);
    int comp21 = this.second.compareTo(other.first());   // 3 choices
    flags |= toRelFlags(comp21, REL_FLAGS_ES_SHIFT);
    flags = addIntervalRelationFlags(flags, false);
    return flags;
  }

  public int addIntervalRelationFlags(int flags, boolean checkFuzzy) {
    int f11 = extractRelationSubflags(flags, REL_FLAGS_SS_SHIFT);
    int f22 = extractRelationSubflags(flags, REL_FLAGS_EE_SHIFT);
    int f12 = extractRelationSubflags(flags, REL_FLAGS_SE_SHIFT);
    int f21 = extractRelationSubflags(flags, REL_FLAGS_ES_SHIFT);
    if (checkFuzzy) {
      boolean isFuzzy = checkMultipleBitSet(f11) || checkMultipleBitSet(f12) || checkMultipleBitSet(f21) || checkMultipleBitSet(f22);
      if (isFuzzy) {
        flags |= REL_FLAGS_INTERVAL_FUZZY;
      }
    }
    if (((f11 & REL_FLAGS_SAME) != 0) && ((f22 & REL_FLAGS_SAME) != 0)) {
      // SS,EE SAME
      flags |= REL_FLAGS_INTERVAL_SAME;  // Possible
    }
    if (((f21 & REL_FLAGS_BEFORE) != 0)) {
      // ES BEFORE => SS, SE, EE BEFORE
      flags |= REL_FLAGS_INTERVAL_BEFORE;  // Possible
    }
    if (((f12 & REL_FLAGS_AFTER) != 0)) {
      // SE AFTER => SS, ES, EE AFTER
      flags |= REL_FLAGS_INTERVAL_AFTER;  // Possible
    }
    if (((f11 & (REL_FLAGS_SAME | REL_FLAGS_AFTER)) != 0) && ((f12 & (REL_FLAGS_SAME | REL_FLAGS_BEFORE)) != 0)) {
      // SS SAME or AFTER, SE SAME or BEFORE
      //     |-----|
      // |------|
      flags |= REL_FLAGS_INTERVAL_OVERLAP;  // Possible
    }
    if (((f11 & (REL_FLAGS_SAME | REL_FLAGS_BEFORE)) != 0) && ((f21 & (REL_FLAGS_SAME | REL_FLAGS_AFTER)) != 0)) {
      // SS SAME or BEFORE, ES SAME or AFTER
      // |------|
      //     |-----|
      flags |= REL_FLAGS_INTERVAL_OVERLAP;  // Possible
    }
    if (((f11 & (REL_FLAGS_SAME | REL_FLAGS_AFTER)) != 0) && ((f22 & (REL_FLAGS_SAME | REL_FLAGS_BEFORE)) != 0)) {
      // SS SAME or AFTER, EE SAME or BEFORE
      flags |= REL_FLAGS_INTERVAL_INSIDE;  // Possible
    }
    if (((f11 & (REL_FLAGS_SAME | REL_FLAGS_BEFORE)) != 0) && ((f22 & (REL_FLAGS_SAME | REL_FLAGS_AFTER)) != 0)) {
      // SS SAME or BEFORE, EE SAME or AFTER
      flags |= REL_FLAGS_INTERVAL_CONTAIN;  // Possible
    }
    return flags;
  }

  public static int extractRelationSubflags(int flags, int shift)
  {
    return (flags >> shift) & 0xf;
  }

  public static boolean checkMultipleBitSet(int flags) {
    boolean set = false;
    while (flags != 0) {
      if ((flags & 0x01) != 0) {
        if (set) { return false; }
        else { set = true; }
      }
      flags = flags >> 1;
    }
    return false;
  }

  public static boolean checkFlagSet(int flags, int flag)
  {
    return ((flags & flag) != 0);
  }

  public static boolean checkFlagExclusiveSet(int flags, int flag, int mask)
  {
    int f = flags & flag;
    if (f != 0) {
      return ((flags & mask & ~flag) != 0)? false:true;
    } else {
      return false;
    }
  }

  // Returns relation of this to other
  // EQUAL: this have same endpoints as other
  // OVERLAP:  this and other overlaps
  // BEFORE: this ends before other starts
  // AFTER: this starts after other ends
  // BEGIN_MEET_END: this begin is the same as the others end
  // END_MEET_BEGIN: this end is the same as the others begin
  // CONTAIN: this contains the other
  // INSIDE: this is inside the other

  // TODO: Handle open/closed intervals?
  public RelType getRelation(Interval<E> other) {
    if (other == null) return RelType.NONE;
    int comp11 = this.first.compareTo(other.first());   // 3 choices
    int comp22 = this.second.compareTo(other.second());   // 3 choices

    if (comp11 == 0) {
      if (comp22 == 0) {
        // |---|  this
        // |---|   other
        return RelType.EQUAL;
      } if (comp22 < 0) {
        // SAME START - this finishes before other
        // |---|  this
        // |------|   other
        return RelType.INSIDE;
      } else {
        // SAME START - this finishes after other
        // |------|  this
        // |---|   other
        return RelType.CONTAIN;
      }
    } else if (comp22 == 0) {
      if (comp11 < 0) {
        // SAME FINISH - this start before other
        // |------|  this
        //    |---|   other
        return RelType.CONTAIN;
      } else /*if (comp11 > 0) */ {
        // SAME FINISH - this starts after other
        //    |---|  this
        // |------|   other
        return RelType.INSIDE;
      }
    } else if (comp11 > 0 && comp22 < 0) {
      //    |---|  this
      // |---------|   other
      return RelType.INSIDE;
    } else if (comp11 < 0 && comp22 > 0) {
      // |---------|  this
      //    |---|   other
      return RelType.CONTAIN;
    } else {
      int comp12 = this.first.compareTo(other.second());
      int comp21 = this.second.compareTo(other.first());
      if (comp12 > 0) {
        //           |---|  this
        // |---|   other
        return RelType.AFTER;
      } else if (comp21 < 0) {
        // |---|  this
        //        |---|   other
        return RelType.BEFORE;
      } else if (comp12 == 0) {
        //     |---|  this
        // |---|   other
        return RelType.BEGIN_MEET_END;
      } else if (comp21 == 0) {
        // |---|  this
        //     |---|   other
        return RelType.END_MEET_BEGIN;
      } else {
        return RelType.OVERLAP;
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    Interval interval = (Interval) o;

    if (flags != interval.flags) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + flags;
    return result;
  }
}