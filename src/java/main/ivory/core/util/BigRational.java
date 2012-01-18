/* BigRational.java -- dynamically sized big rational numbers.
 **
 ** Copyright (C) 2002-2010 Eric Laroche.  All rights reserved.
 **
 ** @author Eric Laroche <laroche@lrdev.com>
 ** @version @(#)$Id: BigRational.java,v 1.3 2010/03/24 20:11:34 laroche Exp $
 **
 ** This program is free software;
 ** you can redistribute it and/or modify it.
 **
 ** This program is distributed in the hope that it will be useful,
 ** but WITHOUT ANY WARRANTY; without even the implied warranty of
 ** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 **
 */

package ivory.core.util;

import java.math.BigInteger;

/**
 * <STRONG> BigRational implements dynamically sized arbitrary precision immutable rational numbers.
 * </STRONG>
 * 
 * <P>
 * Dynamically sized means that (provided enough memory) the BigRational numbers can't overflow (nor
 * underflow); this characteristic is different from Java's data types int and long, but common with
 * BigInteger (which however implements only integer numbers, i.e. no fractions) and BigDecimal
 * (which however only implements precisely rational numbers with denominators that are products of
 * 2 and 5 [and implied 10]). Arbitrary precision means that there is no loss of precision with
 * common arithmetic operations such as addition, subtraction, multiplication, and division
 * (BigDecimal loses precision with division, if factors other than 2 or 5 are involved). [Doubles
 * and floats can overflow and underflow, have a limited precision, and only implement precisely
 * rationals with a denominator that is a power of 2.]
 * 
 * <P>
 * BigRational provides most operations needed in rational number space calculations, including
 * addition, subtraction, multiplication, division, integer power, remainder/modulus, comparison,
 * and different roundings.
 * 
 * <P>
 * BigRational provides conversion methods to and from the native types long, int, short, and byte
 * (including support for unsigned values), double (binary64), float (binary32), quad (binary128,
 * quadruple), half (binary16), and BigInteger. BigRational can parse and print many string
 * representations: rational, dot notations, with exponent, even mixtures thereof, and supports
 * different radixes/bases (2 to typically 36 [limited by BigInteger parsing implementation]).
 * 
 * <P>
 * BigRational uses java.math.BigInteger (JDK 1.1 and later).
 * 
 * <P>
 * Binary operations (e.g. add, multiply) calculate their results from a BigRational object ('this')
 * and one [BigRational or long] argument, returning a new immutable BigRational object. Both the
 * original object and the argument are left unchanged (hence immutable). Unary operations (e.g.
 * negate, invert) calculate their result from the BigRational object ('this'), returning a new
 * immutable BigRational object. The original object is left unchanged.
 * 
 * <P>
 * Most operations are precise (i.e. without loss of precision); exceptions are the conversion
 * methods to limited precision types (doubleValue, floatValue), rounding (round), truncation
 * (bigIntegerValue, floor, ceiling, truncate), as well as obviously the printing methods that
 * include a precision parameter (toStringDot, toStringDotRelative, toStringExponent).
 * 
 * <P>
 * BigRational doesn't provide a notion of "infinity" ([+-]Infinity) and "not a number" (NaN); IEEE
 * 754 floating point Infinity and NaN are rejected (throwing a NumberFormatException). Operations
 * such as 0/0 result in an ArithmeticException.
 * 
 * <P>
 * Some commonly used short function names (abs, ceil, div, inv, max, min, mod, mul, neg, pow, rem,
 * sign, sub, trunc) are additionally defined as aliases to to the full function names (absolute,
 * ceiling, divide, invert, maximum, minimum, modulus, multiply, negate, power, remainder, signum,
 * subtract, truncate). [This makes the interface somewhat fatter.]
 * 
 * <P>
 * BigRational internally uses private proxy functions for BigInteger functionality, including
 * scanning and multiplying, to enhance speed and to realize fast checks for common values (1, 0,
 * etc.).
 * 
 * <P>
 * Constructor samples: normal rational form, abbreviated form, fixed point form, abbreviated fixed
 * point form, [exponential] floating point form, different radixes/bases different from 10,
 * doubles/floats: <SMALL>
 * 
 * <PRE>
 * 	BigRational(&quot;-21/35&quot;): rational -3/5
 * 	BigRational(&quot;/3&quot;): rational 1/3
 * 	BigRational(&quot;3.4&quot;): rational 17/5
 * 	BigRational(&quot;.7&quot;): 0.7, rational 7/10
 * 	BigRational(&quot;-65.4E-3&quot;): -327/5000
 * 	BigRational(&quot;f/37&quot;, 0x10): 3/11
 * 	BigRational(&quot;f.37&quot;, 0x10): 3895/256
 * 	BigRational(&quot;-dcba.efgh&quot;, 23): -46112938320/279841
 * 	BigRational(&quot;1011101011010110&quot;, 2): 47830
 * 	BigRational(StrictMath.E): 6121026514868073/2251799813685248
 * 	BigRational((float)StrictMath.E): 2850325/1048576
 * </PRE>
 * 
 * </SMALL>
 * 
 * <P>
 * Also accepted are denormalized representations such as: <SMALL>
 * 
 * <PRE>
 * 	BigRational(&quot;2.5/-3.5&quot;): -5/7
 * 	BigRational(&quot;12.34E-1/-56.78E-1&quot;): -617/2839
 * </PRE>
 * 
 * </SMALL>
 * 
 * <P>
 * Printing: rational form, fixed point (dot) forms with different absolute precisions (including
 * negative precision), with relative precisions, exponential form, different radix: <SMALL>
 * 
 * <PRE>
 * 	BigRational(&quot;1234.5678&quot;): &quot;6172839/5000&quot;
 * 	BigRational(&quot;1234.5678&quot;).toStringDot(6): &quot;1234.567800&quot;
 * 	BigRational(&quot;1234.5678&quot;).toStringDot(2): &quot;1234.57&quot;
 * 	BigRational(&quot;1234.5678&quot;).toStringDot(-2): &quot;1200&quot;
 * 	BigRational(&quot;1234.5678&quot;).toStringDotRelative(6): &quot;1234.57&quot;
 * 	BigRational(&quot;0.00012345678&quot;).toStringDotRelative(3): &quot;0.000123&quot;
 * 	BigRational(&quot;1234.5678&quot;).toStringExponent(2): &quot;1.2E3&quot;
 * 	BigRational(&quot;1011101011010110&quot;, 2).toString(0x10): &quot;bad6&quot;
 * </PRE>
 * 
 * </SMALL>
 * 
 * <P>
 * Usage: BigRational operations can be conveniently chained (sample from BigRational internal
 * conversion from IEEE 754 bits): <SMALL>
 * 
 * <PRE>
 * BigRational.valueOf(2).power(exponent)
 *     .multiply(fraction.add(BigRational.valueOfUnsigned(mantissa))).multiply(sign);
 * </PRE>
 * 
 * </SMALL>
 * 
 * <P>
 * The BigRational source and documentation can typically be found at the author's (Eric Laroche)
 * site, at <A HREF="http://www.lrdev.com/lr/java/BigRational.java"
 * >http://www.lrdev.com/lr/java/BigRational.java</A> (source) and <A
 * HREF="http://www.lrdev.com/lr/java/BigRational.html"
 * >http://www.lrdev.com/lr/java/BigRational.html</A> (documentation).
 * 
 * @author Eric Laroche &lt;laroche@lrdev.com&gt;
 * @version @(#)$Id: BigRational.java,v 1.3 2010/03/24 20:11:34 laroche Exp $
 * 
 */
// @Immutable
public class BigRational extends Number implements
// note: clone() not so much needed, since the BigRational objects are immutable
    Cloneable,
    // note: Comparable<Object> implemented (instead of Comparable<BigRational>)
    // for backward compatibility
    Comparable<Object>
// implements Comparable<BigRational>
{

  /**
   * Numerator. Numerator may be negative. Numerator may be zero, in which case m_q must be one.
   * [Conditions are put in place by normalize().]
   */
  private BigInteger m_n;

  /**
   * Denominator (quotient). Denominator is never negative and never zero. [Conditions are put in
   * place by normalize().]
   */
  private BigInteger m_q;

  /**
   * Default radix, used in string printing and scanning, 10.
   * <P>
   * Default radix is decimal [of course].
   */
  public final static int DEFAULT_RADIX = 10;

  // note: following constants can't be constructed using bigIntegerValueOf().
  // that one _uses_ the constants (avoid circular dependencies).

  /**
   * Constant internally used, for convenience and speed. Used as zero numerator. Used for fast
   * checks.
   */
  private final static BigInteger BIG_INTEGER_ZERO = BigInteger.valueOf(0);

  /**
   * Constant internally used, for convenience and speed. Used as neutral denominator. Used for fast
   * checks.
   */
  private final static BigInteger BIG_INTEGER_ONE = BigInteger.valueOf(1);

  /**
   * Constant internally used, for convenience and speed. Used for fast checks.
   */
  private final static BigInteger BIG_INTEGER_MINUS_ONE = BigInteger.valueOf(-1);

  /**
   * Constant internally used, for convenience and speed. Used in rounding zero numerator. _Not_
   * used for checks.
   */
  private final static BigInteger BIG_INTEGER_TWO = BigInteger.valueOf(2);

  /**
   * Constant internally used, for convenience and speed. _Not_ used for checks.
   */
  private final static BigInteger BIG_INTEGER_MINUS_TWO = BigInteger.valueOf(-2);

  /**
   * Constant internally used, for convenience and speed. Corresponds to DEFAULT_RADIX, used in
   * reading, scaling and printing. _Not_ used for checks.
   */
  private final static BigInteger BIG_INTEGER_TEN = BigInteger.valueOf(10);

  /**
   * Constant internally used, for convenience and speed. Used in reading, scaling and printing.
   * _Not_ used for checks.
   */
  private final static BigInteger BIG_INTEGER_SIXTEEN = BigInteger.valueOf(16);

  // some more constants

  /**
   * The constant two to the power of 64 (18446744073709551616). Used is slicing larger [than double
   * size] IEEE 754 values.
   */
  private final static BigInteger BIG_INTEGER_TWO_POWER_64 = BigInteger.valueOf(2).pow(64);

  /**
   * Construct a BigRational from numerator and denominator.
   * <P>
   * Both n and q may be negative. n/q may be denormalized (i.e. have common factors, or q being
   * negative).
   */
  public BigRational(BigInteger n, BigInteger q) {
    // note: check for q==null done later
    if (q != null && bigIntegerIsZero(q)) {
      throw new NumberFormatException("quotient zero");
    }

    normalizeFrom(n, q);
  }

  /**
   * Construct a BigRational from a big number integer; denominator is 1.
   */
  public BigRational(BigInteger n) {
    this(n, BIG_INTEGER_ONE);
  }

  /**
   * Construct a BigRational from long fix number integers representing numerator and denominator.
   */
  public BigRational(long n, long q) {
    this(bigIntegerValueOf(n), bigIntegerValueOf(q));
  }

  /**
   * Construct a BigRational from a long fix number integer.
   */
  public BigRational(long n) {
    this(bigIntegerValueOf(n), BIG_INTEGER_ONE);
  }

  // note: byte/short/int implicitly upgraded to long,
  // so we don't implement BigRational(int,int) et al.

  /**
   * Clone a BigRational.
   * <P>
   * [As BigRationals are immutable, this copy-constructor is not that useful.]
   */
  public BigRational(BigRational that) {
    normalizeFrom(that);
  }

  /**
   * Construct a BigRational from a string representation.
   * <P>
   * The supported string formats are "[+-]n", "[+-]n/[+-]q", "[+-]i.f", "[+-]i", "[+-]iE[+-]e",
   * "[+-]i.fE[+-]e" (latter two only with radix&lt;=10, due to possible ambiguities); n and q can
   * be any of the latter (i.e. mixed representations such as "-1.2E-3/-4.5E-6" are supported).
   * <P>
   * Samples: "-21/35", "3.4", "-65.4E-3", "f/37" (base 16), "1011101011010110" (base 2).
   * <P>
   * [Exponential representation wasn't supported in an earlier version.]
   */
  public BigRational(String s, int radix) {
    if (s == null) {
      throw new NumberFormatException("null");
    }

    // '/': least precedence, and left-to-right associative
    // (therefore lastIndexOf and not indexOf: last slash has least precedence)
    final int slash = s.lastIndexOf('/');
    if (slash != -1) {

      // "[+-]n/[+-]q"
      String sn = s.substring(0, slash);
      String sq = s.substring(slash + 1);

      // suppress recursion: make stack-overflow attacks infeasible
      if (sn.indexOf('/') != -1) {
        throw new NumberFormatException("can't nest '/'");
      }

      // handle "/x" as "1/x"
      // [note: "1" and "-1" are treated specially and optimized
      // in bigIntegerValueOf(String,int).]
      if (sn.equals("") || sn.equals("+")) {
        sn = "1";
      } else if (sn.equals("-")) {
        sn = "-1";
      }

      // handle "x/" as "x"
      // [note: "1" and "-1" are treated special and optimized
      // in bigIntegerValueOf(String,int).]
      if (sq.equals("") || sq.equals("+")) {
        sq = "1";
      } else if (sq.equals("-")) {
        sq = "-1";
      }

      // [recursion]
      // [divide()'s outcome is already normalized,
      // so there would be no need to normalize [again]]
      normalizeFrom((new BigRational(sn, radix)).divide(new BigRational(sq, radix)));
      return;
    }

    checkRadix(radix);

    // catch Java's string representations of doubles/floats unsupported by us
    checkNaNAndInfinity(s, radix);

    // [if radix<=10:] 'E': next higher precedence, not associative
    // or right-to-left associative
    int exp = -1;
    // note: a distinction of exponent-'E' from large-base-digit-'e'
    // would be unintuitive, since case doesn't matter with both uses
    if (radix <= 10) {
      // handle both [upper/lower] cases
      final int exp1 = s.indexOf('E');
      final int exp2 = s.indexOf('e');
      exp = (exp1 == -1 || (exp2 != -1 && exp2 < exp1) ? exp2 : exp1);
    }
    if (exp != -1) {
      String sm = s.substring(0, exp);
      String se = s.substring(exp + 1);

      // suppress recursion: make stack-overflow attacks infeasible
      if (se.indexOf('E') != -1 || se.indexOf('e') != -1) {
        throw new NumberFormatException("can't nest 'E'");
      }

      // skip '+'
      if (se.length() > 0 && se.charAt(0) == '+') {
        se = se.substring(1);
      }

      // handle '-'
      boolean negtexp = false;
      if (se.length() > 0 && se.charAt(0) == '-') {
        negtexp = true;
        se = se.substring(1);
      }

      // handle "xE", "xE+", "xE-", as "xE0" aka "x"
      if (se.equals("")) {
        se = "0";
      }

      // [recursion]
      BigRational exponent = new BigRational(se, radix);

      final int iexponent;
      // transform possible [overflow/fraction] exception
      try {
        iexponent = exponent.intValueExact();
      } catch (ArithmeticException e) {
        final NumberFormatException e2 = new NumberFormatException(e.getMessage());
        // make sure this operation doesn't shadow the exception to be thrown
        try {
          e2.initCause(e);
        } catch (Throwable e3) {
          // ignored
        }
        throw e2;
      }
      exponent = valueOf(radix).power(iexponent);
      if (negtexp) {
        exponent = exponent.invert();
      }

      // handle "Ex", "+Ex", "-Ex", as "1Ex"
      if (sm.equals("") || sm.equals("+")) {
        sm = "1";
      } else if (sm.equals("-")) {
        sm = "-1";
      }

      // [multiply()'s outcome is already normalized,
      // so there would be no need to normalize [again]]
      normalizeFrom((new BigRational(sm, radix)).multiply(exponent));
      return;
    }

    // '.': next higher precedence, not associative
    // (can't have more than one dot)
    String si, sf;
    final int dot = s.indexOf('.');
    if (dot != -1) {
      // "[+-]i.f"
      si = s.substring(0, dot);
      sf = s.substring(dot + 1);
    } else {
      // "[+-]i". [not just delegating to BigInteger.]
      si = s;
      sf = "";
    }

    // check for multiple signs or embedded signs
    checkNumberFormat(si);

    // skip '+'
    // skip '+'. [BigInteger [likely] doesn't handle these.]
    if (si.length() > 0 && si.charAt(0) == '+') {
      si = si.substring(1);
    }

    // handle '-'
    boolean negt = false;
    if (si.length() > 0 && si.charAt(0) == '-') {
      negt = true;
      si = si.substring(1);
    }

    // handle ".x" as "0.x" ("." as "0.0")
    // handle "" as "0"
    // note: "0" is treated specially and optimized
    // in bigIntegerValueOf(String,int).
    if (si.equals("")) {
      si = "0";
    }

    BigInteger n = bigIntegerValueOf(si, radix);
    BigInteger q;

    // includes the cases "x." and "."
    if (!sf.equals("")) {
      // check for signs
      checkFractionFormat(sf);

      final BigInteger f = bigIntegerValueOf(sf, radix);
      final int scale = sf.length();
      q = bigIntegerPower(bigIntegerValueOf(radix), scale);
      n = bigIntegerMultiply(n, q).add(f);
    } else {
      q = BIG_INTEGER_ONE;
    }

    if (negt) {
      n = n.negate();
    }

    normalizeFrom(n, q);
  }

  // javadoc: in-sync with the radix parameter version

  /**
   * Construct a BigRational from a string representation, with default radix (10).
   * <P>
   * The supported string formats are "[+-]n", "[+-]n/[+-]q", "[+-]i.f", "[+-]i", "[+-]iE[+-]e",
   * "[+-]i.fE[+-]e" (latter two only with radix&lt;=10, due to possible ambiguities); n and q can
   * be any of the latter (i.e. mixed representations such as "-1.2E-3/-4.5E-6" are supported).
   * <P>
   * Samples: "-21/35", "3.4", "-65.4E-3", "f/37" (base 16), "1011101011010110" (base 2).
   * <P>
   * [Exponential representation wasn't supported in an earlier version.]
   */
  public BigRational(String s) {
    this(s, DEFAULT_RADIX);
  }

  /**
   * Construct a BigRational from an unscaled value and a scale value.
   */
  public BigRational(BigInteger unscaledValue, int scale, int radix) {
    if (unscaledValue == null) {
      throw new NumberFormatException("null");
    }

    final boolean negt = (scale < 0);
    if (negt) {
      scale = -scale;
    }

    checkRadix(radix);
    final BigInteger scaleValue = bigIntegerPower(bigIntegerValueOf(radix), scale);

    normalizeFrom((negt ? bigIntegerMultiply(unscaledValue, scaleValue) : unscaledValue),
        (negt ? BIG_INTEGER_ONE : scaleValue));
  }

  /**
   * Construct a BigRational from an unscaled value and a scale value, default radix (10).
   */
  public BigRational(BigInteger unscaledValue, int scale) {
    this(unscaledValue, scale, DEFAULT_RADIX);
  }

  /**
   * Construct a BigRational from an unscaled fix number value and a scale value.
   */
  public BigRational(long unscaledValue, int scale, int radix) {
    this(bigIntegerValueOf(unscaledValue), scale, radix);
  }

  /**
   * Construct a BigRational from a [IEEE 754] double [size/precision] floating point number.
   */
  public BigRational(double x) {
    // // [rounding step, possible loss of precision step]
    // this (String.valueOf(x));
    normalizeFrom(valueOfDoubleBits(Double.doubleToLongBits(x)));
  }

  /**
   * Construct a BigRational from a [IEEE 754] single [size/precision] floating point number.
   */
  public BigRational(float x) {
    // this ((double)x);
    normalizeFrom(valueOfFloatBits(Float.floatToIntBits(x)));
  }

  // can't have public BigRational(long unscaledValue, int scale)
  // as alias for BigRational(unscaledValue, scale, DEFAULT_RADIX);
  // it's too ambiguous with public BigRational(long n, long q).

  /**
   * Normalize BigRational. Denominator will be positive, numerator and denominator will have no
   * common divisor. BigIntegers -1, 0, 1 will be set to constants for later comparison speed.
   */
  private void normalize() {
    // note: don't call anything that depends on a normalized this.
    // i.e.: don't call most (or all) of the BigRational methods.

    if (m_n == null || m_q == null) {
      throw new NumberFormatException("null");
    }

    // [these are typically cheap.]
    int ns = m_n.signum();
    int qs = m_q.signum();

    // note: we don't throw on qs==0. that'll be done elsewhere.
    // if (qs == 0) {
    // throw new NumberFormatException("quotient zero");
    // }

    if (ns == 0 && qs == 0) {
      // [typically not reached, due to earlier tests.]
      // [both for speed]
      m_n = BIG_INTEGER_ZERO;
      m_q = BIG_INTEGER_ZERO;
      return;
    }

    if (ns == 0) {
      m_q = BIG_INTEGER_ONE;
      // [for speed]
      m_n = BIG_INTEGER_ZERO;
      return;
    }

    if (qs == 0) {
      // [typically not reached, due to earlier tests.]
      m_n = BIG_INTEGER_ONE;
      // [for speed]
      m_q = BIG_INTEGER_ZERO;
      return;
    }

    // optimization
    // check the frequent case of q==1, for speed.
    // note: this only covers the normalized-for-speed 1-case.
    if (m_q == BIG_INTEGER_ONE) {
      // [for [later] speed]
      m_n = bigIntegerValueOf(m_n);
      return;
    }

    // optimization
    // check the symmetric case too, for speed.
    // note: this only covers the normalized-for-speed 1-case.
    if ((m_n == BIG_INTEGER_ONE || m_n == BIG_INTEGER_MINUS_ONE) && qs > 0) {
      // [for [later] speed]
      m_q = bigIntegerValueOf(m_q);
      return;
    }

    // setup torn apart for speed
    BigInteger na = m_n;
    BigInteger qa = m_q;

    if (qs < 0) {
      m_n = m_n.negate();
      m_q = m_q.negate();
      ns = -ns;
      qs = -qs;

      qa = m_q;
      if (ns > 0) {
        na = m_n;
      }

    } else {
      if (ns < 0) {
        na = m_n.negate();
      }
    }

    final BigInteger g = na.gcd(qa);

    // test: optimization (body: not)
    if (!bigIntegerIsOne(g)) {
      m_n = m_n.divide(g);
      m_q = m_q.divide(g);
    }

    // for [later] speed, and normalization generally
    m_n = bigIntegerValueOf(m_n);
    m_q = bigIntegerValueOf(m_q);
  }

  /**
   * Normalize BigRational. [Convenience method to normalize(void).]
   */
  private void normalizeFrom(BigInteger n, BigInteger q) {
    m_n = n;
    m_q = q;

    normalize();
  }

  /**
   * Normalize BigRational. [Convenience method to normalize(void).]
   */
  private void normalizeFrom(BigRational that) {
    if (that == null) {
      throw new NumberFormatException("null");
    }

    normalizeFrom(that.m_n, that.m_q);
  }

  /**
   * Check constraints on radixes. Radix may not be negative or less than two.
   */
  private static void checkRadix(int radix) {
    if (radix < 0) {
      throw new NumberFormatException("radix negative");
    }

    if (radix < 2) {
      throw new NumberFormatException("radix too small");
    }

    // note: we don't check for "radix too large";
    // that's left to BigInteger.toString(radix)
    // [i.e.: we don't really mind whether the underlying
    // system supports base36, or base62, or even more]
  }

  /**
   * Check some of the integer format constraints.
   */
  private static void checkNumberFormat(String s) {
    // "x", "-x", "+x", "", "-", "+"

    if (s == null) {
      throw new NumberFormatException("null");
    }

    // note: 'embedded sign' catches both-signs cases too.

    final int p = s.indexOf('+');
    final int m = s.indexOf('-');

    final int pp = (p == -1 ? -1 : s.indexOf('+', p + 1));
    final int mm = (m == -1 ? -1 : s.indexOf('-', m + 1));

    if ((p != -1 && p != 0) || (m != -1 && m != 0) || pp != -1 || mm != -1) {
      // embedded sign. this covers the both-signs case.
      throw new NumberFormatException("embedded sign");
    }
  }

  /**
   * Check number format for fraction part.
   */
  private static void checkFractionFormat(String s) {
    if (s == null) {
      throw new NumberFormatException("null");
    }

    if (s.indexOf('+') != -1 || s.indexOf('-') != -1) {
      throw new NumberFormatException("sign in fraction");
    }
  }

  /**
   * Check number input for Java's string representations of doubles/floats that are unsupported:
   * "NaN" and "Infinity" (with or without sign).
   */
  private static void checkNaNAndInfinity(String s, int radix) {
    // the strings may actually be valid given a large enough radix
    // (e.g. base 36), so limit the radix/check
    if (radix > 16) {
      return;
    }

    // [null and empty string check]
    final int length = (s == null ? 0 : s.length());
    if (length < 1) {
      return;
    }

    // optimization (String.equals and even more String.equalsIgnoreCase
    // are quite expensive, charAt and switch aren't)
    // test for last character in strings below, both cases
    switch (s.charAt(length - 1)) {
    case 'N':
    case 'n':
    case 'y':
    case 'Y':
      break;
    default:
      return;
    }

    if (s.equalsIgnoreCase("NaN") || s.equalsIgnoreCase("Infinity")
        || s.equalsIgnoreCase("+Infinity") || s.equalsIgnoreCase("-Infinity")) {
      throw new NumberFormatException(s);
    }
  }

  /**
   * Check constraints on radixes. [Convenience method to checkRadix(radix).]
   */
  private static void checkRadixArgument(int radix) {
    try {
      checkRadix(radix);
    } catch (Exception e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  /**
   * Proxy to BigInteger.valueOf(). Speeds up comparisons by using constants.
   */
  private static BigInteger bigIntegerValueOf(long n) {
    // return the internal constants used for checks if possible.

    // optimization
    // check whether it's outside int range.
    // actually check a much narrower range, fitting the switch below.
    if (n >= -16 && n <= 16) {
      // note: test above needed to make the cast below safe
      // jump table, for speed
      switch ((int) n) {
      case 0:
        return BIG_INTEGER_ZERO;
      case 1:
        return BIG_INTEGER_ONE;
      case -1:
        return BIG_INTEGER_MINUS_ONE;
      case 2:
        return BIG_INTEGER_TWO;
      case -2:
        return BIG_INTEGER_MINUS_TWO;
      case 10:
        return BIG_INTEGER_TEN;
      case 16:
        return BIG_INTEGER_SIXTEEN;
      }
    }

    return BigInteger.valueOf(n);
  }

  /**
   * Convert BigInteger to its constant if possible. Speeds up later comparisons by using constants.
   */
  private static BigInteger bigIntegerValueOf(BigInteger n) {
    // note: these tests are quite expensive,
    // so they should be minimized to a reasonable amount.

    // priority in the tests: 1, 0, -1;

    // two phase testing.
    // cheap tests first.

    // optimization
    if (n == BIG_INTEGER_ONE) {
      return n;
    }

    // optimization
    if (n == BIG_INTEGER_ZERO) {
      // [typically not reached, since zero is handled specially.]
      return n;
    }

    // optimization
    if (n == BIG_INTEGER_MINUS_ONE) {
      return n;
    }

    // more expensive tests later.

    // optimization
    if (n.equals(BIG_INTEGER_ONE)) {
      return BIG_INTEGER_ONE;
    }

    // optimization
    if (n.equals(BIG_INTEGER_ZERO)) {
      // [typically not reached from normalize().]
      return BIG_INTEGER_ZERO;
    }

    // optimization
    if (n.equals(BIG_INTEGER_MINUS_ONE)) {
      return BIG_INTEGER_MINUS_ONE;
    }

    // note: BIG_INTEGER_TWO et al. _not_ used for checks
    // and therefore not replaced by constants_here_.
    // this speeds up tests.

    // not a known constant
    return n;
  }

  /**
   * Proxy to (new BigInteger()). Speeds up comparisons by using constants.
   */
  private static BigInteger bigIntegerValueOf(String s, int radix) {
    // note: mind the radix.
    // however, 0/1/-1 are not a problem.

    // _often_ used strings (e.g. 0 for empty fraction and
    // 1 for empty denominator), for speed.

    // optimization
    if (s.equals("1")) {
      return BIG_INTEGER_ONE;
    }

    // optimization
    if (s.equals("0")) {
      return BIG_INTEGER_ZERO;
    }

    // optimization
    if (s.equals("-1")) {
      // typically not reached, due to [private] usage pattern,
      // i.e. the sign is cut before
      return BIG_INTEGER_MINUS_ONE;
    }

    // note: BIG_INTEGER_TWO et al. _not_ used for checks
    // and therefore even less valuable.
    // there's a tradeoff between speeds of these tests
    // and being consistent in using all constants
    // (at least with the common radixes).

    // optimization
    if (radix > 2) {
      if (s.equals("2")) {
        return BIG_INTEGER_TWO;
      }

      if (s.equals("-2")) {
        // typically not reached, due to [private] usage pattern,
        // i.e. the sign is cut before
        return BIG_INTEGER_MINUS_TWO;
      }
    }

    // optimization
    if (s.equals("10")) {
      switch (radix) {
      case 2:
        return BIG_INTEGER_TWO;
      case 10:
        return BIG_INTEGER_TEN;
      case 16:
        return BIG_INTEGER_SIXTEEN;
      }
    }

    // optimization
    if (radix == 10 && s.equals("16")) {
      return BIG_INTEGER_SIXTEEN;
    }

    // note: not directly finding the other [radix'] representations
    // of 10 and 16 in the code above

    // use the constants if possible
    return bigIntegerValueOf(new BigInteger(s, radix));
  }

  /**
   * Proxy to BigInteger.equals(). For speed.
   */
  private static boolean bigIntegerEquals(BigInteger n, BigInteger m) {
    // optimization
    // first test is for speed.
    if (n == m) {
      return true;
    }

    return n.equals(m);
  }

  /**
   * Zero (0) value predicate. [For convenience and speed.]
   */
  private static boolean bigIntegerIsZero(BigInteger n) {
    // optimization
    // first test is for speed.
    if (n == BIG_INTEGER_ZERO) {
      return true;
    }

    // well, this is also optimized for speed a bit.
    return (n.signum() == 0);
  }

  /**
   * One (1) value predicate. [For convenience and speed.]
   */
  private static boolean bigIntegerIsOne(BigInteger n) {
    // optimization
    // first test is for speed.
    if (n == BIG_INTEGER_ONE) {
      return true;
    }

    return bigIntegerEquals(n, BIG_INTEGER_ONE);
  }

  /**
   * Minus-one (-1) value predicate. [For convenience and speed.]
   */
  private static boolean bigIntegerIsMinusOne(BigInteger n) {
    // optimization
    // first test is for speed.
    if (n == BIG_INTEGER_MINUS_ONE) {
      return true;
    }

    return bigIntegerEquals(n, BIG_INTEGER_MINUS_ONE);
  }

  /*
   * / ** Positive value predicate. / private static boolean bigIntegerIsPositive(BigInteger n) {
   * return (n.signum() > 0); }
   */

  /**
   * Negative value predicate.
   */
  private static boolean bigIntegerIsNegative(BigInteger n) {
    return (n.signum() < 0);
  }

  /**
   * Proxy to BigInteger.multiply(). For speed. The more common cases of integers (q == 1) are
   * optimized.
   */
  private static BigInteger bigIntegerMultiply(BigInteger n, BigInteger m) {
    // optimization: one or both operands are zero.
    if (bigIntegerIsZero(n) || bigIntegerIsZero(m)) {
      return BIG_INTEGER_ZERO;
    }

    // optimization: second operand is one (i.e. neutral element).
    if (bigIntegerIsOne(m)) {
      return n;
    }

    // optimization: first operand is one (i.e. neutral element).
    if (bigIntegerIsOne(n)) {
      return m;
    }

    // optimization
    if (bigIntegerIsMinusOne(m)) {
      // optimization
      if (bigIntegerIsMinusOne(n)) {
        // typically not reached due to earlier test(s)
        return BIG_INTEGER_ONE;
      }

      return n.negate();
    }

    // optimization
    if (bigIntegerIsMinusOne(n)) {
      // [m is not -1, see test above]
      return m.negate();
    }

    // default case. [this would handle all cases.]
    return n.multiply(m);
  }

  /**
   * Proxy to BigInteger.pow(). For speed.
   */
  private static BigInteger bigIntegerPower(BigInteger n, int exponent) {
    // generally expecting exponent>=0
    // (there's nor much use in inverting in the integer domain)
    // the checks for exponent<0 below are done all the same

    // optimization
    // jump table, for speed.
    switch (exponent) {

    case 0:
      if (bigIntegerIsZero(n)) {
        // typically not reached, due to earlier test / [private] usage pattern
        throw new ArithmeticException("zero exp zero");
      }
      return BIG_INTEGER_ONE;

    case 1:
      return n;
    }

    // optimization
    if (bigIntegerIsZero(n) && exponent > 0) {
      // note: exponent==0 already handled above
      // typically not reached, due to earlier test
      return BIG_INTEGER_ZERO;
    }

    // optimization
    if (bigIntegerIsOne(n)) {
      return BIG_INTEGER_ONE;
    }

    // optimization
    if (bigIntegerIsMinusOne(n)) {
      return (exponent % 2 == 0 ? BIG_INTEGER_ONE : BIG_INTEGER_MINUS_ONE);
    }

    return n.pow(exponent);
  }

  /**
   * Binary logarithm rounded towards floor (towards negative infinity).
   */
  // @PrecisionLoss
  private static int bigIntegerLogarithm2(BigInteger n) {
    if (bigIntegerIsZero(n)) {
      // [typically not reached, due to [private] usage pattern]
      throw new ArithmeticException("logarithm of zero");
    }
    if (bigIntegerIsNegative(n)) {
      // [typically not reached, due to [private] usage pattern]
      throw new ArithmeticException("logarithm of negative number");
    }

    // take this as a start
    // (don't wholly rely on bitLength() having the same meaning as log2)
    int exponent = n.bitLength() - 1;
    if (exponent < 0) {
      exponent = 0;
    }

    BigInteger p = BIG_INTEGER_TWO.pow(exponent + 1);
    while (n.compareTo(p) >= 0) {
      // typically not reached
      p = p.multiply(BIG_INTEGER_TWO);
      exponent++;
    }
    p = p.divide(BIG_INTEGER_TWO);
    while (n.compareTo(p) < 0) {
      // typically not reached
      p = p.divide(BIG_INTEGER_TWO);
      exponent--;
    }

    // [possible loss of precision step]
    return exponent;
  }

  /**
   * Proxy to BigInteger.toString(int radix).
   */
  private static String stringValueOf(BigInteger n, int radix) {
    return n.toString(radix);
  }

  /**
   * Proxy to stringValueOf(bigIntegerValueOf(long), radix); take the same route to format
   * [long/bigint] integer numbers [despite the overhead].
   */
  private static String stringValueOf(long n, int radix) {
    return stringValueOf(bigIntegerValueOf(n), radix);
  }

  /**
   * Convert a IEEE 754 floating point number (of different sizes, as array of longs, big endian) to
   * a BigRational.
   */
  private static BigRational fromIeee754(long[] value0, int fractionSize, int exponentSize) {
    if (value0 == null) {
      throw new NumberFormatException("null");
    }

    // note: the long(s) in the input array are considered unsigned,
    // so expansion operations (to e.g. BigRational) and [right-] shift operations
    // (unlike assignment, equality-test, narrowing, and/or operations)
    // must be appropriately chosen

    BigRational fraction0 = ZERO;
    // start at the little end of the [bigendian] input
    int i = value0.length - 1;

    while (fractionSize >= 64) {
      if (i < 0) {
        throw new NumberFormatException("not enough bits");
      }
      // mind the long (value0[i]) being unsigned
      fraction0 = fraction0.add(valueOfUnsigned(value0[i])).divide(TWO_POWER_64);
      fractionSize -= 64;
      i--;
    }

    // the rest must now fit into value0[0] (a long),
    // i.e. we don't support exponentSize > 63 at the moment;
    // as the power() method accepts ints (not longs),
    // the restriction is actually even on <= 31 bits

    if (i < 0) {
      throw new NumberFormatException("no bits");
    }
    if (i > 0) {
      throw new NumberFormatException("excess bits");
    }

    long value = value0[0];

    // [fractionSize [now is] < 64 by loop above]
    final long fractionMask = ((long) 1 << fractionSize) - 1;
    final long rawFraction = value & fractionMask;
    value >>>= fractionSize;

    // [exponentSize < 32 by [private] usage pattern; rawExponent < 2**31]
    final int exponentMask = (1 << exponentSize) - 1;
    final int exponentBias = (1 << (exponentSize - 1)) - 1;
    final int rawExponent = (int) value & exponentMask;
    value >>>= exponentSize;

    final int signSize = 1;
    final int signMask = (1 << signSize) - 1; // 1
    final int rawSign = (int) value & signMask;
    value >>>= signSize;

    if (value != 0) {
      throw new NumberFormatException("excess bits");
    }

    // check for Infinity and NaN (IEEE 754 rawExponent at its maximum)
    if (rawExponent == exponentMask) {
      // (no fraction bits means one of the Infinities; else NaN)
      throw new NumberFormatException(
          rawFraction == 0 && fraction0.isZero() ? (rawSign == 0 ? "Infinity" : "-Infinity")
              : "NaN");
    }

    // optimization -- avoids power() calculation below
    // (isZero and zero multiply) are cheap
    // check for zero (IEEE 754 rawExponent zero and no fraction bits)
    if (rawExponent == 0 && rawFraction == 0 && fraction0.isZero()) {
      return ZERO;
    }

    // handle subnormal numbers too (with rawExponent==0)
    // [fractionSize [still is] < 64]
    final long mantissa1 = rawFraction | (rawExponent == 0 ? (long) 0 : (long) 1 << fractionSize);

    // mind mantissa1 being unsigned
    final BigRational mantissa = fraction0.add(valueOfUnsigned(mantissa1));
    // (subnormal numbers; exponent is one off)
    // [rawExponent < 2**31; exponentBias < 2**30]
    final int exponent = rawExponent - exponentBias + (rawExponent == 0 ? 1 : 0) - fractionSize;
    final int sign = (rawSign == 0 ? 1 : -1);

    return valueOf(2).power(exponent).multiply(mantissa).multiply(sign);
  }

  /**
   * Convert a BigRational to a IEEE 754 floating point number (of different sizes, as array of
   * longs, big endian).
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  private static long[] toIeee754(BigRational value, int fractionSize, int exponentSize) {
    if (value == null) {
      throw new NumberFormatException("null");
    }

    // [needed size: fractionSize+exponentSize+1; round up bits to a multiple of 64]
    final long[] out0 = new long[(fractionSize + exponentSize + 1 + (64 - 1)) / 64];

    if (value.isZero()) {
      // 0.0
      // note: as we don't keep a sign with our ZERO,
      // we never return IEEE 754 -0.0 here
      for (int j = 0; j < out0.length; j++) {
        out0[j] = 0;
      }
      return out0;
    }

    final boolean negt = value.isNegative();
    if (negt) {
      value = value.negate();
    }

    // need to scale to this to get the full mantissa
    int exponent = fractionSize;
    final BigRational lower = valueOf(2).power(fractionSize);
    final BigRational upper = lower.multiply(2);

    // optimization, and a good guess (but not exact in all cases)
    final int scale = lower.divide(value).logarithm2();
    value = value.multiply(valueOf(2).power(scale));
    exponent -= scale;

    while (value.compareTo(lower) < 0) {
      // [typically done zero or one time]
      value = value.multiply(2);
      exponent--;
    }

    while (value.compareTo(upper) >= 0) {
      // [typically not reached]
      value = value.divide(2);
      exponent++;
    }

    // [rounding step, possible loss of precision step]
    BigInteger mantissa = value.bigIntegerValue();
    // adjust after [unfortunate] mantissa rounding
    if (upper.compareTo(mantissa) <= 0) {
      mantissa = mantissa.divide(BIG_INTEGER_TWO);
      exponent++;
    }

    // start [to fill] at the little end of the [bigendian] output
    int i = out0.length - 1;

    int fractionSize1 = fractionSize;
    while (fractionSize1 >= 64) {
      final BigInteger[] divrem = mantissa.divideAndRemainder(BIG_INTEGER_TWO_POWER_64);
      // [according to BigInteger javadoc] this takes the least significant 64 bits;
      // i.e. in this case the long is considered unsigned, as we want it
      out0[i] = divrem[1].longValue();

      fractionSize1 -= 64;
      mantissa = divrem[0];
      i--;
    }

    // the rest must now fit into out0[0]

    if (i < 0) {
      // not reached
      throw new NumberFormatException("too many bits");
    }
    if (i > 0) {
      // not reached
      throw new NumberFormatException("not enough bits");
    }

    long fraction = mantissa.longValue();

    final int exponentBias = (1 << (exponentSize - 1)) - 1;
    exponent += exponentBias;
    final int maximalExponent = (1 << exponentSize) - 1;

    if (exponent >= maximalExponent) {
      // overflow
      // throw new NumberFormatException("overflow");
      // [positive or negative] infinity
      exponent = maximalExponent;
      fraction = 0;
      for (int j = 1; j < out0.length; j++) {
        out0[j] = 0;
      }
      // [keep sign]

    } else if (exponent <= 0) {
      // handle subnormal numbers too
      // [with know loss of precision]

      // drop one bit, while keeping the exponent
      int s = 1;

      // [need not shift more than fractionSize]
      final int n = (-exponent > fractionSize ? fractionSize : -exponent);
      s += n;
      exponent += n;

      // [possible loss of precision step]
      fraction = shiftrx(fraction, out0, 1, s);

      boolean zero = (fraction == 0);
      for (int j = 1; zero && j < out0.length; j++) {
        zero = (out0[j] == 0);
      }

      if (zero) {
        // underflow
        // throw new NumberFormatException("underflow");
        // 0.0 or -0.0; i.e.: keep sign
        exponent = 0;
        // [nonzero == 0 implies the rest of the fraction is zero as well]
      }
    }

    // cut implied most significant bit
    // [unless with subnormal numbers]
    if (exponent != 0) {
      fraction &= ~((long) 1 << fractionSize1);
    }

    long out = 0;
    out |= (negt ? 1 : 0);
    out <<= exponentSize;
    out |= exponent;
    out <<= fractionSize1;
    out |= fraction;

    out0[0] = out;
    return out0;
  }

  /**
   * Shift right, while propagating shifted bits (long[] is bigendian).
   */
  private static long shiftrx(long a, long[] b, int boff, int n) {
    while (n > 0) {
      final int n2 = (n < 63 ? n : 63);
      final long m = ((long) 1 << n2) - 1;
      long c = a & m;
      a >>>= n2;
      for (int i = boff; i < b.length; i++) {
        final long t = b[i] & m;
        b[i] >>>= n2;
        b[i] |= (c << (64 - n2));
        c = t;
      }
      n -= n2;
    }
    return a;
  }

  // note: don't use valueOf() here; valueOf implementations use the constants

  /**
   * The constant zero (0).
   */
  // [Constant name: see class BigInteger.]
  public final static BigRational ZERO = new BigRational(0);

  /**
   * The constant one (1).
   */
  // [Name: see class BigInteger.]
  public final static BigRational ONE = new BigRational(1);

  /**
   * The constant minus-one (-1).
   */
  public final static BigRational MINUS_ONE = new BigRational(-1);

  // some more constants, often used as radixes/bases

  /**
   * The constant two (2).
   */
  private final static BigRational TWO = new BigRational(2);

  /**
   * The constant ten (10).
   */
  private final static BigRational TEN = new BigRational(10);

  /**
   * The constant sixteen (16).
   */
  private final static BigRational SIXTEEN = new BigRational(16);

  // some more constants

  /**
   * The constant two to the power of 64 (18446744073709551616). Used is slicing larger [than double
   * size] IEEE 754 values.
   */
  private final static BigRational TWO_POWER_64 = new BigRational(BIG_INTEGER_TWO_POWER_64);

  /**
   * Positive predicate.
   * <P>
   * Indicates whether this BigRational is larger than zero. Zero is not positive.
   * <P>
   * [For convenience.]
   */
  public boolean isPositive() {
    return (signum() > 0);
  }

  /**
   * Negative predicate.
   * <P>
   * Indicates whether this BigRational is smaller than zero. Zero isn't negative either.
   * <P>
   * [For convenience.]
   */
  public boolean isNegative() {
    return (signum() < 0);
  }

  /**
   * Zero predicate.
   * <P>
   * Indicates whether this BigRational is zero.
   * <P>
   * [For convenience and speed.]
   */
  public boolean isZero() {
    // optimization
    // first test is for speed.
    if (this == ZERO || m_n == BIG_INTEGER_ZERO) {
      return true;
    }

    // well, this is also optimized for speed a bit.
    return (signum() == 0);
  }

  /**
   * One predicate.
   * <P>
   * Indicates whether this BigRational is 1.
   * <P>
   * [For convenience and speed.]
   */
  public boolean isOne() {
    // optimization
    // first test is for speed.
    if (this == ONE) {
      return true;
    }

    return equals(ONE);
  }

  /**
   * Minus-one predicate.
   * <P>
   * Indicates whether this BigRational is -1.
   * <P>
   * [For convenience and speed.]
   */
  public boolean isMinusOne() {
    // optimization
    // first test is for speed.
    if (this == MINUS_ONE) {
      return true;
    }

    return equals(MINUS_ONE);
  }

  /**
   * Integer predicate.
   * <P>
   * Indicates whether this BigRational convertible to a BigInteger without loss of precision. True
   * iff quotient is one.
   */
  public boolean isInteger() {
    return bigIntegerIsOne(m_q);
  }

  /**
   * BigRational string representation, format "[-]n[/q]".
   * <P>
   * Sample output: "6172839/5000".
   */
  public String toString(int radix) {
    checkRadixArgument(radix);
    final String s = stringValueOf(m_n, radix);

    if (isInteger()) {
      return s;
    }

    return s + "/" + stringValueOf(m_q, radix);
  }

  /**
   * BigRational string representation, format "[-]n[/q]", default radix (10).
   * <P>
   * Default string representation, as rational, not using an exponent.
   * <P>
   * Sample output: "6172839/5000".
   * <P>
   * Overwrites Object.toString().
   */
  @Override
  public String toString() {
    return toString(DEFAULT_RADIX);
  }

  /**
   * Fixed dot-format "[-]i.f" string representation, with a precision.
   * <P>
   * Precision may be negative, in which case the rounding affects digits left of the dot, i.e. the
   * integer part of the number, as well.
   * <P>
   * Sample output: "1234.567800".
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public String toStringDot(int precision, int radix) {
    return toStringDot(precision, radix, false);
  }

  /**
   * Fixed dot-format "[-]i.f" string representation, with a precision.
   * <P>
   * Precision may be negative, in which case the rounding affects digits left of the dot, i.e. the
   * integer part of the number, as well.
   * <P>
   * The exponentFormat parameter allows for shorter [intermediate] string representation, an
   * optimization, e.g. used with toStringExponent.
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  private String toStringDot(int precision, int radix, boolean exponentFormat) {
    checkRadixArgument(radix);

    BigRational scaleValue = new BigRational(bigIntegerPower(bigIntegerValueOf(radix),
        (precision < 0 ? -precision : precision)));
    if (precision < 0) {
      scaleValue = scaleValue.invert();
    }

    // default round mode.
    // [rounding step, possible loss of precision step]
    BigRational n = multiply(scaleValue).round();
    final boolean negt = n.isNegative();
    if (negt) {
      n = n.negate();
    }

    String s = n.toString(radix);

    if (exponentFormat) {
      // note that this is _not_ the scientific notation
      // (one digit left of the dot exactly),
      // but some intermediate representation suited for post processing
      // [leaving away the left/right padding steps
      // is more performant in time and memory space]
      s = s + "E" + stringValueOf(-precision, radix);

    } else {
      if (precision >= 0) {
        // left-pad with '0'
        while (s.length() <= precision) {
          s = "0" + s;
        }

        final int dot = s.length() - precision;
        final String i = s.substring(0, dot);
        final String f = s.substring(dot);

        s = i;
        if (f.length() > 0) {
          s = s + "." + f;
        }
      } else {
        if (!s.equals("0")) {
          // right-pad with '0'
          for (int i = -precision; i > 0; i--) {
            s = s + "0";
          }
        }
      }
    }

    // add sign
    if (negt) {
      s = "-" + s;
    }

    return s;
  }

  /**
   * Dot-format "[-]i.f" string representation, with a precision, default radix (10). Precision may
   * be negative.
   * <P>
   * Sample output: "1234.567800".
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public String toStringDot(int precision) {
    // [possible loss of precision step]
    return toStringDot(precision, DEFAULT_RADIX, false);
  }

  // note: there is no 'default' precision.

  /**
   * Dot-format "[-]i.f" string representation, with a relative precision.
   * <P>
   * If the relative precision is zero or negative, "0" will be returned (i.e. total loss of
   * precision).
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public String toStringDotRelative(int precision, int radix) {
    // kind of expensive, due to expensive logarithm implementation
    // (with unusual radixes), and post processing

    checkRadixArgument(radix);

    // zero doesn't have any significant digits
    if (isZero()) {
      return "0";
    }

    // relative precision zero [or less means]: no significant digits at all, i.e. 0
    // [loss of precision step]
    if (precision <= 0) {
      return "0";
    }

    // guessed [see below: rounding issues] length: sign doesn't matter;
    // one digit more than logarithm
    final int guessedLength = absolute().logarithm(radix) + 1;
    // [possible loss of precision step]
    String s = toStringDot(precision - guessedLength, radix);

    // [floor of] logarithm and [arithmetic] rounding [half-up]
    // need post-processing:

    // find first significant digit and check for dot
    boolean dot = false;
    int i;
    for (i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (c == '.') {
        dot = true;
      }
      // expecting nothing than '-', '.', and digits
      if (c != '-' && c != '.' && c != '0') {
        break;
      }
    }

    // count digits / [still] check for dot
    int digits = 0;
    for (; i < s.length(); i++) {
      if (s.charAt(i) == '.') {
        dot = true;
      } else {
        digits++;
      }
    }

    // cut excess zeros
    // expecting at most 1 excess zero, e.g. for "0.0099999"
    final int excess = digits - precision;
    if (dot && excess > 0) {
      s = s.substring(0, s.length() - excess);
    }

    return s;
  }

  /**
   * Dot-format "[-]i.f" string representation, with a relative precision, default radix (10).
   * <P>
   * If the relative precision is zero or negative, "0" will be returned (i.e. total loss of
   * precision).
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public String toStringDotRelative(int precision) {
    // [possible loss of precision step]
    return toStringDotRelative(precision, DEFAULT_RADIX);
  }

  /**
   * Exponent-format string representation, with a relative precision, "[-]i[.f]E[-]e" (where i is
   * one digit other than 0 exactly; f has no trailing 0);
   * <P>
   * Sample output: "1.2E3".
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public String toStringExponent(int precision, int radix) {
    checkRadixArgument(radix);

    // zero doesn't have any significant digits
    if (isZero()) {
      return "0";
    }

    // relative precision zero [or less means]: no significant digits at all, i.e. 0
    // [loss of precision step]
    if (precision <= 0) {
      return "0";
    }

    // guessed [see below: rounding issues] length: sign doesn't matter;
    // one digit more than logarithm
    final int guessedLength = absolute().logarithm(radix) + 1;
    // [possible loss of precision step]
    final String s = toStringDot(precision - guessedLength, radix, true);
    return toExponentRepresentation(s, radix);
  }

  /**
   * Exponent-format string representation, with a relative precision, default radix (10),
   * "[-]i[.f]E[-]e" (where i is one digit other than 0 exactly; f has no trailing 0);
   * <P>
   * Sample output: "1.2E3".
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public String toStringExponent(int precision) {
    // [possible loss of precision step]
    return toStringExponent(precision, DEFAULT_RADIX);
  }

  /**
   * Transform a [intermediate] dot representation to an exponent-format representation.
   */
  private static String toExponentRepresentation(String s, int radix) {
    // skip '+'
    if (s.length() > 0 && s.charAt(0) == '+') {
      // typically not reached, due to [private] usage pattern
      s = s.substring(1);
    }

    // handle '-'
    boolean negt = false;
    if (s.length() > 0 && s.charAt(0) == '-') {
      negt = true;
      s = s.substring(1);
    }

    // skip initial zeros
    while (s.length() > 0 && s.charAt(0) == '0') {
      s = s.substring(1);
    }

    // check for and handle exponent
    // handle only upper case 'E' (we know we use that in earlier steps);
    // this allows any base using lower case characters
    int exponent0 = 0;
    final int exp = s.indexOf('E');
    if (exp != -1) {
      final String se = s.substring(exp + 1);
      s = s.substring(0, exp);
      exponent0 = (new BigRational(se, radix)).intValueExact();
    }

    String si, sf;
    int exponent;

    final int dot = s.indexOf('.');
    if (dot != -1) {
      if (dot == 0) {
        // possibly more insignificant digits
        s = s.substring(1);
        exponent = -1;
        while (s.length() > 0 && s.charAt(0) == '0') {
          s = s.substring(1);
          exponent--;
        }

        if (s.equals("")) {
          // typically not reached, due to [private] usage pattern
          return "0";
        }

        // first significant digit
        si = s.substring(0, 1);
        sf = s.substring(1);
      } else {
        // initial [significant] digit
        si = s.substring(0, 1);
        sf = s.substring(1, dot);
        exponent = sf.length();

        sf = sf + s.substring(dot + 1);
      }
    } else {
      // [note that we just cut the zeros above]
      if (s.equals("")) {
        return "0";
      }

      // initial [significant] digit
      si = s.substring(0, 1);
      // rest
      sf = s.substring(1);
      exponent = sf.length();
    }

    exponent += exponent0;

    // drop trailing zeros
    while (sf.length() > 0 && sf.charAt(sf.length() - 1) == '0') {
      sf = sf.substring(0, sf.length() - 1);
    }

    s = si;
    if (!sf.equals("")) {
      s = s + "." + sf;
    }
    if (exponent != 0) {
      s = s + "E" + stringValueOf(exponent, radix);
    }

    if (negt) {
      s = "-" + s;
    }

    return s;
  }

  /**
   * Constant internally used, for speed.
   */
  // calculated via BigRational((float)(StrictMath.log(10)/StrictMath.log(2)))
  // note: don't use float/double operations in this code though (except for test())
  private final static BigRational LOGARITHM_TEN_GUESS = new BigRational(1741647, 524288);

  /**
   * Constant internally used, for speed.
   */
  private final static BigRational LOGARITHM_SIXTEEN = new BigRational(4);

  /**
   * Return binary logarithm rounded towards floor (towards negative infinity).
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  private int logarithm2() {
    if (isZero()) {
      // [typically not reached, due to [private] usage pattern]
      throw new ArithmeticException("logarithm of zero");
    }
    if (isNegative()) {
      // [typically not reached, due to [private] usage pattern]
      throw new ArithmeticException("logarithm of negative number");
    }

    final boolean inverted = (compareTo(ONE) < 0);
    final BigRational a = (inverted ? invert() : this);

    // [possible loss of precision step]
    final int log = bigIntegerLogarithm2(a.bigIntegerValue());
    return (inverted ? -(log + 1) : log);
  }

  /**
   * Return logarithm rounded towards floor (towards negative infinity).
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  private int logarithm(int base) {
    // optimization
    if (base == 2) {
      return logarithm2();
    }

    if (isZero()) {
      // [typically not reached, due to [private] usage pattern]
      throw new ArithmeticException("logarithm of zero");
    }
    if (isNegative()) {
      // [typically not reached, due to [private] usage pattern]
      throw new ArithmeticException("logarithm of negative number");
    }
    // if (base < 2) {
    // // [typically not reached, due to [private] usage pattern]
    // throw new ArithmeticException("bad base");
    // }
    if (base < 0) {
      // [typically not reached, due to [private] usage pattern]
      throw new ArithmeticException("negative base");
    }
    if (base < 2) {
      // [typically not reached, due to [private] usage pattern]
      throw new ArithmeticException("base too small");
    }

    final boolean inverted = (compareTo(ONE) < 0);
    BigRational a = (inverted ? invert() : this);
    final BigRational bbase = valueOf(base);

    // optimization -- we could start from n=0
    // initial guess
    // [base 2 handled earlier]
    // [unusual bases are handled a bit less performant]
    final BigRational lbase = (base == 10 ? LOGARITHM_TEN_GUESS : base == 16 ? LOGARITHM_SIXTEEN
        : valueOf(ilog2(base)));
    int n = valueOf(a.logarithm2()).divide(lbase).intValue();
    a = a.divide(bbase.power(n));

    // note that these steps are needed anyway:
    // LOGARITHM_TEN_GUESS above e.g. is (as the name suggests)
    // a guess only (since most logarithms usually can't be expressed
    // as rationals generally); odd bases or off even worse
    while (a.compareTo(bbase) >= 0) {
      a = a.divide(bbase);
      n++;
    }
    while (a.compareTo(ONE) < 0) {
      a = a.multiply(bbase);
      n--;
    }

    // [possible loss of precision step]
    return (inverted ? -(n + 1) : n);
  }

  /**
   * Return binary logarithm of an int.
   */
  private static int ilog2(int n) {
    if (n == 0) {
      // [typically not reached, due to [private] usage pattern]
      throw new ArithmeticException("logarithm of zero");
    }
    if (n < 0) {
      // [typically not reached, due to [private] usage pattern]
      throw new ArithmeticException("logarithm of negative number");
    }

    int i = 0;

    // as this method is used in the context of [small] bases/radixes,
    // we expect less than 8 iterations at most, so no need to optimize
    while (n > 1) {
      n /= 2;
      i++;
    }

    return i;
  }

  /**
   * Add a BigRational to this BigRational and return a new BigRational.
   * <P>
   * If one of the operands is zero, [as an optimization] the other BigRational is returned.
   */
  // [Name: see class BigInteger.]
  public BigRational add(BigRational that) {
    // optimization: second operand is zero (i.e. neutral element).
    if (that.isZero()) {
      return this;
    }

    // optimization: first operand is zero (i.e. neutral element).
    if (isZero()) {
      return that;
    }

    // note: not checking for that.equals(negate()),
    // since that would involve creation of a temporary object

    // note: the calculated n/q may be denormalized,
    // implicit normalize() is needed.

    // optimization: same denominator.
    if (bigIntegerEquals(m_q, that.m_q)) {
      return new BigRational(m_n.add(that.m_n), m_q);
    }

    // optimization: second operand is an integer.
    if (that.isInteger()) {
      return new BigRational(m_n.add(that.m_n.multiply(m_q)), m_q);
    }

    // optimization: first operand is an integer.
    if (isInteger()) {
      return new BigRational(m_n.multiply(that.m_q).add(that.m_n), that.m_q);
    }

    // default case. [this would handle all cases.]
    return new BigRational(m_n.multiply(that.m_q).add(that.m_n.multiply(m_q)), m_q
        .multiply(that.m_q));
  }

  /**
   * Add a long fix number integer to this BigRational and return a new BigRational.
   */
  public BigRational add(long that) {
    return add(valueOf(that));
  }

  /**
   * Subtract a BigRational from this BigRational and return a new BigRational.
   * <P>
   * If the second operand is zero, [as an optimization] this BigRational is returned.
   */
  // [Name: see class BigInteger.]
  public BigRational subtract(BigRational that) {
    // optimization: second operand is zero.
    if (that.isZero()) {
      return this;
    }

    // optimization: first operand is zero
    if (isZero()) {
      return that.negate();
    }

    // optimization: operands are equal
    if (equals(that)) {
      return ZERO;
    }

    // note: the calculated n/q may be denormalized,
    // implicit normalize() is needed.

    // optimization: same denominator.
    if (bigIntegerEquals(m_q, that.m_q)) {
      return new BigRational(m_n.subtract(that.m_n), m_q);
    }

    // optimization: second operand is an integer.
    if (that.isInteger()) {
      return new BigRational(m_n.subtract(that.m_n.multiply(m_q)), m_q);
    }

    // optimization: first operand is an integer.
    if (isInteger()) {
      return new BigRational(m_n.multiply(that.m_q).subtract(that.m_n), that.m_q);
    }

    // default case. [this would handle all cases.]
    return new BigRational(m_n.multiply(that.m_q).subtract(that.m_n.multiply(m_q)), m_q
        .multiply(that.m_q));
  }

  /**
   * Subtract a long fix number integer from this BigRational and return a new BigRational.
   */
  public BigRational subtract(long that) {
    return subtract(valueOf(that));
  }

  /**
   * An alias to subtract().
   */
  public BigRational sub(BigRational that) {
    return subtract(that);
  }

  /**
   * An alias to subtract().
   */
  public BigRational sub(long that) {
    return subtract(that);
  }

  /**
   * Multiply a BigRational to this BigRational and return a new BigRational.
   * <P>
   * If one of the operands is one, [as an optimization] the other BigRational is returned.
   */
  // [Name: see class BigInteger.]
  public BigRational multiply(BigRational that) {
    // optimization: one or both operands are zero.
    if (that.isZero() || isZero()) {
      return ZERO;
    }

    // optimization: second operand is 1.
    if (that.isOne()) {
      return this;
    }

    // optimization: first operand is 1.
    if (isOne()) {
      return that;
    }

    // optimization: second operand is -1.
    if (that.isMinusOne()) {
      return negate();
    }

    // optimization: first operand is -1.
    if (isMinusOne()) {
      return that.negate();
    }

    // note: the calculated n/q may be denormalized,
    // implicit normalize() is needed.

    return new BigRational(bigIntegerMultiply(m_n, that.m_n), bigIntegerMultiply(m_q, that.m_q));
  }

  /**
   * Multiply a long fix number integer to this BigRational and return a new BigRational.
   */
  public BigRational multiply(long that) {
    return multiply(valueOf(that));
  }

  /**
   * An alias to multiply().
   */
  public BigRational mul(BigRational that) {
    return multiply(that);
  }

  /**
   * An alias to multiply().
   */
  public BigRational mul(long that) {
    return multiply(that);
  }

  /**
   * Divide this BigRational through another BigRational and return a new BigRational.
   * <P>
   * If the second operand is one, [as an optimization] this BigRational is returned.
   */
  public BigRational divide(BigRational that) {
    if (that.isZero()) {
      throw new ArithmeticException("division by zero");
    }

    // optimization: first operand is zero.
    if (isZero()) {
      return ZERO;
    }

    // optimization: second operand is 1.
    if (that.isOne()) {
      return this;
    }

    // optimization: first operand is 1.
    if (isOne()) {
      return that.invert();
    }

    // optimization: second operand is -1.
    if (that.isMinusOne()) {
      return negate();
    }

    // optimization: first operand is -1.
    if (isMinusOne()) {
      return that.invert().negate();
    }

    // note: the calculated n/q may be denormalized,
    // implicit normalize() is needed.

    return new BigRational(bigIntegerMultiply(m_n, that.m_q), bigIntegerMultiply(m_q, that.m_n));
  }

  /**
   * Divide this BigRational through a long fix number integer and return a new BigRational.
   */
  public BigRational divide(long that) {
    return divide(valueOf(that));
  }

  /**
   * An alias to divide().
   */
  public BigRational div(BigRational that) {
    return divide(that);
  }

  /**
   * An alias to divide().
   */
  public BigRational div(long that) {
    return divide(that);
  }

  /**
   * Calculate this BigRational's integer power and return a new BigRational.
   * <P>
   * The integer exponent may be negative.
   * <P>
   * If the exponent is one, [as an optimization] this BigRational is returned.
   */
  public BigRational power(int exponent) {
    final boolean zero = isZero();

    if (zero) {
      if (exponent == 0) {
        throw new ArithmeticException("zero exp zero");
      }

      if (exponent < 0) {
        throw new ArithmeticException("division by zero");
      }
    }

    // optimization
    if (exponent == 0) {
      return ONE;
    }

    // optimization
    // test for exponent<=0 already done
    if (zero) {
      return ZERO;
    }

    // optimization
    if (exponent == 1) {
      return this;
    }

    // optimization
    if (exponent == -1) {
      return invert();
    }

    final boolean negt = (exponent < 0);
    if (negt) {
      exponent = -exponent;
    }

    final BigInteger n = bigIntegerPower(m_n, exponent);
    final BigInteger q = bigIntegerPower(m_q, exponent);

    // note: the calculated n/q are not denormalized in the sense
    // of having common factors, but n might be negative (and become q below)

    return new BigRational((negt ? q : n), (negt ? n : q));
  }

  /**
   * An alias to power().
   */
  // [Name: see classes Math, BigInteger.]
  public BigRational pow(int exponent) {
    return power(exponent);
  }

  /**
   * Calculate the remainder of this BigRational and another BigRational and return a new
   * BigRational.
   * <P>
   * The remainder result may be negative.
   * <P>
   * The remainder is based on round down (towards zero) / truncate. 5/3 == 1 + 2/3 (remainder 2),
   * 5/-3 == -1 + 2/-3 (remainder 2), -5/3 == -1 + -2/3 (remainder -2), -5/-3 == 1 + -2/-3
   * (remainder -2).
   */
  // [Name: see class BigInteger.]
  public BigRational remainder(BigRational that) {
    final int s = signum();
    final int ts = that.signum();

    if (ts == 0) {
      throw new ArithmeticException("division by zero");
    }

    BigRational a = this;
    if (s < 0) {
      a = a.negate();
    }

    // divisor's sign doesn't matter, as stated above.
    // this is also BigInteger's behavior, but don't let us be
    // dependent of a change in that.
    BigRational b = that;
    if (ts < 0) {
      b = b.negate();
    }

    BigRational r = a.remainderOrModulusOfPositive(b);

    if (s < 0) {
      r = r.negate();
    }

    return r;
  }

  /**
   * Calculate the remainder of this BigRational and a long fix number integer and return a new
   * BigRational.
   */
  public BigRational remainder(long that) {
    return remainder(valueOf(that));
  }

  /**
   * An alias to remainder().
   */
  public BigRational rem(BigRational that) {
    return remainder(that);
  }

  /**
   * An alias to remainder().
   */
  public BigRational rem(long that) {
    return remainder(that);
  }

  /**
   * Calculate the modulus of this BigRational and another BigRational and return a new BigRational.
   * <P>
   * The modulus result may be negative.
   * <P>
   * Modulus is based on round floor (towards negative). 5/3 == 1 + 2/3 (modulus 2), 5/-3 == -2 +
   * -1/-3 (modulus -1), -5/3 == -2 + 1/3 (modulus 1), -5/-3 == 1 + -2/-3 (modulus -2).
   */
  public BigRational modulus(BigRational that) {
    final int s = signum();
    final int ts = that.signum();

    if (ts == 0) {
      throw new ArithmeticException("division by zero");
    }

    BigRational a = this;
    if (s < 0) {
      a = a.negate();
    }

    BigRational b = that;
    if (ts < 0) {
      b = b.negate();
    }

    BigRational r = a.remainderOrModulusOfPositive(b);

    if (s < 0 && ts < 0) {
      r = r.negate();
    } else if (ts < 0) {
      r = r.subtract(b);
    } else if (s < 0) {
      r = b.subtract(r);
    }

    return r;
  }

  /**
   * Calculate the modulus of this BigRational and a long fix number integer and return a new
   * BigRational.
   */
  public BigRational modulus(long that) {
    return modulus(valueOf(that));
  }

  /**
   * An alias to modulus().
   */
  // [Name: see class BigInteger.]
  public BigRational mod(BigRational that) {
    return modulus(that);
  }

  /**
   * An alias to modulus().
   */
  public BigRational mod(long that) {
    return modulus(that);
  }

  /**
   * Remainder or modulus of non-negative values. Helper function to remainder() and modulus().
   */
  private BigRational remainderOrModulusOfPositive(BigRational that) {
    final int s = signum();
    final int ts = that.signum();

    if (s < 0 || ts < 0) {
      // typically not reached, due to [private] usage pattern
      throw new IllegalArgumentException("negative values(s)");
    }

    if (ts == 0) {
      // typically not reached, due to [private] usage pattern
      throw new ArithmeticException("division by zero");
    }

    // optimization
    if (s == 0) {
      return ZERO;
    }

    return new BigRational(bigIntegerMultiply(m_n, that.m_q).remainder(
        bigIntegerMultiply(m_q, that.m_n)), bigIntegerMultiply(m_q, that.m_q));
  }

  /**
   * Signum. -1, 0, or 1.
   * <P>
   * If this BigRational is negative, -1 is returned; if it is zero, 0 is returned; if it is
   * positive, 1 is returned.
   */
  // [Name: see class BigInteger.]
  public int signum() {
    // note: m_q is positive.
    return m_n.signum();
  }

  /**
   * An alias to signum().
   */
  public int sign() {
    return signum();
  }

  /**
   * Return a new BigRational with the absolute value of this BigRational.
   * <P>
   * If this BigRational is zero or positive, [as an optimization] this BigRational is returned.
   */
  public BigRational absolute() {
    if (signum() >= 0) {
      return this;
    }

    // optimization
    if (isMinusOne()) {
      return ONE;
    }

    // note: the calculated n/q are not denormalized,
    // implicit normalize() would not be needed.

    return new BigRational(m_n.negate(), m_q);
  }

  /**
   * An alias to absolute(). [Name: see classes Math, BigInteger.]
   */
  public BigRational abs() {
    return absolute();
  }

  /**
   * Return a new BigRational with the negative value of this. [Name: see class BigInteger.]
   */
  public BigRational negate() {
    // optimization
    if (isZero()) {
      return ZERO;
    }

    // optimization
    if (isOne()) {
      return MINUS_ONE;
    }

    // optimization
    if (isMinusOne()) {
      return ONE;
    }

    // note: the calculated n/q are not denormalized,
    // implicit normalize() would not be needed.

    return new BigRational(m_n.negate(), m_q);
  }

  /**
   * An alias to negate().
   */
  public BigRational neg() {
    return negate();
  }

  /**
   * Return a new BigRational with the inverted (reciprocal) value of this.
   */
  public BigRational invert() {
    if (isZero()) {
      throw new ArithmeticException("division by zero");
    }

    // optimization
    if (isOne() || isMinusOne()) {
      return this;
    }

    // note: the calculated n/q are not denormalized in the sense
    // of having common factors, but n might be negative (and become q below)

    return new BigRational(m_q, m_n);
  }

  /**
   * An alias to invert().
   */
  public BigRational inv() {
    return invert();
  }

  /**
   * Return the minimal value of two BigRationals.
   */
  public BigRational minimum(BigRational that) {
    return (compareTo(that) <= 0 ? this : that);
  }

  /**
   * Return the minimal value of a BigRational and a long fix number integer.
   */
  public BigRational minimum(long that) {
    return minimum(valueOf(that));
  }

  /**
   * An alias to minimum(). [Name: see classes Math, BigInteger.]
   */
  public BigRational min(BigRational that) {
    return minimum(that);
  }

  /**
   * An alias to minimum().
   */
  public BigRational min(long that) {
    return minimum(that);
  }

  /**
   * Return the maximal value of two BigRationals.
   */
  public BigRational maximum(BigRational that) {
    return (compareTo(that) >= 0 ? this : that);
  }

  /**
   * Return the maximum value of a BigRational and a long fix number integer.
   */
  public BigRational maximum(long that) {
    return maximum(valueOf(that));
  }

  /**
   * An alias to maximum(). [Name: see classes Math, BigInteger.]
   */
  public BigRational max(BigRational that) {
    return maximum(that);
  }

  /**
   * An alias to maximum().
   */
  public BigRational max(long that) {
    return maximum(that);
  }

  /**
   * Compare object for equality. Overwrites Object.equals(). Semantic of equality to
   * non-BigRational changed from earlier version: only BigRationals can be equal. Never throws.
   * <P>
   * Overwrites Object.equals(Object).
   */
  @Override
  public boolean equals(Object object) {
    // optimization
    if (object == this) {
      return true;
    }

    // test includes null
    if (!(object instanceof BigRational)) {
      return false;
    }
    final BigRational that = (BigRational) object;

    // optimization
    if (that.m_n == m_n && that.m_q == m_q) {
      return true;
    }

    return (bigIntegerEquals(that.m_n, m_n) && bigIntegerEquals(that.m_q, m_q));

    // [old version]
    // // delegate to compareTo(Object)
    // try {
    // return (compareTo(object) == 0);
    // } catch (ClassCastException e) {
    // return false;
    // }
  }

  /**
   * Hash code. Overwrites Object.hashCode().
   * <P>
   * Overwrites Object.hashCode().
   */
  @Override
  public int hashCode() {
    return ((m_n.hashCode() + 1) * (m_q.hashCode() + 2));
  }

  /**
   * Compare this BigRational to another BigRational.
   */
  public int compareTo(BigRational that) {
    // optimization
    if (that == this) {
      return 0;
    }

    final int s = signum();
    final int t = that.signum();

    if (s != t) {
      return (s < t ? -1 : 1);
    }

    // optimization: both zero.
    if (s == 0) {
      return 0;
    }

    // note: both m_q are positive.
    return bigIntegerMultiply(m_n, that.m_q).compareTo(bigIntegerMultiply(that.m_n, m_q));
  }

  /**
   * Compare this BigRational to a BigInteger.
   */
  public int compareTo(BigInteger that) {
    return compareTo(valueOf(that));
  }

  /**
   * Compare this BigRational to a long.
   * <P>
   * Bytes, shorts, and ints can use this by being promoted to long.
   */
  public int compareTo(long that) {
    return compareTo(valueOf(that));
  }

  /**
   * Compare this BigRational to an Object.
   * <P>
   * Object can be BigRational/BigInteger/Long/Integer/Short/Byte.
   * <P>
   * Implements Comparable.compareTo(Object) (JDK 1.2 and later).
   * <P>
   * A sample use is with a sorted map or set, e.g. TreeSet.
   * <P>
   * Only BigRational/BigInteger/Long/Integer objects allowed, method will throw otherwise.
   * <P>
   * For backward compatibility reasons we keep compareTo(Object) additionally to
   * compareTo(BigRational). Comparable&lt;Object&gt; is declared to be implemented rather than
   * Comparable&lt;BigRational&gt;.
   */
  public int compareTo(Object object) {
    if (object instanceof Byte) {
      return compareTo(((Byte) object).longValue());
    }

    if (object instanceof Short) {
      return compareTo(((Short) object).longValue());
    }

    if (object instanceof Integer) {
      return compareTo(((Integer) object).longValue());
    }

    if (object instanceof Long) {
      return compareTo(((Long) object).longValue());
    }

    if (object instanceof BigInteger) {
      return compareTo((BigInteger) object);
    }

    // now assuming that it's either 'instanceof BigRational'
    // or it'll throw a ClassCastException.

    return compareTo((BigRational) object);
  }

  /**
   * Number of explicit fraction bits in an IEEE 754 double (binary64) float, 52.
   */
  private final static int DOUBLE_FLOAT_FRACTION_SIZE = 52;

  /**
   * Number of exponent bits in an IEEE 754 double (binary64) float, 11.
   */
  private final static int DOUBLE_FLOAT_EXPONENT_SIZE = 11;

  /**
   * Number of explicit fraction bits in an IEEE 754 single (binary32) float, 23.
   */
  private final static int SINGLE_FLOAT_FRACTION_SIZE = 23;

  /**
   * Number of exponent bits in an IEEE 754 single (binary32) float, 8.
   */
  private final static int SINGLE_FLOAT_EXPONENT_SIZE = 8;

  /**
   * Number of explicit fraction bits in an IEEE 754 half (binary16) float, 10.
   */
  private final static int HALF_FLOAT_FRACTION_SIZE = 10;

  /**
   * Number of exponent bits in an IEEE 754 half (binary16) float, 5.
   */
  private final static int HALF_FLOAT_EXPONENT_SIZE = 5;

  /**
   * Number of explicit fraction bits in an IEEE 754 quad (binary128, quadruple) float, 112.
   */
  private final static int QUAD_FLOAT_FRACTION_SIZE = 112;

  /**
   * Number of exponent bits in an IEEE 754 quad (binary128, quadruple) float, 15.
   */
  private final static int QUAD_FLOAT_EXPONENT_SIZE = 15;

  /**
   * Convert to BigInteger, by rounding.
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public BigInteger bigIntegerValue() {
    // [rounding step, possible loss of precision step]
    return round().m_n;
  }

  /**
   * Convert to long, by rounding and delegating to BigInteger. Implements Number.longValue(). As
   * described with BigInteger.longValue(), this just returns the low-order [64] bits (losing
   * information about magnitude and sign).
   * <P>
   * Possible loss of precision.
   * <P>
   * Overwrites Number.longValue().
   */
  @Override
  // @PrecisionLoss
  public long longValue() {
    // delegate to BigInteger.
    // [rounding step, possible loss of precision step]
    return bigIntegerValue().longValue();
  }

  /**
   * Convert to int, by rounding and delegating to BigInteger. Implements Number.intValue(). As
   * described with BigInteger.longValue(), this just returns the low-order [32] bits (losing
   * information about magnitude and sign).
   * <P>
   * Possible loss of precision.
   * <P>
   * Overwrites Number.intValue().
   */
  @Override
  // @PrecisionLoss
  public int intValue() {
    // delegate to BigInteger.
    // [rounding step, possible loss of precision step]
    return bigIntegerValue().intValue();
  }

  /**
   * Convert to double floating point value. Implements Number.doubleValue().
   * <P>
   * Possible loss of precision.
   * <P>
   * Overwrites Number.doubleValue().
   */
  @Override
  // @PrecisionLoss
  public double doubleValue() {
    // // [rounding step, possible loss of precision step]
    // return (m_n.doubleValue() / m_q.doubleValue());
    // // [rounding step, possible loss of precision step]
    // return Double.parseDouble(toStringExponent(24));
    return Double.longBitsToDouble(
    // [rounding step, possible loss of precision step]
        doubleBitsValue());
  }

  /**
   * Convert to single floating point value. Implements Number.floatValue().
   * <P>
   * Note that BigRational's [implicit] [default] rounding mode that applies [too] on indirect
   * double to BigRational to float rounding (round-half-up) may differ from what's done in a direct
   * cast/coercion from double to float (e.g. round-half-even).
   * <P>
   * Possible loss of precision.
   * <P>
   * Overwrites Number.floatValue().
   */
  @Override
  // @PrecisionLoss
  public float floatValue() {
    // // [rounding step, possible loss of precision step]
    // return (float)doubleValue();
    return Float.intBitsToFloat(
    // [rounding step, possible loss of precision step]
        singleBitsValue());
  }

  /**
   * Convert to IEEE 754 double float bits. The bits can be converted to a double by
   * Double.longBitsToDouble().
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public long doubleBitsValue() {
    // [rounding step, possible loss of precision step]
    return (toIeee754(this, DOUBLE_FLOAT_FRACTION_SIZE, DOUBLE_FLOAT_EXPONENT_SIZE)[0]);
  }

  /**
   * Convert to IEEE 754 single float bits. The bits can be converted to a float by
   * Float.intBitsToFloat().
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public int floatBitsValue() {
    // [rounding step, possible loss of precision step]
    return (int) (toIeee754(this, SINGLE_FLOAT_FRACTION_SIZE, SINGLE_FLOAT_EXPONENT_SIZE)[0]);
  }

  /**
   * An alias to floatBitsValue().
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public int singleBitsValue() {
    // [rounding step, possible loss of precision step]
    return floatBitsValue();
  }

  /**
   * Convert this BigRational to IEEE 754 half float (binary16) bits.
   * <P>
   * As a short value is returned rather than a int, care has to be taken no unwanted sign expansion
   * happens in subsequent operations, e.g. by masking (x.halfBitsValue()&0xffffl) or similar
   * (x.halfBitsValue()==(short)0xbc00).
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public short halfBitsValue() {
    // [rounding step, possible loss of precision step]
    return (short) (toIeee754(this, HALF_FLOAT_FRACTION_SIZE, HALF_FLOAT_EXPONENT_SIZE)[0]);
  }

  /**
   * Convert this BigRational to IEEE 754 quad float (binary128, quadruple) bits.
   * <P>
   * The bits are returned in an array of two longs, big endian (higher significant long first).
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public long[] quadBitsValue() {
    // [rounding step, possible loss of precision step]
    return toIeee754(this, QUAD_FLOAT_FRACTION_SIZE, QUAD_FLOAT_EXPONENT_SIZE);
  }

  /**
   * Convert this BigRational to a long integer, either returning an exact result (no rounding or
   * truncation needed), or throw an ArithmeticException.
   */
  public long longValueExact() {
    final long i = longValue();
    // test is kind-of costly
    if (!equals(valueOf(i))) {
      throw new ArithmeticException(isInteger() ? "overflow" : "rounding necessary");
    }
    return i;
  }

  /**
   * Convert this BigRational to an int, either returning an exact result (no rounding or truncation
   * needed), or throw an ArithmeticException.
   */
  public int intValueExact() {
    final int i = intValue();
    // test is kind-of costly
    if (!equals(valueOf(i))) {
      throw new ArithmeticException(isInteger() ? "overflow" : "rounding necessary");
    }
    return i;
  }

  /**
   * Convert this BigRational to its constant (ONE, ZERO, MINUS_ONE) if possible.
   */
  public static BigRational valueOf(BigRational value) {
    if (value == null) {
      throw new NumberFormatException("null");
    }

    // note: these tests are quite expensive,
    // but they are minimized to a reasonable amount.

    // priority in the tests: 1, 0, -1;

    // two phase testing.
    // cheap tests first.

    // optimization
    if (value == ONE) {
      return value;
    }

    // optimization
    if (value == ZERO) {
      return value;
    }

    // optimization
    if (value == MINUS_ONE) {
      return value;
    }

    // more expensive tests later.

    // optimization
    if (value.equals(ONE)) {
      return ONE;
    }

    // optimization
    if (value.equals(ZERO)) {
      return ZERO;
    }

    // optimization
    if (value.equals(MINUS_ONE)) {
      return MINUS_ONE;
    }

    // not a known constant
    return value;
  }

  /**
   * Build a BigRational from a String.
   * <P>
   * [Roughly] equivalent to <CODE>new BigRational(value)</CODE>.
   */
  public static BigRational valueOf(String value) {
    if (value == null) {
      throw new NumberFormatException("null");
    }

    // optimization
    if (value.equals("0")) {
      return ZERO;
    }

    // optimization
    if (value.equals("1")) {
      return ONE;
    }

    // optimization
    if (value.equals("-1")) {
      return MINUS_ONE;
    }

    return new BigRational(value);
  }

  /**
   * Build a BigRational from a BigInteger.
   * <P>
   * Equivalent to <CODE>new BigRational(value)</CODE>.
   */
  public static BigRational valueOf(BigInteger value) {
    return new BigRational(value);
  }

  /**
   * Build a BigRational from a long fix number integer.
   * <P>
   * [Roughly] equivalent to <CODE>new BigRational(value)</CODE>.
   * <P>
   * As an optimization, commonly used numbers are returned as a reused constant.
   */
  public static BigRational valueOf(long value) {
    // return the internal constants if possible

    // optimization
    // check whether it's outside int range.
    // actually check a much narrower range, fitting the switch below.
    if (value >= -16 && value <= 16) {
      // note: test above needed to make the cast below safe
      // jump table, for speed
      switch ((int) value) {
      case 0:
        return ZERO;
      case 1:
        return ONE;
      case -1:
        return MINUS_ONE;
      case 2:
        return TWO;
      case 10:
        return TEN;
      case 16:
        return SIXTEEN;
      }
    }

    return new BigRational(value);
  }

  // note: byte/short/int implicitly upgraded to long,
  // so strictly the additional implementations aren't needed;
  // with unsigned (below) they however are

  /**
   * Build a BigRational from an int.
   */
  public static BigRational valueOf(int value) {
    return valueOf((long) value);
  }

  /**
   * Build a BigRational from a short.
   */
  public static BigRational valueOf(short value) {
    return valueOf((long) value);
  }

  /**
   * Build a BigRational from a byte.
   */
  public static BigRational valueOf(byte value) {
    return valueOf((long) value);
  }

  /**
   * Build a BigRational from a [IEEE 754] double [size/precision] floating point number.
   */
  public static BigRational valueOf(double value) {
    return new BigRational(value);
  }

  /**
   * Build a BigRational from a [IEEE 754] single [size/precision] floating point number.
   */
  public static BigRational valueOf(float value) {
    return new BigRational(value);
  }

  /**
   * Build a BigRational from an unsigned long fix number integer.
   * <P>
   * The resulting BigRational is positive, i.e. the negative longs are mapped to 2**63..2**64
   * (exclusive).
   */
  public static BigRational valueOfUnsigned(long value) {
    final BigRational b = valueOf(value);
    // mind the long being unsigned with highest significant
    // bit (bit#63) set (interpreted as negative by valueOf(long))
    return (b.isNegative() ? b.add(TWO_POWER_64) : b);
  }

  /**
   * Build a BigRational from an unsigned int.
   * <P>
   * The resulting BigRational is positive, i.e. the negative ints are mapped to 2**31..2**32
   * (exclusive).
   */
  public static BigRational valueOfUnsigned(int value) {
    // masking: suppress sign expansion
    return valueOf(value & 0xffffffffl);
  }

  /**
   * Build a BigRational from an unsigned short.
   * <P>
   * The resulting BigRational is positive, i.e. the negative shorts are mapped to 2**15..2**16
   * (exclusive).
   */
  public static BigRational valueOfUnsigned(short value) {
    // masking: suppress sign expansion
    return valueOf(value & 0xffffl);
  }

  /**
   * Build a BigRational from an unsigned byte.
   * <P>
   * The resulting BigRational is positive, i.e. the negative bytes are mapped to 2**7..2**8
   * (exclusive).
   */
  public static BigRational valueOfUnsigned(byte value) {
    // masking: suppress sign expansion
    return valueOf(value & 0xffl);
  }

  /**
   * Build a BigRational from an IEEE 754 double size (double precision, binary64) floating point
   * number represented as long.
   * <P>
   * An IEEE 754 double size (binary64) number uses 1 bit for the sign, 11 bits for the exponent,
   * and 52 bits (plus 1 implicit bit) for the fraction/mantissa. The minimal exponent encodes
   * subnormal nubers; the maximal exponent encodes Infinities and NaNs.
   * <P>
   * Infinities and NaNs are not supported as BigRationals.
   * <P>
   * The conversion from the bits to a BigRational is done without loss of precision.
   */
  public static BigRational valueOfDoubleBits(long value) {
    return fromIeee754(new long[] { value, }, DOUBLE_FLOAT_FRACTION_SIZE,
        DOUBLE_FLOAT_EXPONENT_SIZE);
  }

  /**
   * Build a BigRational from an IEEE 754 single size (single precision, binary32) floating point
   * number represented as int.
   * <P>
   * An IEEE 754 single size (binary32) number uses 1 bit for the sign, 8 bits for the exponent, and
   * 23 bits (plus 1 implicit bit) for the fraction/mantissa. The minimal exponent encodes subnormal
   * nubers; the maximal exponent encodes Infinities and NaNs.
   * <P>
   * Infinities and NaNs are not supported as BigRationals.
   * <P>
   * The conversion from the bits to a BigRational is done without loss of precision.
   */
  public static BigRational valueOfFloatBits(int value) {
    // [masking: suppress sign expansion, that leads to excess bits,
    // that's not accepted by fromIeee754()]
    return fromIeee754(new long[] { value & 0xffffffffl, }, SINGLE_FLOAT_FRACTION_SIZE,
        SINGLE_FLOAT_EXPONENT_SIZE);
  }

  /**
   * An alias to valueOfFloatBits().
   */
  public static BigRational valueOfSingleBits(int value) {
    return valueOfFloatBits(value);
  }

  /**
   * Build a BigRational from an IEEE 754 half size (half precision, binary16) floating point number
   * represented as short.
   * <P>
   * An IEEE 754 half size (binary16) number uses 1 bit for the sign, 5 bits for the exponent, and
   * 10 bits (plus 1 implicit bit) for the fraction/mantissa. The minimal exponent encodes subnormal
   * nubers; the maximal exponent encodes Infinities and NaNs.
   * <P>
   * Infinities and NaNs are not supported as BigRationals.
   * <P>
   * The conversion from the bits to a BigRational is done without loss of precision.
   */
  public static BigRational valueOfHalfBits(short value) {
    // [masking: suppress sign expansion, that leads to excess bits,
    // that's not accepted by fromIeee754()]
    return fromIeee754(new long[] { value & 0xffffl, }, HALF_FLOAT_FRACTION_SIZE,
        HALF_FLOAT_EXPONENT_SIZE);
  }

  /**
   * Build a BigRational from an IEEE 754 quad size (quadruple precision, binary128) floating point
   * number represented as an array of two longs (big endian; higher significant long first).
   * <P>
   * An IEEE 754 quad size (binary128, quadruple) number uses 1 bit for the sign, 15 bits for the
   * exponent, and 112 bits (plus 1 implicit bit) for the fraction/mantissa. The minimal exponent
   * encodes subnormal nubers; the maximal exponent encodes Infinities and NaNs.
   * <P>
   * Infinities and NaNs are not supported as BigRationals.
   * <P>
   * The conversion from the bits to a BigRational is done without loss of precision.
   */
  public static BigRational valueOfQuadBits(long[] value) {
    return fromIeee754(value, QUAD_FLOAT_FRACTION_SIZE, QUAD_FLOAT_EXPONENT_SIZE);
  }

  /**
   * Compare two IEEE 754 quad size (quadruple precision, binary128) floating point numbers (each
   * represented as two longs). NaNs are not considered; comparison is done by bits. [Convenience
   * method.]
   */
  // note: especially due the NaN issue commented above
  // (a NaN maps to many bits representations),
  // we call this method quadBitsEqual rather than quadEqual
  public static boolean quadBitsEqual(long[] a, long[] b) {
    if (a == null || b == null) {
      throw new NumberFormatException("null");
    }
    if (a.length != 2 || b.length != 2) {
      throw new NumberFormatException("not a quad");
    }
    return (a[1] == b[1] && a[0] == b[0]);
  }

  /**
   * Rounding mode to round away from zero.
   */
  public final static int ROUND_UP = 0;

  /**
   * Rounding mode to round towards zero.
   */
  public final static int ROUND_DOWN = 1;

  /**
   * Rounding mode to round towards positive infinity.
   */
  public final static int ROUND_CEILING = 2;

  /**
   * Rounding mode to round towards negative infinity.
   */
  public final static int ROUND_FLOOR = 3;

  /**
   * Rounding mode to round towards nearest neighbor unless both neighbors are equidistant, in which
   * case to round up.
   */
  public final static int ROUND_HALF_UP = 4;

  /**
   * Rounding mode to round towards nearest neighbor unless both neighbors are equidistant, in which
   * case to round down.
   */
  public final static int ROUND_HALF_DOWN = 5;

  /**
   * Rounding mode to round towards the nearest neighbor unless both neighbors are equidistant, in
   * which case to round towards the even neighbor.
   */
  public final static int ROUND_HALF_EVEN = 6;

  /**
   * Rounding mode to assert that the requested operation has an exact result, hence no rounding is
   * necessary. If this rounding mode is specified on an operation that yields an inexact result, an
   * ArithmeticException is thrown.
   */
  public final static int ROUND_UNNECESSARY = 7;

  /**
   * Rounding mode to round towards nearest neighbor unless both neighbors are equidistant, in which
   * case to round ceiling.
   */
  public final static int ROUND_HALF_CEILING = 8;

  /**
   * Rounding mode to round towards nearest neighbor unless both neighbors are equidistant, in which
   * case to round floor.
   */
  public final static int ROUND_HALF_FLOOR = 9;

  /**
   * Rounding mode to round towards the nearest neighbor unless both neighbors are equidistant, in
   * which case to round towards the odd neighbor.
   */
  public final static int ROUND_HALF_ODD = 10;

  /**
   * Default round mode, ROUND_HALF_UP.
   */
  public final static int DEFAULT_ROUND_MODE = ROUND_HALF_UP;

  /**
   * Round.
   * <P>
   * Round mode is one of {<CODE>ROUND_UP, ROUND_DOWN, ROUND_CEILING, ROUND_FLOOR,
   * ROUND_HALF_UP, ROUND_HALF_DOWN, ROUND_HALF_EVEN,
   * ROUND_HALF_CEILING, ROUND_HALF_FLOOR, ROUND_HALF_ODD,
   * ROUND_UNNECESSARY, DEFAULT_ROUND_MODE (==ROUND_HALF_UP)</CODE> .
   * <P>
   * If rounding isn't necessary, i.e. this BigRational is an integer, [as an optimization] this
   * BigRational is returned.
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public BigRational round(int roundMode) {
    // optimization
    // return self if we don't need to round, independent of rounding mode
    if (isInteger()) {
      return this;
    }

    return new BigRational(
    // [rounding step, possible loss of precision step]
        roundToBigInteger(roundMode));
  }

  /**
   * Round to BigInteger helper function. Internally used.
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  private BigInteger roundToBigInteger(int roundMode) {
    // note: remainder and its duplicate are calculated for all cases.

    BigInteger n = m_n;
    final BigInteger q = m_q;

    final int sgn = n.signum();

    // optimization
    if (sgn == 0) {
      // [typically not reached due to earlier test for integerp]
      return BIG_INTEGER_ZERO;
    }

    // keep info on the sign
    final boolean pos = (sgn > 0);

    // operate on positive values
    if (!pos) {
      n = n.negate();
    }

    final BigInteger[] divrem = n.divideAndRemainder(q);
    BigInteger dv = divrem[0];
    final BigInteger r = divrem[1];

    // return if we don't need to round, independent of rounding mode
    if (bigIntegerIsZero(r)) {
      // [typically not reached since remainder is not zero
      // with normalized that are not integerp]
      if (!pos) {
        dv = dv.negate();
      }

      return dv;
    }

    boolean up = false;
    final int comp = r.multiply(BIG_INTEGER_TWO).compareTo(q);

    switch (roundMode) {

    // Rounding mode to round away from zero.
    case ROUND_UP:
      up = true;
      break;

    // Rounding mode to round towards zero.
    case ROUND_DOWN:
      up = false;
      break;

    // Rounding mode to round towards positive infinity.
    case ROUND_CEILING:
      up = pos;
      break;

    // Rounding mode to round towards negative infinity.
    case ROUND_FLOOR:
      up = !pos;
      break;

    // Rounding mode to round towards "nearest neighbor" unless both
    // neighbors are equidistant, in which case round up.
    case ROUND_HALF_UP:
      up = (comp >= 0);
      break;

    // Rounding mode to round towards "nearest neighbor" unless both
    // neighbors are equidistant, in which case round down.
    case ROUND_HALF_DOWN:
      up = (comp > 0);
      break;

    case ROUND_HALF_CEILING:
      up = (comp != 0 ? comp > 0 : pos);
      break;

    case ROUND_HALF_FLOOR:
      up = (comp != 0 ? comp > 0 : !pos);
      break;

    // Rounding mode to round towards the "nearest neighbor" unless both
    // neighbors are equidistant, in which case, round towards the even neighbor.
    case ROUND_HALF_EVEN:
      up = (comp != 0 ? comp > 0 : !bigIntegerIsZero(dv.remainder(BIG_INTEGER_TWO)));
      break;

    case ROUND_HALF_ODD:
      up = (comp != 0 ? comp > 0 : bigIntegerIsZero(dv.remainder(BIG_INTEGER_TWO)));
      break;

    // Rounding mode to assert that the requested operation has an exact
    // result, hence no rounding is necessary. If this rounding mode is
    // specified on an operation that yields an inexact result, an
    // ArithmeticException is thrown.
    case ROUND_UNNECESSARY:
      if (!bigIntegerIsZero(r)) {
        throw new ArithmeticException("rounding necessary");
      }
      // [typically not reached due to earlier test for integerp]
      up = false;
      break;

    default:
      throw new IllegalArgumentException("unsupported rounding mode");
    }

    if (up) {
      dv = dv.add(BIG_INTEGER_ONE);
    }

    if (!pos) {
      dv = dv.negate();
    }

    // [rounding step, possible loss of precision step]
    return dv;
  }

  /**
   * Round by default mode (ROUND_HALF_UP).
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public BigRational round() {
    // [rounding step, possible loss of precision step]
    return round(DEFAULT_ROUND_MODE);
  }

  /**
   * Floor, round towards negative infinity.
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public BigRational floor() {
    // [rounding step, possible loss of precision step]
    return round(ROUND_FLOOR);
  }

  /**
   * Ceiling, round towards positive infinity.
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public BigRational ceiling() {
    // [rounding step, possible loss of precision step]
    return round(ROUND_CEILING);
  }

  /**
   * An alias to ceiling().
   * <P>
   * Possible loss of precision.
   */
  // [Name: see class Math.]
  // @PrecisionLoss
  public BigRational ceil() {
    // [possible loss of precision step]
    return ceiling();
  }

  /**
   * Truncate, round towards zero.
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public BigRational truncate() {
    // [rounding step, possible loss of precision step]
    return round(ROUND_DOWN);
  }

  /**
   * An alias to truncate().
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public BigRational trunc() {
    // [possible loss of precision step]
    return truncate();
  }

  /**
   * Integer part.
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public BigRational integerPart() {
    // [rounding step, possible loss of precision step]
    return round(ROUND_DOWN);
  }

  /**
   * Fractional part.
   * <P>
   * Possible loss of precision.
   */
  // @PrecisionLoss
  public BigRational fractionalPart() {
    // this==ip+fp; sign(fp)==sign(this)
    // [possible loss of precision step]
    return subtract(integerPart());
  }

  /**
   * Return an array of BigRationals with both integer and fractional part.
   * <P>
   * Integer part is returned at offset 0; fractional part at offset 1.
   */
  public BigRational[] integerAndFractionalPart() {
    // note: this duplicates fractionalPart() code, for speed.

    final BigRational[] pp = new BigRational[2];
    final BigRational ip = integerPart();
    pp[0] = ip;
    pp[1] = subtract(ip);

    return pp;
  }

  /**
   * Run tests.
   * <P>
   * Implementation is commented out at distribution time; run at development and integration time.
   * <P>
   * Asserts conditions; use '-ea' java startup option to enable asserts.
   */
  public static void test() {

    /*
     * 
     * // implementation can be commented out.
     * 
     * // note: don't use C style comments here, // in order to be able to comment the test code out
     * altogether.
     * 
     * // note: only testing the _public_ interfaces. // typically not testing the aliases.
     * 
     * // BigRational default radix // well, this makes sense, doesn't it? assert
     * (BigRational.DEFAULT_RADIX == 10);
     * 
     * // note: we're not using BigIntegers in these tests
     * 
     * // constructors
     * 
     * // "BigRational zero quotient" try { new BigRational(2, 0); assert (false); } catch
     * (NumberFormatException e) { // empty }
     * 
     * // "BigRational equality"
     * 
     * // long/long ctor assert ((new BigRational(21, 35)).equals(new BigRational(3, 5))); assert
     * ((new BigRational(-21, 35)).equals(new BigRational(-3, 5))); assert ((new BigRational(-21,
     * 35)).equals(new BigRational(3, -5))); assert ((new BigRational(21, -35)).equals(new
     * BigRational(-3, 5))); assert ((new BigRational(-21, -35)).equals(new BigRational(3, 5)));
     * 
     * // long ctor assert ((new BigRational(1)).equals(new BigRational(1))); assert ((new
     * BigRational(0)).equals(new BigRational(0))); assert ((new BigRational(2)).equals(new
     * BigRational(2))); assert ((new BigRational(-1)).equals(new BigRational(-1)));
     * 
     * // string ctors
     * 
     * // "BigRational normalization"
     * 
     * assert ((new BigRational("11")).toString().equals("11")); assert ((new
     * BigRational("-11")).toString().equals("-11")); assert ((new
     * BigRational("+11")).toString().equals("11"));
     * 
     * assert ((new BigRational("21/35")).toString().equals("3/5")); assert ((new
     * BigRational("-21/35")).toString().equals("-3/5")); assert ((new
     * BigRational("21/-35")).toString().equals("-3/5")); // special, but defined assert ((new
     * BigRational("-21/-35")).toString().equals("3/5")); assert ((new
     * BigRational("+21/35")).toString().equals("3/5")); // special, but defined assert ((new
     * BigRational("21/+35")).toString().equals("3/5")); // special, but defined assert ((new
     * BigRational("+21/+35")).toString().equals("3/5"));
     * 
     * // "BigRational special formats" // 1/x assert ((new
     * BigRational("/3")).toString().equals("1/3")); assert ((new
     * BigRational("-/3")).toString().equals("-1/3")); assert ((new
     * BigRational("/-3")).toString().equals("-1/3")); // x/1 assert ((new
     * BigRational("3/")).toString().equals("3")); assert ((new
     * BigRational("-3/")).toString().equals("-3")); assert ((new
     * BigRational("3/-")).toString().equals("-3")); assert ((new
     * BigRational("-3/-")).toString().equals("3")); // special, but defined assert ((new
     * BigRational("/")).toString().equals("1")); assert ((new
     * BigRational("-/")).toString().equals("-1")); assert ((new
     * BigRational("/-")).toString().equals("-1")); // even more special, but defined assert ((new
     * BigRational("-/-")).toString().equals("1"));
     * 
     * // "BigRational normalization" assert ((new BigRational("3.4")).toString().equals("17/5"));
     * assert ((new BigRational("-3.4")).toString().equals("-17/5")); assert ((new
     * BigRational("+3.4")).toString().equals("17/5"));
     * 
     * // "BigRational missing leading/trailing zero" assert ((new
     * BigRational("5.")).toString().equals("5")); assert ((new
     * BigRational("-5.")).toString().equals("-5")); assert ((new
     * BigRational(".7")).toString().equals("7/10")); assert ((new
     * BigRational("-.7")).toString().equals("-7/10")); // special, but defined assert ((new
     * BigRational(".")).toString().equals("0")); assert ((new
     * BigRational("-.")).toString().equals("0")); assert ((new
     * BigRational("-")).toString().equals("0")); // special, but defined assert ((new
     * BigRational("")).toString().equals("0"));
     * 
     * // "BigRational radix" assert ((new BigRational("f/37", 0x10)).toString().equals("3/11"));
     * assert ((new BigRational("f.37", 0x10)).toString().equals("3895/256")); assert ((new
     * BigRational("-dcba.efgh", 23)).toString().equals("-46112938320/279841")); assert ((new
     * BigRational("1011101011010110", 2)).toString(0x10).equals("bad6"));
     * 
     * // illegal radixes try { new BigRational("101", 1); assert (false); } catch
     * (NumberFormatException e) { // empty } try { new BigRational("101", 0); assert (false); }
     * catch (NumberFormatException e) { // empty } try { new BigRational("101", -1); assert
     * (false); } catch (NumberFormatException e) { // empty } try { new BigRational("101", -2);
     * assert (false); } catch (NumberFormatException e) { // empty } try { (new
     * BigRational("33")).toString(1); assert (false); } catch (IllegalArgumentException e) { //
     * empty } try { (new BigRational("33")).toString(0); assert (false); } catch
     * (IllegalArgumentException e) { // empty } try { (new BigRational("33")).toString(-1); assert
     * (false); } catch (IllegalArgumentException e) { // empty } try { (new
     * BigRational("33")).toString(-2); assert (false); } catch (IllegalArgumentException e) { //
     * empty } try { (new BigRational("33")).toStringDot(4, 1); assert (false); } catch
     * (IllegalArgumentException e) { // empty } try { (new
     * BigRational("33")).toStringDotRelative(4, 1); assert (false); } catch
     * (IllegalArgumentException e) { // empty } try { (new BigRational("33")).toStringExponent(4,
     * 1); assert (false); } catch (IllegalArgumentException e) { // empty }
     * 
     * // "BigRational slash and dot" assert ((new BigRational("2.5/3")).toString().equals("5/6"));
     * assert ((new BigRational("2/3.5")).toString().equals("4/7")); assert ((new
     * BigRational("2.5/3.5")).toString().equals("5/7")); assert ((new
     * BigRational("-2.5/3")).toString().equals("-5/6")); assert ((new
     * BigRational("-2/3.5")).toString().equals("-4/7")); assert ((new
     * BigRational("-2.5/3.5")).toString().equals("-5/7")); assert ((new
     * BigRational("2.5/-3")).toString().equals("-5/6")); assert ((new
     * BigRational("2/-3.5")).toString().equals("-4/7")); assert ((new
     * BigRational("2.5/-3.5")).toString().equals("-5/7")); assert ((new
     * BigRational("-2.5/-3")).toString().equals("5/6")); assert ((new
     * BigRational("-2/-3.5")).toString().equals("4/7")); assert ((new
     * BigRational("-2.5/-3.5")).toString().equals("5/7"));
     * 
     * // "BigRational multiple signs, embedded signs" try { new BigRational("+-2/3"); assert
     * (false); } catch (NumberFormatException e) { // empty } try { new BigRational("-+2/3");
     * assert (false); } catch (NumberFormatException e) { // empty } try { new
     * BigRational("++2/3"); assert (false); } catch (NumberFormatException e) { // empty } try {
     * new BigRational("--2/3"); assert (false); } catch (NumberFormatException e) { // empty } try
     * { new BigRational("2-/3"); assert (false); } catch (NumberFormatException e) { // empty } try
     * { new BigRational("2+/3"); assert (false); } catch (NumberFormatException e) { // empty }
     * 
     * // "BigRational sign in fraction" try { new BigRational("2.+3"); assert (false); } catch
     * (NumberFormatException e) { // empty } try { new BigRational("2.-3"); assert (false); } catch
     * (NumberFormatException e) { // empty } try { new BigRational("2.3+"); assert (false); } catch
     * (NumberFormatException e) { // empty } try { new BigRational("2.3-"); assert (false); } catch
     * (NumberFormatException e) { // empty }
     * 
     * // slash nesting try { assert ((new BigRational("2/3/5")).toString().equals("2/15")); assert
     * (false); } catch (NumberFormatException e) { // empty } // assert ((new
     * BigRational("2/3/5/7")).toString().equals("2/105")); // similar tests with sign, dot, etc,
     * left out for the time being
     * 
     * // representations with exponent assert ((new BigRational("1E2")).toString().equals("100"));
     * assert ((new BigRational("1E1")).toString().equals("10")); assert ((new
     * BigRational("1E0")).toString().equals("1")); assert ((new
     * BigRational("1E-1")).toString().equals("1/10")); assert ((new
     * BigRational("1E-2")).toString().equals("1/100")); assert ((new
     * BigRational("-65.4E-3")).toString().equals("-327/5000")); assert ((new
     * BigRational("7.89629601826806E13")).toStringDot(4).equals("78962960182680.6000"));
     * 
     * // special representations with exponent
     * 
     * assert ((new BigRational("1E+1")).toString().equals("10")); assert ((new
     * BigRational("1E+0")).toString().equals("1")); assert ((new
     * BigRational("1E-0")).toString().equals("1")); assert ((new
     * BigRational("1E")).toString().equals("1")); assert ((new
     * BigRational("1E+")).toString().equals("1")); assert ((new
     * BigRational("1E-")).toString().equals("1"));
     * 
     * assert ((new BigRational("77E+1")).toString().equals("770")); assert ((new
     * BigRational("77E+0")).toString().equals("77")); assert ((new
     * BigRational("77E-0")).toString().equals("77")); assert ((new
     * BigRational("77E")).toString().equals("77")); assert ((new
     * BigRational("77E+")).toString().equals("77")); assert ((new
     * BigRational("77E-")).toString().equals("77"));
     * 
     * assert ((new BigRational("E")).toString().equals("1")); assert ((new
     * BigRational("E+1")).toString().equals("10")); assert ((new
     * BigRational("E+0")).toString().equals("1")); assert ((new
     * BigRational("E-0")).toString().equals("1")); assert ((new
     * BigRational("E+")).toString().equals("1")); assert ((new
     * BigRational("E-")).toString().equals("1")); assert ((new
     * BigRational("+E")).toString().equals("1")); assert ((new
     * BigRational("+E+1")).toString().equals("10")); assert ((new
     * BigRational("+E+0")).toString().equals("1")); assert ((new
     * BigRational("+E-0")).toString().equals("1")); assert ((new
     * BigRational("+E+")).toString().equals("1")); assert ((new
     * BigRational("+E-")).toString().equals("1")); assert ((new
     * BigRational("-E")).toString().equals("-1")); assert ((new
     * BigRational("-E+1")).toString().equals("-10")); assert ((new
     * BigRational("-E+0")).toString().equals("-1")); assert ((new
     * BigRational("-E-0")).toString().equals("-1")); assert ((new
     * BigRational("-E+")).toString().equals("-1")); assert ((new
     * BigRational("-E-")).toString().equals("-1"));
     * 
     * assert ((new BigRational("12.34/56.78")).toString().equals("617/2839")); assert ((new
     * BigRational("12.34E1/56.78")).toString().equals("6170/2839")); assert ((new
     * BigRational("12.34/56.78E1")).toString().equals("617/28390")); assert ((new
     * BigRational("12.34E1/56.78E1")).toString().equals("617/2839")); assert ((new
     * BigRational("12.34E-1/56.78")).toString().equals("617/28390")); assert ((new
     * BigRational("12.34/56.78E-1")).toString().equals("6170/2839")); assert ((new
     * BigRational("12.34E-1/56.78E-1")).toString().equals("617/2839")); assert ((new
     * BigRational("-12.34/56.78")).toString().equals("-617/2839")); assert ((new
     * BigRational("-12.34E1/56.78")).toString().equals("-6170/2839")); assert ((new
     * BigRational("-12.34/56.78E1")).toString().equals("-617/28390")); assert ((new
     * BigRational("-12.34E1/56.78E1")).toString().equals("-617/2839")); assert ((new
     * BigRational("-12.34E-1/56.78")).toString().equals("-617/28390")); assert ((new
     * BigRational("-12.34/56.78E-1")).toString().equals("-6170/2839")); assert ((new
     * BigRational("-12.34E-1/56.78E-1")).toString().equals("-617/2839")); assert ((new
     * BigRational("12.34/-56.78")).toString().equals("-617/2839")); assert ((new
     * BigRational("12.34E1/-56.78")).toString().equals("-6170/2839")); assert ((new
     * BigRational("12.34/-56.78E1")).toString().equals("-617/28390")); assert ((new
     * BigRational("12.34E1/-56.78E1")).toString().equals("-617/2839")); assert ((new
     * BigRational("12.34E-1/-56.78")).toString().equals("-617/28390")); assert ((new
     * BigRational("12.34/-56.78E-1")).toString().equals("-6170/2839")); assert ((new
     * BigRational("12.34E-1/-56.78E-1")).toString().equals("-617/2839")); assert ((new
     * BigRational("-12.34/-56.78")).toString().equals("617/2839")); assert ((new
     * BigRational("-12.34E1/-56.78")).toString().equals("6170/2839")); assert ((new
     * BigRational("-12.34/-56.78E1")).toString().equals("617/28390")); assert ((new
     * BigRational("-12.34E1/-56.78E1")).toString().equals("617/2839")); assert ((new
     * BigRational("-12.34E-1/-56.78")).toString().equals("617/28390")); assert ((new
     * BigRational("-12.34/-56.78E-1")).toString().equals("6170/2839")); assert ((new
     * BigRational("-12.34E-1/-56.78E-1")).toString().equals("617/2839"));
     * 
     * // fractional exponent try { new BigRational("1E2.5"); assert (false); } catch
     * (NumberFormatException e) { // empty }
     * 
     * // exponent overflow try { new BigRational("1E" + ((long)Integer.MAX_VALUE * 3)); assert
     * (false); } catch (NumberFormatException e) { // empty }
     * 
     * // exponent nesting try { assert ((new BigRational("1E2E3")).equals((new
     * BigRational(10)).power(2000))); assert (false); } catch (NumberFormatException e) { // empty
     * } // assert ((new BigRational("1E2E3")).equals(new BigRational("1E2000"))); // assert ((new
     * BigRational("-1E2E3")).equals(new BigRational("-1E2000"))); // assert ((new
     * BigRational("1E-2E3")).equals(new BigRational("1E-2000"))); // assert ((new
     * BigRational("-1E-2E3")).equals(new BigRational("-1E-2000"))); // // assert ((new
     * BigRational("1E2E-3")).equals(new BigRational("1E0.002")));
     * 
     * // null try { new BigRational((String)null); assert (false); } catch (NumberFormatException
     * e) { // empty }
     * 
     * // NaN, Infinities try { new BigRational(String.valueOf(Double.NaN)); assert (false); } catch
     * (NumberFormatException e) { // empty } try { new
     * BigRational(String.valueOf(Double.POSITIVE_INFINITY)); assert (false); } catch
     * (NumberFormatException e) { // empty } try { new
     * BigRational(String.valueOf(Double.NEGATIVE_INFINITY)); assert (false); } catch
     * (NumberFormatException e) { // empty }
     * 
     * // clone // [clone() not so much needed, since the BigRational objects are immutable] assert
     * (BigRational.ONE == BigRational.ONE); assert (BigRational.ONE.equals(BigRational.ONE)); try {
     * assert (BigRational.ONE.clone() != BigRational.ONE); assert
     * (BigRational.ONE.clone().equals(BigRational.ONE)); assert
     * (BigRational.ONE.equals(BigRational.ONE.clone())); } catch (CloneNotSupportedException e) {
     * assert (false); } assert (new BigRational(BigRational.ONE) != BigRational.ONE); assert ((new
     * BigRational(BigRational.ONE)).equals(BigRational.ONE)); assert (BigRational.ONE.equals(new
     * BigRational(BigRational.ONE)));
     * 
     * // scaling
     * 
     * // zero scale // "BigRational normalization" assert ((new BigRational(123456, 0,
     * BigRational.DEFAULT_RADIX)).toString().equals("123456")); assert ((new BigRational(123456, 1,
     * BigRational.DEFAULT_RADIX)).toString().equals("61728/5")); assert ((new BigRational(123456,
     * 2, BigRational.DEFAULT_RADIX)).toString().equals("30864/25")); assert ((new
     * BigRational(123456, 5, BigRational.DEFAULT_RADIX)).toString().equals("3858/3125")); // <1
     * assert ((new BigRational(123456, 6,
     * BigRational.DEFAULT_RADIX)).toString().equals("1929/15625")); // negative scale assert ((new
     * BigRational(123456, -1, BigRational.DEFAULT_RADIX)).toString().equals("1234560")); assert
     * ((new BigRational(123456, -2, BigRational.DEFAULT_RADIX)).toString().equals("12345600"));
     * 
     * // "BigRational constants" assert (BigRational.ZERO.toString().equals("0")); assert
     * (BigRational.ONE.toString().equals("1")); assert
     * (BigRational.MINUS_ONE.toString().equals("-1"));
     * 
     * // predicates (isPositive, isNegative, isZero, isOne, isMinusOne, isInteger)
     * 
     * assert (!BigRational.ZERO.isPositive()); assert (!(new BigRational(0)).isPositive()); assert
     * (BigRational.ONE.isPositive()); assert ((new BigRational(1)).isPositive()); assert
     * (!BigRational.MINUS_ONE.isPositive()); assert (!(new BigRational(-1)).isPositive()); assert
     * ((new BigRational(77)).isPositive()); assert (!(new BigRational(-77)).isPositive()); assert
     * ((new BigRational(3, 5)).isPositive()); assert (!(new BigRational(-3, 5)).isPositive());
     * 
     * assert (!BigRational.ZERO.isNegative()); assert (!(new BigRational(0)).isNegative()); assert
     * (!BigRational.ONE.isNegative()); assert (!(new BigRational(1)).isNegative()); assert
     * (BigRational.MINUS_ONE.isNegative()); assert ((new BigRational(-1)).isNegative()); assert
     * (!(new BigRational(77)).isNegative()); assert ((new BigRational(-77)).isNegative()); assert
     * (!(new BigRational(3, 5)).isNegative()); assert ((new BigRational(-3, 5)).isNegative());
     * 
     * assert (BigRational.ZERO.isZero()); assert ((new BigRational(0)).isZero()); assert
     * (!BigRational.ONE.isZero()); assert (!(new BigRational(1)).isZero()); assert
     * (!BigRational.MINUS_ONE.isZero()); assert (!(new BigRational(-1)).isZero()); assert (!(new
     * BigRational(77)).isZero()); assert (!(new BigRational(-77)).isZero()); assert (!(new
     * BigRational(3, 5)).isZero()); assert (!(new BigRational(-3, 5)).isZero());
     * 
     * assert (!BigRational.ZERO.isOne()); assert (!(new BigRational(0)).isOne()); assert
     * (BigRational.ONE.isOne()); assert ((new BigRational(1)).isOne()); assert
     * (!BigRational.MINUS_ONE.isOne()); assert (!(new BigRational(-1)).isOne()); assert (!(new
     * BigRational(77)).isOne()); assert (!(new BigRational(-77)).isOne()); assert (!(new
     * BigRational(3, 5)).isOne()); assert (!(new BigRational(-3, 5)).isOne());
     * 
     * assert (!BigRational.ZERO.isMinusOne()); assert (!(new BigRational(0)).isMinusOne()); assert
     * (!BigRational.ONE.isMinusOne()); assert (!(new BigRational(1)).isMinusOne()); assert
     * (BigRational.MINUS_ONE.isMinusOne()); assert ((new BigRational(-1)).isMinusOne()); assert
     * (!(new BigRational(77)).isMinusOne()); assert (!(new BigRational(-77)).isMinusOne()); assert
     * (!(new BigRational(3, 5)).isMinusOne()); assert (!(new BigRational(-3, 5)).isMinusOne());
     * 
     * assert (BigRational.ZERO.isInteger()); assert ((new BigRational(0)).isInteger()); assert
     * (BigRational.ONE.isInteger()); assert ((new BigRational(1)).isInteger()); assert
     * (BigRational.MINUS_ONE.isInteger()); assert ((new BigRational(-1)).isInteger()); assert ((new
     * BigRational(77)).isInteger()); assert ((new BigRational(-77)).isInteger()); assert (!(new
     * BigRational(3, 5)).isInteger()); assert (!(new BigRational(-3, 5)).isInteger());
     * 
     * // "BigRational string dot"
     * 
     * assert ((new BigRational("1234.5678")).toStringDot(4).equals("1234.5678")); assert ((new
     * BigRational("1234.5678")).toStringDot(5).equals("1234.56780")); assert ((new
     * BigRational("1234.5678")).toStringDot(6).equals("1234.567800")); assert ((new
     * BigRational("1234.5678")).toStringDot(3).equals("1234.568")); assert ((new
     * BigRational("1234.5678")).toStringDot(2).equals("1234.57")); assert ((new
     * BigRational("1234.5678")).toStringDot(1).equals("1234.6")); assert ((new
     * BigRational("1234.5678")).toStringDot(0).equals("1235")); assert ((new
     * BigRational("1234.5678")).toStringDot(-1).equals("1230")); assert ((new
     * BigRational("1234.5678")).toStringDot(-2).equals("1200")); assert ((new
     * BigRational("1234.5678")).toStringDot(-3).equals("1000")); assert ((new
     * BigRational("1234.5678")).toStringDot(-4).equals("0")); assert ((new
     * BigRational("1234.5678")).toStringDot(-5).equals("0")); assert ((new
     * BigRational("1234.5678")).toStringDot(-6).equals("0"));
     * 
     * assert ((new BigRational("8765.4321")).toStringDot(-2).equals("8800")); assert ((new
     * BigRational("0.0148")).toStringDot(6).equals("0.014800")); assert ((new
     * BigRational("0.0148")).toStringDot(4).equals("0.0148")); assert ((new
     * BigRational("0.0148")).toStringDot(3).equals("0.015")); assert ((new
     * BigRational("0.0148")).toStringDot(2).equals("0.01")); assert ((new
     * BigRational("0.0148")).toStringDot(1).equals("0.0")); assert ((new
     * BigRational("0.001")).toStringDot(4).equals("0.0010")); assert ((new
     * BigRational("0.001")).toStringDot(3).equals("0.001")); assert ((new
     * BigRational("0.001")).toStringDot(2).equals("0.00")); assert ((new
     * BigRational("0.001")).toStringDot(1).equals("0.0")); assert ((new
     * BigRational("0.001")).toStringDot(0).equals("0")); assert ((new
     * BigRational("0.001")).toStringDot(-1).equals("0")); assert ((new
     * BigRational("0.001")).toStringDot(-2).equals("0"));
     * 
     * assert ((new BigRational("-1234.5678")).toStringDot(4).equals("-1234.5678")); assert ((new
     * BigRational("-1234.5678")).toStringDot(5).equals("-1234.56780")); assert ((new
     * BigRational("-1234.5678")).toStringDot(6).equals("-1234.567800")); assert ((new
     * BigRational("-1234.5678")).toStringDot(3).equals("-1234.568")); assert ((new
     * BigRational("-1234.5678")).toStringDot(2).equals("-1234.57")); assert ((new
     * BigRational("-1234.5678")).toStringDot(1).equals("-1234.6")); assert ((new
     * BigRational("-1234.5678")).toStringDot(0).equals("-1235")); assert ((new
     * BigRational("-1234.5678")).toStringDot(-1).equals("-1230")); assert ((new
     * BigRational("-1234.5678")).toStringDot(-2).equals("-1200")); assert ((new
     * BigRational("-1234.5678")).toStringDot(-3).equals("-1000")); assert ((new
     * BigRational("-1234.5678")).toStringDot(-4).equals("0")); assert ((new
     * BigRational("-1234.5678")).toStringDot(-5).equals("0"));
     * 
     * assert ((new BigRational("0.0")).toStringDot(4).equals("0.0000")); assert ((new
     * BigRational("0.0")).toStringDot(3).equals("0.000")); assert ((new
     * BigRational("0.0")).toStringDot(2).equals("0.00")); assert ((new
     * BigRational("0.0")).toStringDot(1).equals("0.0")); assert ((new
     * BigRational("0.0")).toStringDot(0).equals("0")); assert ((new
     * BigRational("0.0")).toStringDot(-1).equals("0")); assert ((new
     * BigRational("0.0")).toStringDot(-2).equals("0")); assert ((new
     * BigRational("0.0")).toStringDot(-3).equals("0")); assert ((new
     * BigRational("0.0")).toStringDot(-4).equals("0"));
     * 
     * // "BigRational string dot radix" assert ((new BigRational("1234.5678", 20)).toStringDot(3,
     * 20).equals("1234.567")); assert ((new BigRational("abcd.5b7g", 20)).toStringDot(-2,
     * 20).equals("ac00"));
     * 
     * // logarithm; rounded floor // commented out since method is not public // assert ((new
     * BigRational("1")).logarithm(10) == 0); // assert ((new BigRational("1.0001")).logarithm(10)
     * == 0); // assert ((new BigRational("1.1")).logarithm(10) == 0); // assert ((new
     * BigRational("2")).logarithm(10) == 0); // assert ((new BigRational("5")).logarithm(10) == 0);
     * // assert ((new BigRational("9.999")).logarithm(10) == 0); // assert ((new
     * BigRational("10")).logarithm(10) == 1); // assert ((new BigRational("10.0001")).logarithm(10)
     * == 1); // assert ((new BigRational("99")).logarithm(10) == 1); // assert ((new
     * BigRational("100")).logarithm(10) == 2); // assert ((new BigRational("999")).logarithm(10) ==
     * 2); // assert ((new BigRational("1000")).logarithm(10) == 3); // assert ((new
     * BigRational(".5")).logarithm(10) == -1); // assert ((new BigRational(".2")).logarithm(10) ==
     * -1); // assert ((new BigRational(".10001")).logarithm(10) == -1); // assert ((new
     * BigRational(".1")).logarithm(10) == -2); // assert ((new BigRational(".0999")).logarithm(10)
     * == -2); // assert ((new BigRational(".010001")).logarithm(10) == -2); // assert ((new
     * BigRational(".01")).logarithm(10) == -3); // assert ((new
     * BigRational(".00999")).logarithm(10) == -3); // assert ((new
     * BigRational(".001")).logarithm(10) == -4);
     * 
     * // assert ((new BigRational("1")).logarithm(5) == 0); // assert ((new
     * BigRational("1.0001")).logarithm(5) == 0); // assert ((new BigRational("1.1")).logarithm(5)
     * == 0); // assert ((new BigRational("2")).logarithm(5) == 0); // assert ((new
     * BigRational("4")).logarithm(5) == 0); // assert ((new BigRational("4.999")).logarithm(5) ==
     * 0); // assert ((new BigRational("5")).logarithm(5) == 1); // assert ((new
     * BigRational("5.0001")).logarithm(5) == 1); // assert ((new BigRational("24")).logarithm(5) ==
     * 1); // assert ((new BigRational("25")).logarithm(5) == 2); // assert ((new
     * BigRational("124")).logarithm(5) == 2); // assert ((new BigRational("125")).logarithm(5) ==
     * 3); // assert ((new BigRational(".5")).logarithm(5) == -1); // assert ((new
     * BigRational(".3")).logarithm(5) == -1); // assert ((new BigRational(".20001")).logarithm(5)
     * == -1); // assert ((new BigRational(".2")).logarithm(5) == -2); // assert ((new
     * BigRational(".1999")).logarithm(5) == -2); // assert ((new
     * BigRational(".040001")).logarithm(5) == -2); // assert ((new BigRational(".04")).logarithm(5)
     * == -3); // assert ((new BigRational(".03999")).logarithm(5) == -3); // assert ((new
     * BigRational(".008")).logarithm(5) == -4);
     * 
     * // assert (BigRational.valueOf("2E0").logarithm(10) == 0); // assert
     * (BigRational.valueOf("2E1").logarithm(10) == 1); // assert
     * (BigRational.valueOf("2E2").logarithm(10) == 2); // assert
     * (BigRational.valueOf("2E20").logarithm(10) == 20); // assert
     * (BigRational.valueOf("2E200").logarithm(10) == 200);
     * 
     * // assert (ilog2(1) == 0); // assert (ilog2(2) == 1); // assert (ilog2(3) == 1); // assert
     * (ilog2(4) == 2); // assert (ilog2(5) == 2); // assert (ilog2(6) == 2); // assert (ilog2(7) ==
     * 2); // assert (ilog2(8) == 3);
     * 
     * // stringDotRelative
     * 
     * assert ((new BigRational("1234.5678")).toStringDotRelative(8).equals("1234.5678")); assert
     * ((new BigRational("1234.5678")).toStringDotRelative(9).equals("1234.56780")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(10).equals("1234.567800")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(7).equals("1234.568")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(6).equals("1234.57")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(5).equals("1234.6")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(4).equals("1235")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(3).equals("1230")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(2).equals("1200")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(1).equals("1000")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(0).equals("0")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(-1).equals("0")); assert ((new
     * BigRational("1234.5678")).toStringDotRelative(-2).equals("0"));
     * 
     * assert ((new BigRational("-1234.5678")).toStringDotRelative(8).equals("-1234.5678")); assert
     * ((new BigRational("-1234.5678")).toStringDotRelative(9).equals("-1234.56780")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(10).equals("-1234.567800")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(7).equals("-1234.568")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(6).equals("-1234.57")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(5).equals("-1234.6")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(4).equals("-1235")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(3).equals("-1230")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(2).equals("-1200")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(1).equals("-1000")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(0).equals("0")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(-1).equals("0")); assert ((new
     * BigRational("-1234.5678")).toStringDotRelative(-2).equals("0"));
     * 
     * assert ((new BigRational("0.00012345678")).toStringDotRelative(8).equals("0.00012345678"));
     * assert ((new BigRational("0.00012345678")).toStringDotRelative(9).equals("0.000123456780"));
     * assert ((new
     * BigRational("0.00012345678")).toStringDotRelative(10).equals("0.0001234567800")); assert
     * ((new BigRational("0.00012345678")).toStringDotRelative(7).equals("0.0001234568")); assert
     * ((new BigRational("0.00012345678")).toStringDotRelative(6).equals("0.000123457")); assert
     * ((new BigRational("0.00012345678")).toStringDotRelative(5).equals("0.00012346")); assert
     * ((new BigRational("0.00012345678")).toStringDotRelative(4).equals("0.0001235")); assert ((new
     * BigRational("0.00012345678")).toStringDotRelative(3).equals("0.000123")); assert ((new
     * BigRational("0.00012345678")).toStringDotRelative(2).equals("0.00012")); assert ((new
     * BigRational("0.00012345678")).toStringDotRelative(1).equals("0.0001")); assert ((new
     * BigRational("0.00012345678")).toStringDotRelative(0).equals("0")); assert ((new
     * BigRational("0.00012345678")).toStringDotRelative(-1).equals("0")); assert ((new
     * BigRational("0.00012345678")).toStringDotRelative(-2).equals("0"));
     * 
     * assert ((new BigRational("-0.00012345678")).toStringDotRelative(8).equals("-0.00012345678"));
     * assert ((new
     * BigRational("-0.00012345678")).toStringDotRelative(9).equals("-0.000123456780")); assert
     * ((new BigRational("-0.00012345678")).toStringDotRelative(10).equals("-0.0001234567800"));
     * assert ((new BigRational("-0.00012345678")).toStringDotRelative(7).equals("-0.0001234568"));
     * assert ((new BigRational("-0.00012345678")).toStringDotRelative(6).equals("-0.000123457"));
     * assert ((new BigRational("-0.00012345678")).toStringDotRelative(5).equals("-0.00012346"));
     * assert ((new BigRational("-0.00012345678")).toStringDotRelative(4).equals("-0.0001235"));
     * assert ((new BigRational("-0.00012345678")).toStringDotRelative(3).equals("-0.000123"));
     * assert ((new BigRational("-0.00012345678")).toStringDotRelative(2).equals("-0.00012"));
     * assert ((new BigRational("-0.00012345678")).toStringDotRelative(1).equals("-0.0001")); assert
     * ((new BigRational("-0.00012345678")).toStringDotRelative(0).equals("0")); assert ((new
     * BigRational("-0.00012345678")).toStringDotRelative(-1).equals("0")); assert ((new
     * BigRational("-0.00012345678")).toStringDotRelative(-2).equals("0"));
     * 
     * assert ((new BigRational("8765.4321")).toStringDotRelative(8).equals("8765.4321")); assert
     * ((new BigRational("8765.4321")).toStringDotRelative(9).equals("8765.43210")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(10).equals("8765.432100")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(7).equals("8765.432")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(6).equals("8765.43")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(5).equals("8765.4")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(4).equals("8765")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(3).equals("8770")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(2).equals("8800")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(1).equals("9000")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(0).equals("0")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(-1).equals("0")); assert ((new
     * BigRational("8765.4321")).toStringDotRelative(-2).equals("0"));
     * 
     * assert ((new BigRational("0.0")).toStringDotRelative(4).equals("0")); assert ((new
     * BigRational("0.0")).toStringDotRelative(3).equals("0")); assert ((new
     * BigRational("0.0")).toStringDotRelative(2).equals("0")); assert ((new
     * BigRational("0.0")).toStringDotRelative(1).equals("0")); assert ((new
     * BigRational("0.0")).toStringDotRelative(0).equals("0")); assert ((new
     * BigRational("0.0")).toStringDotRelative(-1).equals("0")); assert ((new
     * BigRational("0.0")).toStringDotRelative(-2).equals("0")); assert ((new
     * BigRational("0.0")).toStringDotRelative(-3).equals("0")); assert ((new
     * BigRational("0.0")).toStringDotRelative(-4).equals("0"));
     * 
     * assert ((new BigRational(1499)).toStringDotRelative(1).equals("1000")); assert ((new
     * BigRational(1500)).toStringDotRelative(1).equals("2000")); assert ((new
     * BigRational(1501)).toStringDotRelative(1).equals("2000")); assert ((new
     * BigRational(1001)).toStringDotRelative(1).equals("1000")); assert ((new
     * BigRational(1000)).toStringDotRelative(1).equals("1000")); assert ((new
     * BigRational(999)).toStringDotRelative(1).equals("1000")); assert ((new
     * BigRational(951)).toStringDotRelative(1).equals("1000")); assert ((new
     * BigRational(950)).toStringDotRelative(1).equals("1000")); assert ((new
     * BigRational(949)).toStringDotRelative(1).equals("900"));
     * 
     * // check inverted radix power cases too assert ((new BigRational(1,
     * 1)).toStringDotRelative(1).equals("1")); assert ((new BigRational(1,
     * 2)).toStringDotRelative(1).equals("0.5")); assert ((new BigRational(1,
     * 3)).toStringDotRelative(1).equals("0.3")); assert ((new BigRational(1,
     * 5)).toStringDotRelative(1).equals("0.2")); assert ((new BigRational(1,
     * 10)).toStringDotRelative(1).equals("0.1")); assert ((new BigRational(1,
     * 20)).toStringDotRelative(1).equals("0.05")); assert ((new BigRational(1,
     * 30)).toStringDotRelative(1).equals("0.03")); assert ((new BigRational(1,
     * 50)).toStringDotRelative(1).equals("0.02")); assert ((new BigRational(1,
     * 100)).toStringDotRelative(1).equals("0.01")); assert ((new BigRational(1,
     * 200)).toStringDotRelative(1).equals("0.005")); assert ((new BigRational(1,
     * 300)).toStringDotRelative(1).equals("0.003")); assert ((new BigRational(1,
     * 500)).toStringDotRelative(1).equals("0.002")); assert ((new BigRational(1,
     * 1000)).toStringDotRelative(1).equals("0.001")); assert ((new BigRational(1,
     * 4)).toStringDotRelative(1).equals("0.3")); assert ((new BigRational(1,
     * 11)).toStringDotRelative(1).equals("0.09")); assert ((new BigRational(1,
     * 12)).toStringDotRelative(1).equals("0.08")); assert ((new BigRational(1,
     * 31)).toStringDotRelative(1).equals("0.03")); assert ((new BigRational(1,
     * 99)).toStringDotRelative(1).equals("0.01")); assert ((new
     * BigRational("0.0100001")).toStringDotRelative(1).equals("0.01"));
     * 
     * // intermediately excess zeros assert ((new BigRational(1,
     * 101)).toStringDotRelative(1).equals("0.01")); assert ((new
     * BigRational("0.0099999")).toStringDotRelative(1).equals("0.01"));
     * 
     * // exponent representation
     * 
     * assert ((new BigRational("1234.5678")).toStringExponent(8).equals("1.2345678E3")); assert
     * ((new BigRational("1234.5678")).toStringExponent(4).equals("1.235E3")); assert ((new
     * BigRational("1234.5678")).toStringExponent(2).equals("1.2E3")); assert ((new
     * BigRational("1234.5678")).toStringExponent(1).equals("1E3")); assert ((new
     * BigRational("1234.5678")).toStringExponent(0).equals("0")); assert ((new
     * BigRational("0.00012345678")).toStringExponent(8).equals("1.2345678E-4")); assert ((new
     * BigRational("0.00012345678")).toStringExponent(4).equals("1.235E-4")); assert ((new
     * BigRational("0.00012345678")).toStringExponent(2).equals("1.2E-4")); assert ((new
     * BigRational("0.00012345678")).toStringExponent(1).equals("1E-4")); assert ((new
     * BigRational("0.00012345678")).toStringExponent(0).equals("0"));
     * 
     * assert ((new BigRational("-1234.5678")).toStringExponent(8).equals("-1.2345678E3")); assert
     * ((new BigRational("-1234.5678")).toStringExponent(4).equals("-1.235E3")); assert ((new
     * BigRational("-1234.5678")).toStringExponent(2).equals("-1.2E3")); assert ((new
     * BigRational("-1234.5678")).toStringExponent(1).equals("-1E3")); assert ((new
     * BigRational("-1234.5678")).toStringExponent(0).equals("0")); assert ((new
     * BigRational("-0.00012345678")).toStringExponent(8).equals("-1.2345678E-4")); assert ((new
     * BigRational("-0.00012345678")).toStringExponent(4).equals("-1.235E-4")); assert ((new
     * BigRational("-0.00012345678")).toStringExponent(2).equals("-1.2E-4")); assert ((new
     * BigRational("-0.00012345678")).toStringExponent(1).equals("-1E-4")); assert ((new
     * BigRational("-0.00012345678")).toStringExponent(0).equals("0"));
     * 
     * assert ((new BigRational("1234")).toStringExponent(8).equals("1.234E3")); assert ((new
     * BigRational("1234")).toStringExponent(4).equals("1.234E3")); assert ((new
     * BigRational("1234")).toStringExponent(2).equals("1.2E3")); assert ((new
     * BigRational("1234")).toStringExponent(1).equals("1E3")); assert ((new
     * BigRational("1234")).toStringExponent(0).equals("0"));
     * 
     * assert ((new BigRational("10")).toStringExponent(6).equals("1E1")); assert ((new
     * BigRational("1")).toStringExponent(6).equals("1")); assert ((new
     * BigRational("0")).toStringExponent(6).equals("0")); assert ((new
     * BigRational("0.1")).toStringExponent(6).equals("1E-1")); assert ((new
     * BigRational("10")).toStringExponent(1).equals("1E1")); assert ((new
     * BigRational("1")).toStringExponent(1).equals("1")); assert ((new
     * BigRational("0")).toStringExponent(1).equals("0")); assert ((new
     * BigRational("0.1")).toStringExponent(1).equals("1E-1"));
     * 
     * assert (BigRational.valueOf("2E222").toStringExponent(4).equals("2E222")); assert
     * (BigRational.valueOf("-2E222").toStringExponent(4).equals("-2E222")); assert
     * (BigRational.valueOf("2E-222").toStringExponent(4).equals("2E-222")); assert
     * (BigRational.valueOf("-2E-222").toStringExponent(4).equals("-2E-222"));
     * 
     * // non-default radix assert (new BigRational("2E222", 5).toStringExponent(4,
     * 5).equals("2E222")); assert (new BigRational("-2E222", 5).toStringExponent(4,
     * 5).equals("-2E222")); assert (new BigRational("2E-222", 5).toStringExponent(4,
     * 5).equals("2E-222")); assert (new BigRational("-2E-222", 5).toStringExponent(4,
     * 5).equals("-2E-222"));
     * 
     * // note: some of the arithmetic long forms not tested as well.
     * 
     * // "BigRational add" assert ((new BigRational(3, 5)).add(new BigRational(7,
     * 11)).toString().equals("68/55")); assert ((new BigRational(3, 5)).add(new BigRational(-7,
     * 11)).toString().equals("-2/55")); assert ((new BigRational(-3, 5)).add(new BigRational(7,
     * 11)).toString().equals("2/55")); assert ((new BigRational(-3, 5)).add(new BigRational(-7,
     * 11)).toString().equals("-68/55")); // same denominator assert ((new BigRational(3,
     * 5)).add(new BigRational(1, 5)).toString().equals("4/5")); // with integers assert ((new
     * BigRational(3, 5)).add(new BigRational(1)).toString().equals("8/5")); assert ((new
     * BigRational(2)).add(new BigRational(3, 5)).toString().equals("13/5")); // zero assert ((new
     * BigRational(3, 5)).add(BigRational.ZERO).toString().equals("3/5")); assert
     * (BigRational.ZERO.add(new BigRational(3, 5)).toString().equals("3/5"));
     * 
     * // "BigRational subtract" assert ((new BigRational(3, 5)).subtract(new BigRational(7,
     * 11)).toString().equals("-2/55")); assert ((new BigRational(3, 5)).subtract(new
     * BigRational(-7, 11)).toString().equals("68/55")); assert ((new BigRational(-3,
     * 5)).subtract(new BigRational(7, 11)).toString().equals("-68/55")); assert ((new
     * BigRational(-3, 5)).subtract(new BigRational(-7, 11)).toString().equals("2/55")); // same
     * denominator assert ((new BigRational(3, 5)).subtract(new BigRational(1,
     * 5)).toString().equals("2/5")); // with integers assert ((new BigRational(3, 5)).subtract(new
     * BigRational(1)).toString().equals("-2/5")); assert ((new BigRational(2)).subtract(new
     * BigRational(3, 5)).toString().equals("7/5")); // zero assert ((new BigRational(3,
     * 5)).subtract(BigRational.ZERO).toString().equals("3/5")); assert
     * (BigRational.ZERO.subtract(new BigRational(3, 5)).toString().equals("-3/5"));
     * 
     * // normalization, e.g after subtract // "BigRational normalization" assert ((new
     * BigRational(7, 5)).subtract(new BigRational(2, 5)).compareTo(1) == 0); assert ((new
     * BigRational(7, 5)).subtract(new BigRational(7, 5)).compareTo(0) == 0); assert ((new
     * BigRational(7, 5)).subtract(new BigRational(12, 5)).compareTo(-1) == 0);
     * 
     * // "BigRational multiply" assert ((new BigRational(3, 5)).multiply(new BigRational(7,
     * 11)).toString().equals("21/55")); assert ((new BigRational(3, 5)).multiply(new
     * BigRational(-7, 11)).toString().equals("-21/55")); assert ((new BigRational(-3,
     * 5)).multiply(new BigRational(7, 11)).toString().equals("-21/55")); assert ((new
     * BigRational(-3, 5)).multiply(new BigRational(-7, 11)).toString().equals("21/55")); assert
     * ((new BigRational(3, 5)).multiply(7).toString().equals("21/5")); assert ((new BigRational(-3,
     * 5)).multiply(7).toString().equals("-21/5")); assert ((new BigRational(3,
     * 5)).multiply(-7).toString().equals("-21/5")); assert ((new BigRational(-3,
     * 5)).multiply(-7).toString().equals("21/5"));
     * 
     * // multiply() with integers, 0, etc. (some repetitions too) // "BigRational multiply" assert
     * ((new BigRational(3, 5)).multiply(new BigRational(7, 1)).toString().equals("21/5")); assert
     * ((new BigRational(3, 5)).multiply(new BigRational(1, 7)).toString().equals("3/35")); assert
     * ((new BigRational(3, 1)).multiply(new BigRational(7, 11)).toString().equals("21/11")); assert
     * ((new BigRational(3, 5)).multiply(new BigRational(0)).toString().equals("0")); assert ((new
     * BigRational(0)).multiply(new BigRational(3, 5)).toString().equals("0")); assert ((new
     * BigRational(3, 5)).multiply(new BigRational(1)).toString().equals("3/5")); assert ((new
     * BigRational(3, 5)).multiply(new BigRational(-1)).toString().equals("-3/5")); assert ((new
     * BigRational(3, 5)).multiply(new BigRational(-1, 3)).toString().equals("-1/5"));
     * 
     * // special cases (zeroing, negating) assert
     * (BigRational.ZERO.multiply(BigRational.ZERO).equals(BigRational.ZERO)); assert
     * (BigRational.ZERO.multiply(BigRational.ONE).equals(BigRational.ZERO)); assert
     * (BigRational.ONE.multiply(BigRational.ZERO).equals(BigRational.ZERO)); assert
     * (BigRational.ZERO.multiply(BigRational.MINUS_ONE).equals(BigRational.ZERO)); assert
     * (BigRational.MINUS_ONE.multiply(BigRational.ZERO).equals(BigRational.ZERO)); assert
     * (BigRational.ZERO.multiply(new BigRational(5)).equals(BigRational.ZERO)); assert ((new
     * BigRational(5)).multiply(BigRational.ZERO).equals(BigRational.ZERO)); assert
     * (BigRational.ZERO.multiply(new BigRational(-5)).equals(BigRational.ZERO)); assert ((new
     * BigRational(-5)).multiply(BigRational.ZERO).equals(BigRational.ZERO)); assert
     * (BigRational.ONE.multiply(BigRational.ONE).equals(BigRational.ONE)); assert
     * (BigRational.ONE.multiply(BigRational.MINUS_ONE).equals(BigRational.MINUS_ONE)); assert
     * (BigRational.MINUS_ONE.multiply(BigRational.ONE).equals(BigRational.MINUS_ONE)); assert
     * (BigRational.ONE.multiply(new BigRational(5)).equals(new BigRational(5))); assert ((new
     * BigRational(5)).multiply(BigRational.ONE).equals(new BigRational(5))); assert
     * (BigRational.ONE.multiply(new BigRational(-5)).equals(new BigRational(-5))); assert ((new
     * BigRational(-5)).multiply(BigRational.ONE).equals(new BigRational(-5))); assert
     * (BigRational.MINUS_ONE.multiply(BigRational.MINUS_ONE).equals(BigRational.ONE)); assert
     * (BigRational.MINUS_ONE.multiply(new BigRational(5)).equals(new BigRational(-5))); assert
     * ((new BigRational(5)).multiply(BigRational.MINUS_ONE).equals(new BigRational(-5))); assert
     * (BigRational.MINUS_ONE.multiply(new BigRational(-5)).equals(new BigRational(5))); assert
     * ((new BigRational(-5)).multiply(BigRational.MINUS_ONE).equals(new BigRational(5)));
     * 
     * // "BigRational divide" assert ((new BigRational(3, 5)).divide(new BigRational(7,
     * 11)).toString().equals("33/35")); assert ((new BigRational(3, 5)).divide(new BigRational(-7,
     * 11)).toString().equals("-33/35")); assert ((new BigRational(-3, 5)).divide(new BigRational(7,
     * 11)).toString().equals("-33/35")); assert ((new BigRational(-3, 5)).divide(new
     * BigRational(-7, 11)).toString().equals("33/35")); assert ((new BigRational(3,
     * 5)).divide(7).toString().equals("3/35")); assert ((new BigRational(-3,
     * 5)).divide(7).toString().equals("-3/35")); assert ((new BigRational(3,
     * 5)).divide(-7).toString().equals("-3/35")); assert ((new BigRational(-3,
     * 5)).divide(-7).toString().equals("3/35"));
     * 
     * assert ((new BigRational(0)).divide(new BigRational(7, 11)).toString().equals("0"));
     * 
     * // "BigRational divide" try { (new BigRational(3, 5)).divide(new BigRational(0)); assert
     * (false); } catch (ArithmeticException e) { // empty } try { (new BigRational(-3,
     * 5)).divide(new BigRational(0)); assert (false); } catch (ArithmeticException e) { // empty }
     * try { (new BigRational(3, 5)).divide(0); assert (false); } catch (ArithmeticException e) { //
     * empty } try { (new BigRational(-3, 5)).divide(0); assert (false); } catch
     * (ArithmeticException e) { // empty }
     * 
     * try { (new BigRational(0)).divide(0); assert (false); } catch (ArithmeticException e) { //
     * empty }
     * 
     * // "BigRational power" assert ((new BigRational(3,
     * 5)).power(7).toString().equals("2187/78125")); assert ((new BigRational(-3,
     * 5)).power(7).toString().equals("-2187/78125")); assert ((new BigRational(3,
     * 5)).power(-7).toString().equals("78125/2187")); assert ((new BigRational(-3,
     * 5)).power(-7).toString().equals("-78125/2187")); assert ((new BigRational(3,
     * 5)).power(6).toString().equals("729/15625")); assert ((new BigRational(-3,
     * 5)).power(6).toString().equals("729/15625")); assert ((new BigRational(3,
     * 5)).power(0).toString().equals("1")); assert ((new BigRational(-3,
     * 5)).power(0).toString().equals("1")); assert ((new
     * BigRational(0)).power(1).toString().equals("0")); assert ((new
     * BigRational(1)).power(0).toString().equals("1"));
     * 
     * assert ((new BigRational(3, 5)).power(0).equals(BigRational.ONE)); assert ((new
     * BigRational(3, 5)).power(1).equals(new BigRational(3, 5))); assert ((new BigRational(3,
     * 5)).power(-1).equals((new BigRational(3, 5)).invert())); assert ((new
     * BigRational(2)).power(2).equals(new BigRational(4)));
     * 
     * // special cases assert (BigRational.ZERO.power(1).equals(BigRational.ZERO)); assert
     * (BigRational.ZERO.power(2).equals(BigRational.ZERO)); assert
     * (BigRational.ZERO.power(3).equals(BigRational.ZERO)); assert
     * (BigRational.ZERO.power(4).equals(BigRational.ZERO)); assert
     * (BigRational.ONE.power(0).equals(BigRational.ONE)); assert
     * (BigRational.ONE.power(1).equals(BigRational.ONE)); assert
     * (BigRational.ONE.power(2).equals(BigRational.ONE)); assert
     * (BigRational.ONE.power(3).equals(BigRational.ONE)); assert
     * (BigRational.ONE.power(4).equals(BigRational.ONE)); assert
     * (BigRational.ONE.power(-1).equals(BigRational.ONE)); assert
     * (BigRational.ONE.power(-2).equals(BigRational.ONE)); assert
     * (BigRational.ONE.power(-3).equals(BigRational.ONE)); assert
     * (BigRational.ONE.power(-4).equals(BigRational.ONE)); assert
     * (BigRational.MINUS_ONE.power(0).equals(BigRational.ONE)); assert
     * (BigRational.MINUS_ONE.power(1).equals(BigRational.MINUS_ONE)); assert
     * (BigRational.MINUS_ONE.power(2).equals(BigRational.ONE)); assert
     * (BigRational.MINUS_ONE.power(3).equals(BigRational.MINUS_ONE)); assert
     * (BigRational.MINUS_ONE.power(4).equals(BigRational.ONE)); assert
     * (BigRational.MINUS_ONE.power(-1).equals(BigRational.MINUS_ONE)); assert
     * (BigRational.MINUS_ONE.power(-2).equals(BigRational.ONE)); assert
     * (BigRational.MINUS_ONE.power(-3).equals(BigRational.MINUS_ONE)); assert
     * (BigRational.MINUS_ONE.power(-4).equals(BigRational.ONE));
     * 
     * // "BigRational zeroth power of zero" try { (new BigRational(0)).power(0); assert (false); }
     * catch (ArithmeticException e) { // empty } try { BigRational.ZERO.power(-1); assert (false);
     * } catch (ArithmeticException e) { // empty } try { BigRational.ZERO.power(-2); assert
     * (false); } catch (ArithmeticException e) { // empty }
     * 
     * // "BigRational remainder" assert ((new BigRational(5)).remainder(new
     * BigRational(3)).equals(new BigRational(2))); assert ((new BigRational(-5)).remainder(new
     * BigRational(3)).equals(new BigRational(-2))); assert ((new BigRational(5)).remainder(new
     * BigRational(-3)).equals(new BigRational(2))); assert ((new BigRational(-5)).remainder(new
     * BigRational(-3)).equals(new BigRational(-2))); assert ((new BigRational(0)).remainder(new
     * BigRational(1)).equals(new BigRational(0))); assert ((new BigRational("5.6")).remainder(new
     * BigRational("1.8")).equals(new BigRational(1, 5))); assert ((new
     * BigRational("-5.6")).remainder(new BigRational("1.8")).equals(new BigRational(-1, 5)));
     * assert ((new BigRational("5.6")).remainder(new BigRational("-1.8")).equals(new BigRational(1,
     * 5))); assert ((new BigRational("-5.6")).remainder(new BigRational("-1.8")).equals(new
     * BigRational(-1, 5))); assert ((new BigRational("1")).remainder(new
     * BigRational("0.13")).equals(new BigRational("0.09")));
     * 
     * try { BigRational.ONE.remainder(0); assert (false); } catch (ArithmeticException e) { //
     * empty } try { BigRational.ZERO.remainder(0); assert (false); } catch (ArithmeticException e)
     * { // empty }
     * 
     * // "BigRational modulus" assert ((new BigRational(5)).modulus(new BigRational(3)).equals(new
     * BigRational(2))); assert ((new BigRational(-5)).modulus(new BigRational(3)).equals(new
     * BigRational(1))); assert ((new BigRational(5)).modulus(new BigRational(-3)).equals(new
     * BigRational(-1))); assert ((new BigRational(-5)).modulus(new BigRational(-3)).equals(new
     * BigRational(-2))); assert ((new BigRational(0)).modulus(new BigRational(1)).equals(new
     * BigRational(0))); assert ((new BigRational("5.6")).modulus(new BigRational("1.8")).equals(new
     * BigRational(1, 5))); assert ((new BigRational("-5.6")).modulus(new
     * BigRational("1.8")).equals(new BigRational(8, 5))); assert ((new
     * BigRational("5.6")).modulus(new BigRational("-1.8")).equals(new BigRational(-8, 5))); assert
     * ((new BigRational("-5.6")).modulus(new BigRational("-1.8")).equals(new BigRational(-1, 5)));
     * assert ((new BigRational("1")).modulus(new BigRational("0.13")).equals(new
     * BigRational("0.09")));
     * 
     * try { BigRational.ONE.modulus(0); assert (false); } catch (ArithmeticException e) { // empty
     * } try { BigRational.ZERO.modulus(0); assert (false); } catch (ArithmeticException e) { //
     * empty }
     * 
     * // "BigRational signum" assert ((new BigRational(0)).signum() == 0); assert ((new
     * BigRational(1)).signum() == 1); assert ((new BigRational(-1)).signum() == -1); assert ((new
     * BigRational(2)).signum() == 1); assert ((new BigRational(-2)).signum() == -1); assert ((new
     * BigRational(3, 5)).signum() == 1); assert ((new BigRational(-3, 5)).signum() == -1);
     * 
     * // "BigRational absolute" assert ((new BigRational(0)).absolute().toString().equals("0"));
     * assert ((new BigRational(3, 5)).absolute().toString().equals("3/5")); assert ((new
     * BigRational(-3, 5)).absolute().toString().equals("3/5")); assert ((new
     * BigRational(1)).absolute().toString().equals("1")); assert ((new
     * BigRational(-1)).absolute().toString().equals("1")); assert
     * (BigRational.ZERO.absolute().equals(BigRational.ZERO)); assert
     * (BigRational.ONE.absolute().equals(BigRational.ONE)); assert
     * (BigRational.MINUS_ONE.absolute().equals(BigRational.ONE));
     * 
     * // "BigRational negate" assert ((new BigRational(0)).negate().toString().equals("0")); assert
     * ((new BigRational(1)).negate().toString().equals("-1")); assert ((new
     * BigRational(-1)).negate().toString().equals("1")); assert ((new BigRational(3,
     * 5)).negate().toString().equals("-3/5")); assert ((new BigRational(-3,
     * 5)).negate().toString().equals("3/5")); assert
     * (BigRational.ZERO.negate().equals(BigRational.ZERO)); assert
     * (BigRational.ONE.negate().equals(BigRational.MINUS_ONE)); assert
     * (BigRational.MINUS_ONE.negate().equals(BigRational.ONE));
     * 
     * // "BigRational invert" assert ((new BigRational(3, 5)).invert().toString().equals("5/3"));
     * assert ((new BigRational(-3, 5)).invert().toString().equals("-5/3")); assert ((new
     * BigRational(11, 7)).invert().toString().equals("7/11")); assert ((new BigRational(-11,
     * 7)).invert().toString().equals("-7/11")); assert ((new
     * BigRational(1)).invert().toString().equals("1")); assert ((new
     * BigRational(-1)).invert().toString().equals("-1")); assert ((new
     * BigRational(2)).invert().toString().equals("1/2")); assert
     * (BigRational.ONE.invert().equals(BigRational.ONE)); assert
     * (BigRational.MINUS_ONE.invert().equals(BigRational.MINUS_ONE));
     * 
     * try { BigRational.ZERO.invert(); assert (false); } catch (ArithmeticException e) { // empty }
     * 
     * // "BigRational minimum" assert ((new BigRational(3, 5)).minimum(new BigRational(7,
     * 11)).toString().equals("3/5")); assert ((new BigRational(-3, 5)).minimum(new BigRational(7,
     * 11)).toString().equals("-3/5")); assert ((new BigRational(3, 5)).minimum(new BigRational(-7,
     * 11)).toString().equals("-7/11")); assert ((new BigRational(-3, 5)).minimum(new
     * BigRational(-7, 11)).toString().equals("-7/11")); assert ((new BigRational(7,
     * 11)).minimum(new BigRational(3, 5)).toString().equals("3/5"));
     * 
     * // "BigRational maximum" assert ((new BigRational(3, 5)).maximum(new BigRational(7,
     * 11)).toString().equals("7/11")); assert ((new BigRational(-3, 5)).maximum(new BigRational(7,
     * 11)).toString().equals("7/11")); assert ((new BigRational(3, 5)).maximum(new BigRational(-7,
     * 11)).toString().equals("3/5")); assert ((new BigRational(-3, 5)).maximum(new BigRational(-7,
     * 11)).toString().equals("-3/5")); assert ((new BigRational(7, 11)).maximum(new BigRational(3,
     * 5)).toString().equals("7/11"));
     * 
     * // equals()
     * 
     * // "BigRational [polymorph] equals" assert (BigRational.ONE.equals(BigRational.ONE)); assert
     * (BigRational.ONE.equals(new BigRational(3, 3))); assert (BigRational.ONE.equals(new
     * BigRational(987654, 987654))); assert (!BigRational.ONE.equals(new BigRational(987653,
     * 987654))); assert (BigRational.ZERO.equals(new BigRational(0, 987654))); assert
     * (BigRational.MINUS_ONE.equals(new BigRational(-987654, 987654))); assert
     * (!BigRational.MINUS_ONE.equals(new BigRational(-987653, 987654))); assert ((new
     * BigRational(3, 5)).equals(new BigRational(3, 5))); assert (!(new BigRational(3,
     * 5)).equals(new BigRational(5, 3))); assert (BigRational.ZERO.equals(BigRational.ZERO));
     * assert (BigRational.ONE.equals(BigRational.ONE)); assert
     * (BigRational.MINUS_ONE.equals(BigRational.MINUS_ONE)); assert
     * (!BigRational.ZERO.equals(BigRational.ONE)); assert
     * (!BigRational.ONE.equals(BigRational.ZERO)); assert
     * (!BigRational.ONE.equals(BigRational.MINUS_ONE)); assert
     * (!BigRational.MINUS_ONE.equals(BigRational.ONE));
     * 
     * // following tests address things that changed from earlier version; // i.e. will fail with
     * that version assert (!BigRational.ZERO.equals(new Integer(0))); assert (!(new
     * BigRational(3)).equals(new Integer(3))); assert (!BigRational.ZERO.equals(new Long(0)));
     * assert (!(new BigRational(3)).equals(new Long(3))); assert (!BigRational.ZERO.equals("0"));
     * assert (!(new BigRational(3)).equals("3"));
     * 
     * assert (!(new Integer(0)).equals(BigRational.ZERO)); assert (!(new
     * Long(0)).equals(BigRational.ZERO)); assert (!(new String("0")).equals(BigRational.ZERO));
     * 
     * assert (!BigRational.ONE.equals(new RuntimeException())); assert (!(new
     * RuntimeException()).equals(BigRational.ONE));
     * 
     * // "BigRational hash code" assert ((new BigRational(3, 5)).hashCode() == (new BigRational(6,
     * 10)).hashCode());
     * 
     * // well, the necessity of these hashCode() inequality tests ain't undisputed. // so they're
     * commented out; since not a misbehavior or error. // assert (BigRational.ONE.hashCode() !=
     * BigRational.ZERO.hashCode()); // assert ((new BigRational(3, 5)).hashCode() != (new
     * BigRational(5, 3)).hashCode()); // assert ((new BigRational(3, 5)).hashCode() != (new
     * BigRational(-3, 5)).hashCode()); // assert ((new BigRational(4)).hashCode() != (new
     * BigRational(8)).hashCode());
     * 
     * // "BigRational compare to" assert ((new BigRational(3, 5)).compareTo(new BigRational(3, 5))
     * == 0); assert ((new BigRational(3, 5)).compareTo(new BigRational(5, 3)) == -1); assert ((new
     * BigRational(5, 3)).compareTo(new BigRational(3, 5)) == 1); assert ((new BigRational(3,
     * 5)).compareTo(new BigRational(-5, 3)) == 1); assert ((new BigRational(-5, 3)).compareTo(new
     * BigRational(3, 5)) == -1);
     * 
     * assert (BigRational.ZERO.compareTo(BigRational.ZERO) == 0); assert
     * (BigRational.ZERO.compareTo(BigRational.ONE) == -1); assert
     * (BigRational.ONE.compareTo(BigRational.ZERO) == 1); assert
     * (BigRational.ZERO.compareTo(BigRational.MINUS_ONE) == 1); assert
     * (BigRational.MINUS_ONE.compareTo(BigRational.ZERO) == -1); assert
     * (BigRational.ONE.compareTo(BigRational.MINUS_ONE) == 1); assert
     * (BigRational.MINUS_ONE.compareTo(BigRational.ONE) == -1); assert
     * (BigRational.ONE.compareTo(BigRational.ONE) == 0);
     * 
     * // "BigRational polymorph compare to" assert ((new BigRational(3, 5)).compareTo((Object)new
     * BigRational(3, 5)) == 0); assert ((new BigRational(3, 5)).compareTo((Object)new
     * BigRational(5, 3)) == -1); assert ((new BigRational(5, 3)).compareTo((Object)new
     * BigRational(3, 5)) == 1); assert ((new BigRational(3, 5)).compareTo((Object)new
     * BigRational(-5, 3)) == 1); assert ((new BigRational(-5, 3)).compareTo((Object)new
     * BigRational(3, 5)) == -1);
     * 
     * // "BigRational small type compare to" assert ((new BigRational(3, 5)).compareTo(0) != 0);
     * assert ((new BigRational(0)).compareTo(0) == 0); assert ((new BigRational(1)).compareTo(1) ==
     * 0); assert ((new BigRational(2)).compareTo(2) == 0); assert ((new BigRational(3,
     * 5)).compareTo(new Long(0)) != 0); assert ((new BigRational(1)).compareTo(new Long(1)) == 0);
     * assert ((new BigRational(1)).compareTo(new Integer(1)) == 0);
     * 
     * // "BigRational long/int value" assert ((new BigRational(7)).longValue() == 7); assert ((new
     * BigRational(-7)).intValue() == -7); assert (BigRational.ZERO.longValue() == 0); assert
     * (BigRational.MINUS_ONE.longValue() == -1); assert (BigRational.MINUS_ONE.intValue() == -1);
     * assert (BigRational.MINUS_ONE.shortValue() == -1); assert (BigRational.MINUS_ONE.byteValue()
     * == -1);
     * 
     * // round()/floor()/ceiling()
     * 
     * // "BigRational round"
     * 
     * assert ((new BigRational("23.49")).round(BigRational.ROUND_UP).toString().equals("24"));
     * assert ((new BigRational("23.49")).round(BigRational.ROUND_DOWN).toString().equals("23"));
     * assert ((new BigRational("23.49")).round(BigRational.ROUND_CEILING).toString().equals("24"));
     * assert ((new BigRational("23.49")).round(BigRational.ROUND_FLOOR).toString().equals("23"));
     * assert ((new BigRational("23.49")).round(BigRational.ROUND_HALF_UP).toString().equals("23"));
     * assert ((new
     * BigRational("23.49")).round(BigRational.ROUND_HALF_DOWN).toString().equals("23")); assert
     * ((new BigRational("23.49")).round(BigRational.ROUND_HALF_CEILING).toString().equals("23"));
     * assert ((new
     * BigRational("23.49")).round(BigRational.ROUND_HALF_FLOOR).toString().equals("23")); assert
     * ((new BigRational("23.49")).round(BigRational.ROUND_HALF_EVEN).toString().equals("23"));
     * assert ((new
     * BigRational("23.49")).round(BigRational.ROUND_HALF_ODD).toString().equals("23"));
     * 
     * assert ((new BigRational("-23.49")).round(BigRational.ROUND_UP).toString().equals("-24"));
     * assert ((new BigRational("-23.49")).round(BigRational.ROUND_DOWN).toString().equals("-23"));
     * assert ((new
     * BigRational("-23.49")).round(BigRational.ROUND_CEILING).toString().equals("-23")); assert
     * ((new BigRational("-23.49")).round(BigRational.ROUND_FLOOR).toString().equals("-24")); assert
     * ((new BigRational("-23.49")).round(BigRational.ROUND_HALF_UP).toString().equals("-23"));
     * assert ((new
     * BigRational("-23.49")).round(BigRational.ROUND_HALF_DOWN).toString().equals("-23")); assert
     * ((new BigRational("-23.49")).round(BigRational.ROUND_HALF_CEILING).toString().equals("-23"));
     * assert ((new
     * BigRational("-23.49")).round(BigRational.ROUND_HALF_FLOOR).toString().equals("-23")); assert
     * ((new BigRational("-23.49")).round(BigRational.ROUND_HALF_EVEN).toString().equals("-23"));
     * assert ((new
     * BigRational("-23.49")).round(BigRational.ROUND_HALF_ODD).toString().equals("-23"));
     * 
     * assert ((new BigRational("23.51")).round(BigRational.ROUND_UP).toString().equals("24"));
     * assert ((new BigRational("23.51")).round(BigRational.ROUND_DOWN).toString().equals("23"));
     * assert ((new BigRational("23.51")).round(BigRational.ROUND_CEILING).toString().equals("24"));
     * assert ((new BigRational("23.51")).round(BigRational.ROUND_FLOOR).toString().equals("23"));
     * assert ((new BigRational("23.51")).round(BigRational.ROUND_HALF_UP).toString().equals("24"));
     * assert ((new
     * BigRational("23.51")).round(BigRational.ROUND_HALF_DOWN).toString().equals("24")); assert
     * ((new BigRational("23.51")).round(BigRational.ROUND_HALF_CEILING).toString().equals("24"));
     * assert ((new
     * BigRational("23.51")).round(BigRational.ROUND_HALF_FLOOR).toString().equals("24")); assert
     * ((new BigRational("23.51")).round(BigRational.ROUND_HALF_EVEN).toString().equals("24"));
     * assert ((new
     * BigRational("23.51")).round(BigRational.ROUND_HALF_ODD).toString().equals("24"));
     * 
     * assert ((new BigRational("-23.51")).round(BigRational.ROUND_UP).toString().equals("-24"));
     * assert ((new BigRational("-23.51")).round(BigRational.ROUND_DOWN).toString().equals("-23"));
     * assert ((new
     * BigRational("-23.51")).round(BigRational.ROUND_CEILING).toString().equals("-23")); assert
     * ((new BigRational("-23.51")).round(BigRational.ROUND_FLOOR).toString().equals("-24")); assert
     * ((new BigRational("-23.51")).round(BigRational.ROUND_HALF_UP).toString().equals("-24"));
     * assert ((new
     * BigRational("-23.51")).round(BigRational.ROUND_HALF_DOWN).toString().equals("-24")); assert
     * ((new BigRational("-23.51")).round(BigRational.ROUND_HALF_CEILING).toString().equals("-24"));
     * assert ((new
     * BigRational("-23.51")).round(BigRational.ROUND_HALF_FLOOR).toString().equals("-24")); assert
     * ((new BigRational("-23.51")).round(BigRational.ROUND_HALF_EVEN).toString().equals("-24"));
     * assert ((new
     * BigRational("-23.51")).round(BigRational.ROUND_HALF_ODD).toString().equals("-24"));
     * 
     * assert ((new BigRational("23.5")).round(BigRational.ROUND_UP).toString().equals("24"));
     * assert ((new BigRational("23.5")).round(BigRational.ROUND_DOWN).toString().equals("23"));
     * assert ((new BigRational("23.5")).round(BigRational.ROUND_CEILING).toString().equals("24"));
     * assert ((new BigRational("23.5")).round(BigRational.ROUND_FLOOR).toString().equals("23"));
     * assert ((new BigRational("23.5")).round(BigRational.ROUND_HALF_UP).toString().equals("24"));
     * assert ((new
     * BigRational("23.5")).round(BigRational.ROUND_HALF_DOWN).toString().equals("23")); assert
     * ((new BigRational("23.5")).round(BigRational.ROUND_HALF_CEILING).toString().equals("24"));
     * assert ((new
     * BigRational("23.5")).round(BigRational.ROUND_HALF_FLOOR).toString().equals("23")); assert
     * ((new BigRational("23.5")).round(BigRational.ROUND_HALF_EVEN).toString().equals("24"));
     * assert ((new BigRational("23.5")).round(BigRational.ROUND_HALF_ODD).toString().equals("23"));
     * 
     * assert ((new BigRational("-23.5")).round(BigRational.ROUND_UP).toString().equals("-24"));
     * assert ((new BigRational("-23.5")).round(BigRational.ROUND_DOWN).toString().equals("-23"));
     * assert ((new
     * BigRational("-23.5")).round(BigRational.ROUND_CEILING).toString().equals("-23")); assert
     * ((new BigRational("-23.5")).round(BigRational.ROUND_FLOOR).toString().equals("-24")); assert
     * ((new BigRational("-23.5")).round(BigRational.ROUND_HALF_UP).toString().equals("-24"));
     * assert ((new
     * BigRational("-23.5")).round(BigRational.ROUND_HALF_DOWN).toString().equals("-23")); assert
     * ((new BigRational("-23.5")).round(BigRational.ROUND_HALF_CEILING).toString().equals("-23"));
     * assert ((new
     * BigRational("-23.5")).round(BigRational.ROUND_HALF_FLOOR).toString().equals("-24")); assert
     * ((new BigRational("-23.5")).round(BigRational.ROUND_HALF_EVEN).toString().equals("-24"));
     * assert ((new
     * BigRational("-23.5")).round(BigRational.ROUND_HALF_ODD).toString().equals("-23"));
     * 
     * assert ((new BigRational("22")).round(BigRational.ROUND_UP).toString().equals("22")); assert
     * ((new BigRational("22")).round(BigRational.ROUND_DOWN).toString().equals("22")); assert ((new
     * BigRational("22")).round(BigRational.ROUND_CEILING).toString().equals("22")); assert ((new
     * BigRational("22")).round(BigRational.ROUND_FLOOR).toString().equals("22")); assert ((new
     * BigRational("22")).round(BigRational.ROUND_HALF_UP).toString().equals("22")); assert ((new
     * BigRational("22")).round(BigRational.ROUND_HALF_DOWN).toString().equals("22")); assert ((new
     * BigRational("22")).round(BigRational.ROUND_HALF_CEILING).toString().equals("22")); assert
     * ((new BigRational("22")).round(BigRational.ROUND_HALF_FLOOR).toString().equals("22")); assert
     * ((new BigRational("22")).round(BigRational.ROUND_HALF_EVEN).toString().equals("22")); assert
     * ((new BigRational("22")).round(BigRational.ROUND_HALF_ODD).toString().equals("22"));
     * 
     * assert ((new BigRational("-22")).round(BigRational.ROUND_UP).toString().equals("-22"));
     * assert ((new BigRational("-22")).round(BigRational.ROUND_DOWN).toString().equals("-22"));
     * assert ((new BigRational("-22")).round(BigRational.ROUND_CEILING).toString().equals("-22"));
     * assert ((new BigRational("-22")).round(BigRational.ROUND_FLOOR).toString().equals("-22"));
     * assert ((new BigRational("-22")).round(BigRational.ROUND_HALF_UP).toString().equals("-22"));
     * assert ((new
     * BigRational("-22")).round(BigRational.ROUND_HALF_DOWN).toString().equals("-22")); assert
     * ((new BigRational("-22")).round(BigRational.ROUND_HALF_CEILING).toString().equals("-22"));
     * assert ((new
     * BigRational("-22")).round(BigRational.ROUND_HALF_FLOOR).toString().equals("-22")); assert
     * ((new BigRational("-22")).round(BigRational.ROUND_HALF_EVEN).toString().equals("-22"));
     * assert ((new BigRational("-22")).round(BigRational.ROUND_HALF_ODD).toString().equals("-22"));
     * 
     * // "BigRational round unnecessary" assert ((new
     * BigRational("23")).round(BigRational.ROUND_UNNECESSARY).toString().equals("23")); assert
     * ((new BigRational("-23")).round(BigRational.ROUND_UNNECESSARY).toString().equals("-23"));
     * 
     * try { (new BigRational("23.5")).round(BigRational.ROUND_UNNECESSARY); assert (false); } catch
     * (ArithmeticException e) { // empty } try { (new
     * BigRational("-23.5")).round(BigRational.ROUND_UNNECESSARY); assert (false); } catch
     * (ArithmeticException e) { // empty }
     * 
     * // round special cases assert (BigRational.ZERO.round().equals(BigRational.ZERO)); assert
     * (BigRational.ZERO.round(BigRational.ROUND_UNNECESSARY).equals(BigRational.ZERO));
     * 
     * // "BigRational integer part" assert ((new
     * BigRational("56.8")).integerPart().toString().equals("56")); assert ((new
     * BigRational("-56.8")).integerPart().toString().equals("-56")); assert ((new
     * BigRational("0.8")).integerPart().toString().equals("0")); assert ((new
     * BigRational("-0.8")).integerPart().toString().equals("0"));
     * 
     * // "BigRational fractional part" assert ((new
     * BigRational("56.8")).fractionalPart().equals(new BigRational("0.8"))); assert ((new
     * BigRational("-56.8")).fractionalPart().equals(new BigRational("-0.8"))); assert ((new
     * BigRational("0.8")).fractionalPart().equals(new BigRational("0.8"))); assert ((new
     * BigRational("-0.8")).fractionalPart().equals(new BigRational("-0.8")));
     * 
     * // "BigRational integer and fractional part" assert ((new
     * BigRational("56.8")).integerAndFractionalPart()[0].equals(new BigRational("56"))); assert
     * ((new BigRational("56.8")).integerAndFractionalPart()[1].equals(new BigRational("0.8")));
     * assert ((new BigRational("-56.8")).integerAndFractionalPart()[0].equals(new
     * BigRational("-56"))); assert ((new
     * BigRational("-56.8")).integerAndFractionalPart()[1].equals(new BigRational("-0.8")));
     * 
     * // sanity/harmlessness of using internal constants of commonly used numbers // scanned in by
     * commonly used radixes assert ((new BigRational("0", 2)).toString(2).equals("0")); assert
     * ((new BigRational("0", 10)).toString(10).equals("0")); assert ((new BigRational("0",
     * 16)).toString(16).equals("0")); assert ((new BigRational("1", 2)).toString(2).equals("1"));
     * assert ((new BigRational("1", 10)).toString(10).equals("1")); assert ((new BigRational("1",
     * 16)).toString(16).equals("1")); assert ((new BigRational("-1", 2)).toString(2).equals("-1"));
     * assert ((new BigRational("-1", 10)).toString(10).equals("-1")); assert ((new
     * BigRational("-1", 16)).toString(16).equals("-1")); assert ((new BigRational("10",
     * 2)).toString(2).equals("10")); assert ((new BigRational("10",
     * 10)).toString(10).equals("10")); assert ((new BigRational("10",
     * 16)).toString(16).equals("10"));
     * 
     * // more constants assert ((new BigRational("2", 10)).toString(10).equals("2")); assert ((new
     * BigRational("2", 16)).toString(16).equals("2")); assert ((new BigRational("-2",
     * 10)).toString(10).equals("-2")); assert ((new BigRational("-2",
     * 16)).toString(16).equals("-2")); assert ((new BigRational("16",
     * 10)).toString(10).equals("16")); assert ((new BigRational("16",
     * 16)).toString(16).equals("16"));
     * 
     * // floating point // remember: [IEEE 754] floats are exact where the quotient is a power of
     * two
     * 
     * assert (BigRational.valueOf(0.0).equals(BigRational.ZERO)); assert
     * (BigRational.valueOf(1.0).equals(BigRational.ONE)); assert
     * (BigRational.valueOf(-1.0).equals(BigRational.MINUS_ONE));
     * 
     * assert (BigRational.valueOf(2.0).equals(BigRational.valueOf(2))); assert
     * (BigRational.valueOf(-2.0).equals(BigRational.valueOf(-2))); assert
     * (BigRational.valueOf(4.0).equals(BigRational.valueOf(4))); assert
     * (BigRational.valueOf(-4.0).equals(BigRational.valueOf(-4))); assert
     * (BigRational.valueOf(16.0).equals(BigRational.valueOf(16))); assert
     * (BigRational.valueOf(-16.0).equals(BigRational.valueOf(-16)));
     * 
     * assert (BigRational.valueOf(3.0).equals(BigRational.valueOf(3))); assert
     * (BigRational.valueOf(-3.0).equals(BigRational.valueOf(-3))); assert
     * (BigRational.valueOf(6.0).equals(BigRational.valueOf(6))); assert
     * (BigRational.valueOf(-6.0).equals(BigRational.valueOf(-6))); assert
     * (BigRational.valueOf(12.0).equals(BigRational.valueOf(12))); assert
     * (BigRational.valueOf(-12.0).equals(BigRational.valueOf(-12)));
     * 
     * assert (BigRational.valueOf(0.5).equals(new BigRational(1, 2))); assert
     * (BigRational.valueOf(-0.5).equals(new BigRational(-1, 2))); assert
     * (BigRational.valueOf(0.25).equals(new BigRational(1, 4))); assert
     * (BigRational.valueOf(-0.25).equals(new BigRational(-1, 4))); assert
     * (BigRational.valueOf(0.0625).equals(new BigRational(1, 16))); assert
     * (BigRational.valueOf(-0.0625).equals(new BigRational(-1, 16)));
     * 
     * assert (BigRational.valueOf(1.5).equals(new BigRational(3, 2))); assert
     * (BigRational.valueOf(-1.5).equals(new BigRational(-3, 2))); assert
     * (BigRational.valueOf(0.75).equals(new BigRational(3, 4))); assert
     * (BigRational.valueOf(-0.75).equals(new BigRational(-3, 4))); assert
     * (BigRational.valueOf(0.375).equals(new BigRational(3, 8))); assert
     * (BigRational.valueOf(-0.375).equals(new BigRational(-3, 8)));
     * 
     * // other conversion direction
     * 
     * assert (BigRational.ZERO.doubleValue() == 0.0); assert (BigRational.ONE.doubleValue() ==
     * 1.0); assert (BigRational.MINUS_ONE.doubleValue() == -1.0);
     * 
     * assert (BigRational.valueOf(2).doubleValue() == 2.0); assert
     * (BigRational.valueOf(-2).doubleValue() == -2.0); assert (BigRational.valueOf(4).doubleValue()
     * == 4.0); assert (BigRational.valueOf(-4).doubleValue() == -4.0); assert
     * (BigRational.valueOf(16).doubleValue() == 16.0); assert
     * (BigRational.valueOf(-16).doubleValue() == -16.0);
     * 
     * assert (BigRational.valueOf(3).doubleValue() == 3.0); assert
     * (BigRational.valueOf(-3).doubleValue() == -3.0); assert (BigRational.valueOf(6).doubleValue()
     * == 6.0); assert (BigRational.valueOf(-6).doubleValue() == -6.0); assert
     * (BigRational.valueOf(12).doubleValue() == 12.0); assert
     * (BigRational.valueOf(-12).doubleValue() == -12.0);
     * 
     * assert ((new BigRational(1, 2)).doubleValue() == 0.5); assert ((new BigRational(-1,
     * 2)).doubleValue() == -0.5); assert ((new BigRational(1, 4)).doubleValue() == 0.25); assert
     * ((new BigRational(-1, 4)).doubleValue() == -0.25); assert ((new BigRational(1,
     * 16)).doubleValue() == 0.0625); assert ((new BigRational(-1, 16)).doubleValue() == -0.0625);
     * 
     * assert ((new BigRational(3, 2)).doubleValue() == 1.5); assert ((new BigRational(-3,
     * 2)).doubleValue() == -1.5); assert ((new BigRational(3, 4)).doubleValue() == 0.75); assert
     * ((new BigRational(-3, 4)).doubleValue() == -0.75); assert ((new BigRational(3,
     * 8)).doubleValue() == 0.375); assert ((new BigRational(-3, 8)).doubleValue() == -0.375);
     * 
     * // conversion forth and back
     * 
     * assert (BigRational.valueOf(0.0).doubleValue() == 0.0); assert
     * (BigRational.valueOf(1.0).doubleValue() == 1.0); assert
     * (BigRational.valueOf(-1.0).doubleValue() == -1.0); assert
     * (BigRational.valueOf(2.0).doubleValue() == 2.0); assert
     * (BigRational.valueOf(-2.0).doubleValue() == -2.0);
     * 
     * // maximal and minimal values, and near there assert
     * (BigRational.valueOf(Double.MAX_VALUE).doubleValue() == Double.MAX_VALUE); assert
     * (BigRational.valueOf(-Double.MAX_VALUE).doubleValue() == -Double.MAX_VALUE); assert
     * (BigRational.valueOf(Double.MAX_VALUE / 16).doubleValue() == Double.MAX_VALUE / 16); assert
     * (BigRational.valueOf(-Double.MAX_VALUE / 16).doubleValue() == -Double.MAX_VALUE / 16); //
     * [subnormal value] assert (BigRational.valueOf(Double.MIN_VALUE).doubleValue() ==
     * Double.MIN_VALUE); assert (BigRational.valueOf(-Double.MIN_VALUE).doubleValue() ==
     * -Double.MIN_VALUE); assert (BigRational.valueOf(Double.MIN_VALUE * 16).doubleValue() ==
     * Double.MIN_VALUE * 16); assert (BigRational.valueOf(-Double.MIN_VALUE * 16).doubleValue() ==
     * -Double.MIN_VALUE * 16);
     * 
     * // overflow assert (BigRational.valueOf(Double.MAX_VALUE).multiply(2).doubleValue() ==
     * Double.POSITIVE_INFINITY); assert
     * (BigRational.valueOf(Double.MAX_VALUE).multiply(4).doubleValue() ==
     * Double.POSITIVE_INFINITY); assert
     * (BigRational.valueOf(Double.MAX_VALUE).multiply(valueOf(1.2)).doubleValue() ==
     * Double.POSITIVE_INFINITY); assert
     * (BigRational.valueOf(Double.MAX_VALUE).multiply(16).doubleValue() ==
     * Double.POSITIVE_INFINITY); assert
     * (BigRational.valueOf(-Double.MAX_VALUE).multiply(2).doubleValue() ==
     * Double.NEGATIVE_INFINITY); assert
     * (BigRational.valueOf(-Double.MAX_VALUE).multiply(4).doubleValue() ==
     * Double.NEGATIVE_INFINITY); assert
     * (BigRational.valueOf(-Double.MAX_VALUE).multiply(valueOf(1.2)).doubleValue() ==
     * Double.NEGATIVE_INFINITY); assert
     * (BigRational.valueOf(-Double.MAX_VALUE).multiply(16).doubleValue() ==
     * Double.NEGATIVE_INFINITY);
     * 
     * // underflow // note that the (double)x==(double)y test yields true for 0.0==-0.0 assert
     * (BigRational.valueOf(Double.MIN_VALUE).divide(2).doubleValue() == 0.0); assert
     * (BigRational.valueOf(Double.MIN_VALUE).divide(4).doubleValue() == 0.0); assert
     * (BigRational.valueOf(Double.MIN_VALUE).divide(valueOf(1.2)).doubleValue() == 0.0); assert
     * (BigRational.valueOf(Double.MIN_VALUE).divide(16).doubleValue() == 0.0); // returning -0.0
     * (signed zero) assert (BigRational.valueOf(-Double.MIN_VALUE).divide(2).doubleValue() ==
     * -0.0); assert (BigRational.valueOf(-Double.MIN_VALUE).divide(4).doubleValue() == -0.0);
     * assert (BigRational.valueOf(-Double.MIN_VALUE).divide(valueOf(1.2)).doubleValue() == -0.0);
     * assert (BigRational.valueOf(-Double.MIN_VALUE).divide(16).doubleValue() == -0.0);
     * 
     * // signed underflow, alternative tests assert
     * (String.valueOf(BigRational.valueOf(Double.MIN_VALUE
     * ).divide(2).doubleValue()).equals("0.0")); assert
     * (String.valueOf(BigRational.valueOf(Double.MIN_VALUE
     * ).divide(4).doubleValue()).equals("0.0")); assert
     * (String.valueOf(BigRational.valueOf(Double.MIN_VALUE
     * ).divide(valueOf(1.2)).doubleValue()).equals("0.0")); assert
     * (String.valueOf(BigRational.valueOf
     * (Double.MIN_VALUE).divide(16).doubleValue()).equals("0.0")); assert
     * (String.valueOf(BigRational
     * .valueOf(-Double.MIN_VALUE).divide(2).doubleValue()).equals("-0.0")); assert
     * (String.valueOf(BigRational
     * .valueOf(-Double.MIN_VALUE).divide(4).doubleValue()).equals("-0.0")); assert
     * (String.valueOf(BigRational
     * .valueOf(-Double.MIN_VALUE).divide(valueOf(1.2)).doubleValue()).equals("-0.0")); assert
     * (String
     * .valueOf(BigRational.valueOf(-Double.MIN_VALUE).divide(16).doubleValue()).equals("-0.0"));
     * 
     * // ulp assert (BigRational.valueOf(1.0 - StrictMath.ulp(1.0)).doubleValue() == 1.0 -
     * StrictMath.ulp(1.0)); assert (BigRational.valueOf(1.0 + StrictMath.ulp(1.0)).doubleValue() ==
     * 1.0 + StrictMath.ulp(1.0)); assert (BigRational.valueOf(1.0 - StrictMath.ulp(1.0) /
     * 2).doubleValue() == 1.0 - StrictMath.ulp(1.0) / 2); assert (BigRational.valueOf(1.0 +
     * StrictMath.ulp(1.0) / 2).doubleValue() == 1.0 + StrictMath.ulp(1.0) / 2); assert
     * (BigRational.valueOf(1.0 - StrictMath.ulp(1.0) / 4).doubleValue() == 1.0 -
     * StrictMath.ulp(1.0) / 4); assert (BigRational.valueOf(1.0 + StrictMath.ulp(1.0) /
     * 4).doubleValue() == 1.0 + StrictMath.ulp(1.0) / 4);
     * 
     * // mantissa rounding // a delta of ulp/4 is expected to be rounded assert
     * (BigRational.valueOf
     * (1.0).subtract(BigRational.valueOf(StrictMath.ulp(1.0)).divide(4)).doubleValue() == 1.0);
     * assert
     * (BigRational.valueOf(16.0).subtract(BigRational.valueOf(StrictMath.ulp(16.0)).divide(4)
     * ).doubleValue() == 16.0); // note: MAX_VALUE is not a power-of-two, so it won't run into the
     * mantissa rounding case in question; // and 1/MIN_VALUE is larger than MAX_VALUE (due to
     * MIN_VALUE being subnormal) // assert
     * (BigRational.valueOf(0x1P1023).subtract(BigRational.valueOf
     * (StrictMath.ulp(0x1P1023)).divide(4)).doubleValue() == 0x1P1023);
     * 
     * // more values in between [0 and max/min] assert
     * (BigRational.valueOf(StrictMath.sqrt(Double.MAX_VALUE)).doubleValue() ==
     * StrictMath.sqrt(Double.MAX_VALUE)); assert
     * (BigRational.valueOf(StrictMath.pow(Double.MAX_VALUE, 0.2)).doubleValue() ==
     * StrictMath.pow(Double.MAX_VALUE, 0.2)); assert
     * (BigRational.valueOf(Double.MAX_VALUE).power(2).doubleValue() == Double.POSITIVE_INFINITY);
     * assert (BigRational.valueOf(Double.MAX_VALUE).power(5).doubleValue() ==
     * Double.POSITIVE_INFINITY); assert
     * (BigRational.valueOf(StrictMath.sqrt(Double.MIN_VALUE)).doubleValue() ==
     * StrictMath.sqrt(Double.MIN_VALUE)); assert
     * (BigRational.valueOf(StrictMath.pow(Double.MIN_VALUE, 0.2)).doubleValue() ==
     * StrictMath.pow(Double.MIN_VALUE, 0.2)); assert
     * (BigRational.valueOf(Double.MIN_VALUE).power(2).doubleValue() == 0.0); assert
     * (BigRational.valueOf(Double.MIN_VALUE).power(5).doubleValue() == 0.0);
     * 
     * // Infinities, NaNs try { BigRational.valueOf(Double.POSITIVE_INFINITY); assert (false); }
     * catch (NumberFormatException e) { // empty } try {
     * BigRational.valueOf(Double.NEGATIVE_INFINITY); assert (false); } catch (NumberFormatException
     * e) { // empty } try { BigRational.valueOf(Double.NaN); assert (false); } catch
     * (NumberFormatException e) { // empty }
     * 
     * assert (BigRational.valueOf(StrictMath.E).doubleValue() == StrictMath.E); assert
     * (BigRational.valueOf(StrictMath.PI).doubleValue() == StrictMath.PI); assert
     * (BigRational.valueOf(StrictMath.pow(StrictMath.E, 2)).doubleValue() ==
     * StrictMath.pow(StrictMath.E, 2)); assert
     * (BigRational.valueOf(StrictMath.sqrt(StrictMath.E)).doubleValue() ==
     * StrictMath.sqrt(StrictMath.E)); assert (BigRational.valueOf(StrictMath.pow(StrictMath.PI,
     * 2)).doubleValue() == StrictMath.pow(StrictMath.PI, 2)); assert
     * (BigRational.valueOf(StrictMath.sqrt(StrictMath.PI)).doubleValue() ==
     * StrictMath.sqrt(StrictMath.PI));
     * 
     * // same tests with single precision float
     * 
     * assert (BigRational.valueOf(0.0f).equals(BigRational.ZERO)); assert
     * (BigRational.valueOf(1.0f).equals(BigRational.ONE)); assert
     * (BigRational.valueOf(-1.0f).equals(BigRational.MINUS_ONE));
     * 
     * assert (BigRational.valueOf(2.0f).equals(BigRational.valueOf(2))); assert
     * (BigRational.valueOf(-2.0f).equals(BigRational.valueOf(-2))); assert
     * (BigRational.valueOf(4.0f).equals(BigRational.valueOf(4))); assert
     * (BigRational.valueOf(-4.0f).equals(BigRational.valueOf(-4))); assert
     * (BigRational.valueOf(16.0f).equals(BigRational.valueOf(16))); assert
     * (BigRational.valueOf(-16.0f).equals(BigRational.valueOf(-16)));
     * 
     * assert (BigRational.valueOf(3.0f).equals(BigRational.valueOf(3))); assert
     * (BigRational.valueOf(-3.0f).equals(BigRational.valueOf(-3))); assert
     * (BigRational.valueOf(6.0f).equals(BigRational.valueOf(6))); assert
     * (BigRational.valueOf(-6.0f).equals(BigRational.valueOf(-6))); assert
     * (BigRational.valueOf(12.0f).equals(BigRational.valueOf(12))); assert
     * (BigRational.valueOf(-12.0f).equals(BigRational.valueOf(-12)));
     * 
     * assert (BigRational.valueOf(0.5f).equals(new BigRational(1, 2))); assert
     * (BigRational.valueOf(-0.5f).equals(new BigRational(-1, 2))); assert
     * (BigRational.valueOf(0.25f).equals(new BigRational(1, 4))); assert
     * (BigRational.valueOf(-0.25f).equals(new BigRational(-1, 4))); assert
     * (BigRational.valueOf(0.0625f).equals(new BigRational(1, 16))); assert
     * (BigRational.valueOf(-0.0625f).equals(new BigRational(-1, 16)));
     * 
     * assert (BigRational.valueOf(1.5f).equals(new BigRational(3, 2))); assert
     * (BigRational.valueOf(-1.5f).equals(new BigRational(-3, 2))); assert
     * (BigRational.valueOf(0.75f).equals(new BigRational(3, 4))); assert
     * (BigRational.valueOf(-0.75f).equals(new BigRational(-3, 4))); assert
     * (BigRational.valueOf(0.375f).equals(new BigRational(3, 8))); assert
     * (BigRational.valueOf(-0.375f).equals(new BigRational(-3, 8)));
     * 
     * // other conversion direction
     * 
     * assert (BigRational.ZERO.floatValue() == 0.0f); assert (BigRational.ONE.floatValue() ==
     * 1.0f); assert (BigRational.MINUS_ONE.floatValue() == -1.0f);
     * 
     * assert (BigRational.valueOf(2).floatValue() == 2.0f); assert
     * (BigRational.valueOf(-2).floatValue() == -2.0f); assert (BigRational.valueOf(4).floatValue()
     * == 4.0f); assert (BigRational.valueOf(-4).floatValue() == -4.0f); assert
     * (BigRational.valueOf(16).floatValue() == 16.0f); assert
     * (BigRational.valueOf(-16).floatValue() == -16.0f);
     * 
     * assert (BigRational.valueOf(3).floatValue() == 3.0f); assert
     * (BigRational.valueOf(-3).floatValue() == -3.0f); assert (BigRational.valueOf(6).floatValue()
     * == 6.0f); assert (BigRational.valueOf(-6).floatValue() == -6.0f); assert
     * (BigRational.valueOf(12).floatValue() == 12.0f); assert
     * (BigRational.valueOf(-12).floatValue() == -12.0f);
     * 
     * assert ((new BigRational(1, 2)).floatValue() == 0.5f); assert ((new BigRational(-1,
     * 2)).floatValue() == -0.5f); assert ((new BigRational(1, 4)).floatValue() == 0.25f); assert
     * ((new BigRational(-1, 4)).floatValue() == -0.25f); assert ((new BigRational(1,
     * 16)).floatValue() == 0.0625f); assert ((new BigRational(-1, 16)).floatValue() == -0.0625f);
     * 
     * assert ((new BigRational(3, 2)).floatValue() == 1.5f); assert ((new BigRational(-3,
     * 2)).floatValue() == -1.5f); assert ((new BigRational(3, 4)).floatValue() == 0.75f); assert
     * ((new BigRational(-3, 4)).floatValue() == -0.75f); assert ((new BigRational(3,
     * 8)).floatValue() == 0.375f); assert ((new BigRational(-3, 8)).floatValue() == -0.375f);
     * 
     * // conversion forth and back
     * 
     * assert (BigRational.valueOf(0.0f).floatValue() == 0.0f); assert
     * (BigRational.valueOf(1.0f).floatValue() == 1.0f); assert
     * (BigRational.valueOf(-1.0f).floatValue() == -1.0f); assert
     * (BigRational.valueOf(2.0f).floatValue() == 2.0f); assert
     * (BigRational.valueOf(-2.0f).floatValue() == -2.0f);
     * 
     * // maximal and minimal values, and near there assert
     * (BigRational.valueOf(Float.MAX_VALUE).floatValue() == Float.MAX_VALUE); assert
     * (BigRational.valueOf(-Float.MAX_VALUE).floatValue() == -Float.MAX_VALUE); assert
     * (BigRational.valueOf(Float.MAX_VALUE / 16).floatValue() == Float.MAX_VALUE / 16); assert
     * (BigRational.valueOf(-Float.MAX_VALUE / 16).floatValue() == -Float.MAX_VALUE / 16); //
     * [subnormal value] assert (BigRational.valueOf(Float.MIN_VALUE).floatValue() ==
     * Float.MIN_VALUE); assert (BigRational.valueOf(-Float.MIN_VALUE).floatValue() ==
     * -Float.MIN_VALUE); assert (BigRational.valueOf(Float.MIN_VALUE * 16).floatValue() ==
     * Float.MIN_VALUE * 16); assert (BigRational.valueOf(-Float.MIN_VALUE * 16).floatValue() ==
     * -Float.MIN_VALUE * 16);
     * 
     * // overflow assert (BigRational.valueOf(Float.MAX_VALUE).multiply(2).floatValue() ==
     * Float.POSITIVE_INFINITY); assert
     * (BigRational.valueOf(Float.MAX_VALUE).multiply(4).floatValue() == Float.POSITIVE_INFINITY);
     * assert (BigRational.valueOf(Float.MAX_VALUE).multiply(valueOf(1.2f)).floatValue() ==
     * Float.POSITIVE_INFINITY); assert
     * (BigRational.valueOf(Float.MAX_VALUE).multiply(16).floatValue() == Float.POSITIVE_INFINITY);
     * assert (BigRational.valueOf(-Float.MAX_VALUE).multiply(2).floatValue() ==
     * Float.NEGATIVE_INFINITY); assert
     * (BigRational.valueOf(-Float.MAX_VALUE).multiply(4).floatValue() == Float.NEGATIVE_INFINITY);
     * assert (BigRational.valueOf(-Float.MAX_VALUE).multiply(valueOf(1.2f)).floatValue() ==
     * Float.NEGATIVE_INFINITY); assert
     * (BigRational.valueOf(-Float.MAX_VALUE).multiply(16).floatValue() == Float.NEGATIVE_INFINITY);
     * 
     * // underflow // note that the (float)x==(float)y test yields true for 0.0f==-0.0f assert
     * (BigRational.valueOf(Float.MIN_VALUE).divide(2).floatValue() == 0.0f); assert
     * (BigRational.valueOf(Float.MIN_VALUE).divide(4).floatValue() == 0.0f); assert
     * (BigRational.valueOf(Float.MIN_VALUE).divide(valueOf(1.2f)).floatValue() == 0.0f); assert
     * (BigRational.valueOf(Float.MIN_VALUE).divide(16).floatValue() == 0.0f); // returning -0.0f
     * (signed zero) assert (BigRational.valueOf(-Float.MIN_VALUE).divide(2).floatValue() == -0.0f);
     * assert (BigRational.valueOf(-Float.MIN_VALUE).divide(4).floatValue() == -0.0f); assert
     * (BigRational.valueOf(-Float.MIN_VALUE).divide(valueOf(1.2f)).floatValue() == -0.0f); assert
     * (BigRational.valueOf(-Float.MIN_VALUE).divide(16).floatValue() == -0.0f);
     * 
     * // signed underflow, alternative tests assert
     * (String.valueOf(BigRational.valueOf(Float.MIN_VALUE).divide(2).floatValue()).equals("0.0"));
     * assert
     * (String.valueOf(BigRational.valueOf(Float.MIN_VALUE).divide(4).floatValue()).equals("0.0"));
     * assert
     * (String.valueOf(BigRational.valueOf(Float.MIN_VALUE).divide(valueOf(1.2f)).floatValue()
     * ).equals("0.0")); assert
     * (String.valueOf(BigRational.valueOf(Float.MIN_VALUE).divide(16).floatValue()).equals("0.0"));
     * assert
     * (String.valueOf(BigRational.valueOf(-Float.MIN_VALUE).divide(2).floatValue()).equals("-0.0"
     * )); assert
     * (String.valueOf(BigRational.valueOf(-Float.MIN_VALUE).divide(4).floatValue()).equals
     * ("-0.0")); assert
     * (String.valueOf(BigRational.valueOf(-Float.MIN_VALUE).divide(valueOf(1.2f)).
     * floatValue()).equals("-0.0")); assert
     * (String.valueOf(BigRational.valueOf(-Float.MIN_VALUE).divide
     * (16).floatValue()).equals("-0.0"));
     * 
     * // ulp assert (BigRational.valueOf(1.0f - StrictMath.ulp(1.0f)).floatValue() == 1.0f -
     * StrictMath.ulp(1.0f)); assert (BigRational.valueOf(1.0f + StrictMath.ulp(1.0f)).floatValue()
     * == 1.0f + StrictMath.ulp(1.0f)); assert (BigRational.valueOf(1.0f - StrictMath.ulp(1.0f) /
     * 2).floatValue() == 1.0f - StrictMath.ulp(1.0f) / 2); assert (BigRational.valueOf(1.0f +
     * StrictMath.ulp(1.0f) / 2).floatValue() == 1.0f + StrictMath.ulp(1.0f) / 2); assert
     * (BigRational.valueOf(1.0f - StrictMath.ulp(1.0f) / 4).floatValue() == 1.0f -
     * StrictMath.ulp(1.0f) / 4); assert (BigRational.valueOf(1.0f + StrictMath.ulp(1.0f) /
     * 4).floatValue() == 1.0f + StrictMath.ulp(1.0f) / 4);
     * 
     * // mantissa rounding // a delta of ulp/4 is expected to be rounded assert
     * (BigRational.valueOf
     * (1.0f).subtract(BigRational.valueOf(StrictMath.ulp(1.0f)).divide(4)).floatValue() == 1.0f);
     * assert
     * (BigRational.valueOf(16.0f).subtract(BigRational.valueOf(StrictMath.ulp(16.0f)).divide(
     * 4)).floatValue() == 16.0f); // note: MAX_VALUE is not a power-of-two, so it won't run into
     * the mantissa rounding case in question; // and 1/MIN_VALUE is larger than MAX_VALUE (due to
     * MIN_VALUE being subnormal) // assert
     * (BigRational.valueOf(0x1P127f).subtract(BigRational.valueOf
     * (StrictMath.ulp(0x1P127f)).divide(4)).floatValue() == 0x1P127f);
     * 
     * // more values in between [0 and max/min] assert
     * (BigRational.valueOf((float)StrictMath.sqrt(Float.MAX_VALUE)).floatValue() ==
     * (float)StrictMath.sqrt(Float.MAX_VALUE)); assert
     * (BigRational.valueOf((float)StrictMath.pow(Float.MAX_VALUE, 0.2f)).floatValue() ==
     * (float)StrictMath.pow(Float.MAX_VALUE, 0.2f)); assert
     * (BigRational.valueOf(Float.MAX_VALUE).power(2).floatValue() == Float.POSITIVE_INFINITY);
     * assert (BigRational.valueOf(Float.MAX_VALUE).power(5).floatValue() ==
     * Float.POSITIVE_INFINITY); assert
     * (BigRational.valueOf((float)StrictMath.sqrt(Float.MIN_VALUE)).floatValue() ==
     * (float)StrictMath.sqrt(Float.MIN_VALUE)); assert
     * (BigRational.valueOf((float)StrictMath.pow(Float.MIN_VALUE, 0.2f)).floatValue() ==
     * (float)StrictMath.pow(Float.MIN_VALUE, 0.2f)); assert
     * (BigRational.valueOf(Float.MIN_VALUE).power(2).floatValue() == 0.0f); assert
     * (BigRational.valueOf(Float.MIN_VALUE).power(5).floatValue() == 0.0f);
     * 
     * // Infinities, NaNs try { BigRational.valueOf(Float.POSITIVE_INFINITY); assert (false); }
     * catch (NumberFormatException e) { // empty } try {
     * BigRational.valueOf(Float.NEGATIVE_INFINITY); assert (false); } catch (NumberFormatException
     * e) { // empty } try { BigRational.valueOf(Float.NaN); assert (false); } catch
     * (NumberFormatException e) { // empty }
     * 
     * assert (BigRational.valueOf((float)StrictMath.E).floatValue() == (float)StrictMath.E); assert
     * (BigRational.valueOf((float)StrictMath.PI).floatValue() == (float)StrictMath.PI); assert
     * (BigRational.valueOf((float)StrictMath.pow((float)StrictMath.E, 2)).floatValue() ==
     * (float)StrictMath.pow((float)StrictMath.E, 2)); assert
     * (BigRational.valueOf((float)StrictMath.sqrt((float)StrictMath.E)).floatValue() ==
     * (float)StrictMath.sqrt((float)StrictMath.E)); assert
     * (BigRational.valueOf((float)StrictMath.pow((float)StrictMath.PI, 2)).floatValue() ==
     * (float)StrictMath.pow((float)StrictMath.PI, 2)); assert
     * (BigRational.valueOf((float)StrictMath.sqrt((float)StrictMath.PI)).floatValue() ==
     * (float)StrictMath.sqrt((float)StrictMath.PI));
     * 
     * // toIeee754 rounding vs. double to float rounding // tests commented out: we don't really
     * care if our mode of rounding // [doubles indirectly to floats] (round-half-up) is different
     * // (e.g. from round-half-even) // assert (BigRational.valueOf(0.0).floatValue() ==
     * (float)0.0); // assert (BigRational.valueOf(1.0).floatValue() == (float)1.0); // assert
     * (BigRational.valueOf(0.5).floatValue() == (float)0.5); // assert
     * (BigRational.valueOf(0.1).floatValue() == (float)0.1); // assert
     * (BigRational.valueOf((double)0.1f).floatValue() == (float)(double)0.1f); // assert
     * (BigRational.valueOf((double)0.2f).floatValue() == (float)(double)0.2f); // assert
     * (BigRational.valueOf((double)0.3f).floatValue() == (float)(double)0.3f); // assert
     * (BigRational.valueOf((double)0.4f).floatValue() == (float)(double)0.4f); // assert
     * (BigRational.valueOf(1.0 + 0.4 * StrictMath.ulp(1.0f)).floatValue() == (float)(1.0 + 0.4 *
     * StrictMath.ulp(1.0f))); // assert (BigRational.valueOf(1.0 + 0.8 *
     * StrictMath.ulp(1.0f)).floatValue() == (float)(1.0 + 0.8 * StrictMath.ulp(1.0f))); // assert
     * (BigRational.valueOf(1.0 + 1.6 * StrictMath.ulp(1.0f)).floatValue() == (float)(1.0 + 1.6 *
     * StrictMath.ulp(1.0f)));
     * 
     * // IEEE 754 constants // commented out since not public // assert
     * (BigRational.HALF_FLOAT_FRACTION_SIZE + BigRational.HALF_FLOAT_EXPONENT_SIZE + 1 == 0x10); //
     * assert (BigRational.SINGLE_FLOAT_FRACTION_SIZE + BigRational.SINGLE_FLOAT_EXPONENT_SIZE + 1
     * == 0x20); // assert (BigRational.DOUBLE_FLOAT_FRACTION_SIZE +
     * BigRational.DOUBLE_FLOAT_EXPONENT_SIZE + 1 == 0x40); // assert
     * (BigRational.QUAD_FLOAT_FRACTION_SIZE + BigRational.QUAD_FLOAT_EXPONENT_SIZE + 1 == 0x80); //
     * assert (BigRational.HALF_FLOAT_FRACTION_SIZE >= 0); // assert
     * (BigRational.HALF_FLOAT_EXPONENT_SIZE >= 0); // // assert
     * (BigRational.HALF_FLOAT_EXPONENT_SIZE <= 64 - 1); // assert
     * (BigRational.HALF_FLOAT_EXPONENT_SIZE <= 31); // assert
     * (BigRational.SINGLE_FLOAT_FRACTION_SIZE >= 0); // assert
     * (BigRational.SINGLE_FLOAT_EXPONENT_SIZE >= 0); // // assert
     * (BigRational.SINGLE_FLOAT_EXPONENT_SIZE <= 64 - 1); // assert
     * (BigRational.SINGLE_FLOAT_EXPONENT_SIZE <= 31); // assert
     * (BigRational.DOUBLE_FLOAT_FRACTION_SIZE >= 0); // assert
     * (BigRational.DOUBLE_FLOAT_EXPONENT_SIZE >= 0); // // assert
     * (BigRational.DOUBLE_FLOAT_EXPONENT_SIZE <= 64 - 1); // assert
     * (BigRational.DOUBLE_FLOAT_EXPONENT_SIZE <= 31); // assert
     * (BigRational.QUAD_FLOAT_FRACTION_SIZE >= 0); // assert (BigRational.QUAD_FLOAT_EXPONENT_SIZE
     * >= 0); // // assert (BigRational.QUAD_FLOAT_EXPONENT_SIZE <= 64 - 1); // assert
     * (BigRational.QUAD_FLOAT_EXPONENT_SIZE <= 31);
     * 
     * // direct fromIeee754/toIeee754 methods
     * 
     * // half (5 exponent bits) assert
     * (BigRational.valueOfHalfBits((short)0).equals(BigRational.valueOf("0"))); assert
     * (BigRational.valueOf("0").halfBitsValue() == 0); assert
     * (BigRational.valueOfHalfBits((short)0x3c00).equals(BigRational.valueOf("1"))); assert
     * (BigRational.valueOf("1").halfBitsValue() == (short)0x3c00); assert
     * (BigRational.valueOfHalfBits((short)0xbc00).equals(BigRational.valueOf("-1"))); assert
     * (BigRational.valueOf("-1").halfBitsValue() == (short)0xbc00); assert
     * (BigRational.valueOfHalfBits((short)0x3e00).equals(BigRational.valueOf("1.5"))); assert
     * (BigRational.valueOf("1.5").halfBitsValue() == (short)0x3e00); assert
     * (BigRational.valueOfHalfBits((short)0xbe00).equals(BigRational.valueOf("-1.5"))); assert
     * (BigRational.valueOf("-1.5").halfBitsValue() == (short)0xbe00); assert
     * (BigRational.valueOfHalfBits((short)0x3f00).equals(BigRational.valueOf("1.75"))); assert
     * (BigRational.valueOf("1.75").halfBitsValue() == (short)0x3f00); assert
     * (BigRational.valueOfHalfBits((short)0xbf00).equals(BigRational.valueOf("-1.75"))); assert
     * (BigRational.valueOf("-1.75").halfBitsValue() == (short)0xbf00);
     * 
     * // single (8 exponent bits) assert
     * (BigRational.valueOfSingleBits(0).equals(BigRational.valueOf("0"))); assert
     * (BigRational.valueOf("0").singleBitsValue() == 0); assert
     * (BigRational.valueOfSingleBits(0x3f800000).equals(BigRational.valueOf("1"))); assert
     * (BigRational.valueOf("1").singleBitsValue() == 0x3f800000); assert
     * (BigRational.valueOfSingleBits(0xbf800000).equals(BigRational.valueOf("-1"))); assert
     * (BigRational.valueOf("-1").singleBitsValue() == 0xbf800000); assert
     * (BigRational.valueOfSingleBits(0x3fc00000).equals(BigRational.valueOf("1.5"))); assert
     * (BigRational.valueOf("1.5").singleBitsValue() == 0x3fc00000); assert
     * (BigRational.valueOfSingleBits(0xbfc00000).equals(BigRational.valueOf("-1.5"))); assert
     * (BigRational.valueOf("-1.5").singleBitsValue() == 0xbfc00000); assert
     * (BigRational.valueOfSingleBits(0x3fe00000).equals(BigRational.valueOf("1.75"))); assert
     * (BigRational.valueOf("1.75").singleBitsValue() == 0x3fe00000); assert
     * (BigRational.valueOfSingleBits(0xbfe00000).equals(BigRational.valueOf("-1.75"))); assert
     * (BigRational.valueOf("-1.75").singleBitsValue() == 0xbfe00000);
     * 
     * // double (11 exponent bits) assert
     * (BigRational.valueOfDoubleBits(0).equals(BigRational.valueOf("0"))); assert
     * (BigRational.valueOf("0").doubleBitsValue() == 0); assert
     * (BigRational.valueOfDoubleBits(0x3ff0000000000000l).equals(BigRational.valueOf("1"))); assert
     * (BigRational.valueOf("1").doubleBitsValue() == 0x3ff0000000000000l); assert
     * (BigRational.valueOfDoubleBits(0xbff0000000000000l).equals(BigRational.valueOf("-1")));
     * assert (BigRational.valueOf("-1").doubleBitsValue() == 0xbff0000000000000l); assert
     * (BigRational.valueOfDoubleBits(0x3ff8000000000000l).equals(BigRational.valueOf("1.5")));
     * assert (BigRational.valueOf("1.5").doubleBitsValue() == 0x3ff8000000000000l); assert
     * (BigRational.valueOfDoubleBits(0xbff8000000000000l).equals(BigRational.valueOf("-1.5")));
     * assert (BigRational.valueOf("-1.5").doubleBitsValue() == 0xbff8000000000000l); assert
     * (BigRational.valueOfDoubleBits(0x3ffc000000000000l).equals(BigRational.valueOf("1.75")));
     * assert (BigRational.valueOf("1.75").doubleBitsValue() == 0x3ffc000000000000l); assert
     * (BigRational.valueOfDoubleBits(0xbffc000000000000l).equals(BigRational.valueOf("-1.75")));
     * assert (BigRational.valueOf("-1.75").doubleBitsValue() == 0xbffc000000000000l);
     * 
     * // quadBitsEqual assert (BigRational.quadBitsEqual(new long[] {0xfffeffffffffffffl,
     * 0xffffffffffffffffl,}, new long[] {0xfffeffffffffffffl, 0xffffffffffffffffl,})); assert
     * (!BigRational.quadBitsEqual(new long[] {0xfffeffffffffffffl, 0xffffffffffffffffl,}, new
     * long[] {0xffffffffffffffffl, 0xfffeffffffffffffl,}));
     * 
     * // quad (15 exponent bits) assert
     * (BigRational.quadBitsEqual(BigRational.valueOf("0").quadBitsValue(), new long[] {0, 0,}));
     * assert (BigRational.valueOfQuadBits(new long[] {0x3fff000000000000l,
     * 0,}).equals(BigRational.valueOf("1"))); assert
     * (BigRational.quadBitsEqual(BigRational.valueOf("1").quadBitsValue(), new long[]
     * {0x3fff000000000000l, 0,})); assert (BigRational.valueOfQuadBits(new long[]
     * {0xbfff000000000000l, 0,}).equals(BigRational.valueOf("-1"))); assert
     * (BigRational.quadBitsEqual(BigRational.valueOf("-1").quadBitsValue(), new long[]
     * {0xbfff000000000000l, 0,})); assert (BigRational.valueOfQuadBits(new long[]
     * {0x3fff800000000000l, 0,}).equals(BigRational.valueOf("1.5"))); assert
     * (BigRational.quadBitsEqual(BigRational.valueOf("1.5").quadBitsValue(), new long[]
     * {0x3fff800000000000l, 0,})); assert (BigRational.valueOfQuadBits(new long[]
     * {0xbfff800000000000l, 0,}).equals(BigRational.valueOf("-1.5"))); assert
     * (BigRational.quadBitsEqual(BigRational.valueOf("-1.5").quadBitsValue(), new long[]
     * {0xbfff800000000000l, 0,})); assert (BigRational.valueOfQuadBits(new long[]
     * {0x3fffc00000000000l, 0,}).equals(BigRational.valueOf("1.75"))); assert
     * (BigRational.quadBitsEqual(BigRational.valueOf("1.75").quadBitsValue(), new long[]
     * {0x3fffc00000000000l, 0,})); assert (BigRational.valueOfQuadBits(new long[]
     * {0xbfffc00000000000l, 0,}).equals(BigRational.valueOf("-1.75"))); assert
     * (BigRational.quadBitsEqual(BigRational.valueOf("-1.75").quadBitsValue(), new long[]
     * {0xbfffc00000000000l, 0,}));
     * 
     * // more quad tests assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new long[]
     * {0x0000000000000000l, 0x0000000000000000l,}).quadBitsValue(), new long[]
     * {0x0000000000000000l, 0x0000000000000000l,})); assert
     * (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new long[] {0x0000000000000001l,
     * 0x0000000000000000l,}).quadBitsValue(), new long[] {0x0000000000000001l,
     * 0x0000000000000000l,})); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x0000000000000000l, 0x8000000000000000l,}).quadBitsValue(), new long[]
     * {0x0000000000000000l, 0x8000000000000000l,})); assert
     * (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new long[] {0x0000000000000000l,
     * 0x0000000000000001l,}).quadBitsValue(), new long[] {0x0000000000000000l,
     * 0x0000000000000001l,})); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x0000000000000002l, 0x0000000000000000l,}).quadBitsValue(), new long[]
     * {0x0000000000000002l, 0x0000000000000000l,})); assert
     * (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new long[] {0x0000000000000000l,
     * 0x4000000000000000l,}).quadBitsValue(), new long[] {0x0000000000000000l,
     * 0x4000000000000000l,})); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x0000000000000000l, 0x0000000000000002l,}).quadBitsValue(), new long[]
     * {0x0000000000000000l, 0x0000000000000002l,})); assert
     * (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new long[] {0x7ffe000000000000l,
     * 0x0000000000000000l,}).quadBitsValue(), new long[] {0x7ffe000000000000l,
     * 0x0000000000000000l,})); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x7ffe000000000001l, 0x0000000000000000l,}).quadBitsValue(), new long[]
     * {0x7ffe000000000001l, 0x0000000000000000l,})); assert
     * (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new long[] {0x7ffe000000000000l,
     * 0x8000000000000000l,}).quadBitsValue(), new long[] {0x7ffe000000000000l,
     * 0x8000000000000000l,})); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x7ffe000000000000l, 0x0000000000000001l,}).quadBitsValue(), new long[]
     * {0x7ffe000000000000l, 0x0000000000000001l,})); assert
     * (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new long[] {0x7ffe000000000002l,
     * 0x0000000000000000l,}).quadBitsValue(), new long[] {0x7ffe000000000002l,
     * 0x0000000000000000l,})); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x7ffe000000000000l, 0x4000000000000000l,}).quadBitsValue(), new long[]
     * {0x7ffe000000000000l, 0x4000000000000000l,})); assert
     * (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new long[] {0x7ffe000000000000l,
     * 0x0000000000000002l,}).quadBitsValue(), new long[] {0x7ffe000000000000l,
     * 0x0000000000000002l,}));
     * 
     * // forth and back
     * 
     * // most bits possible set (i.e. largest negative) assert
     * (BigRational.valueOfHalfBits((short)0xfbff).halfBitsValue() == (short)0xfbff); assert
     * (BigRational.valueOfSingleBits(0xff7fffff).singleBitsValue() == 0xff7fffff); assert
     * (BigRational.valueOfDoubleBits(0xffefffffffffffffl).doubleBitsValue() ==
     * 0xffefffffffffffffl); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0xfffeffffffffffffl, 0xffffffffffffffffl,}).quadBitsValue(), new long[]
     * {0xfffeffffffffffffl, 0xffffffffffffffffl,}));
     * 
     * // smallest non-subnormal number assert
     * (BigRational.valueOfHalfBits((short)0x0400).halfBitsValue() == (short)0x0400); assert
     * (BigRational.valueOfSingleBits(0x00800000).singleBitsValue() == 0x00800000); assert
     * (BigRational.valueOfDoubleBits(0x0010000000000000l).doubleBitsValue() ==
     * 0x0010000000000000l); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x0001000000000000l, 0x0000000000000000l,}).quadBitsValue(), new long[]
     * {0x0001000000000000l, 0x0000000000000000l,})); // largest subnormal number assert
     * (BigRational.valueOfHalfBits((short)0x02ff).halfBitsValue() == (short)0x02ff); assert
     * (BigRational.valueOfSingleBits(0x004fffff).singleBitsValue() == 0x004fffff); assert
     * (BigRational.valueOfDoubleBits(0x0008ffffffffffffl).doubleBitsValue() ==
     * 0x0008ffffffffffffl); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x00008fffffffffffl, 0xffffffffffffffffl,}).quadBitsValue(), new long[]
     * {0x00008fffffffffffl, 0xffffffffffffffffl,})); // largest subnormal number with one bit set
     * only assert (BigRational.valueOfHalfBits((short)0x0200).halfBitsValue() == (short)0x0200);
     * assert (BigRational.valueOfSingleBits(0x00400000).singleBitsValue() == 0x00400000); assert
     * (BigRational.valueOfDoubleBits(0x0008000000000000l).doubleBitsValue() ==
     * 0x0008000000000000l); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x0000800000000000l, 0x0000000000000000l,}).quadBitsValue(), new long[]
     * {0x0000800000000000l, 0x0000000000000000l,})); // one less of above assert
     * (BigRational.valueOfHalfBits((short)0x01ff).halfBitsValue() == (short)0x01ff); assert
     * (BigRational.valueOfSingleBits(0x003fffff).singleBitsValue() == 0x003fffff); assert
     * (BigRational.valueOfDoubleBits(0x0007ffffffffffffl).doubleBitsValue() ==
     * 0x0007ffffffffffffl); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x00007fffffffffffl, 0xffffffffffffffffl,}).quadBitsValue(), new long[]
     * {0x00007fffffffffffl, 0xffffffffffffffffl,})); // half of above assert
     * (BigRational.valueOfHalfBits((short)0x0100).halfBitsValue() == (short)0x0100); assert
     * (BigRational.valueOfSingleBits(0x00200000).singleBitsValue() == 0x00200000); assert
     * (BigRational.valueOfDoubleBits(0x0004000000000000l).doubleBitsValue() ==
     * 0x0004000000000000l); assert (BigRational.quadBitsEqual(BigRational.valueOfQuadBits(new
     * long[] {0x0000400000000000l, 0x0000000000000000l,}).quadBitsValue(), new long[]
     * {0x0000400000000000l, 0x0000000000000000l,}));
     * 
     * // round-off vs. exact assert
     * (BigRational.valueOfHalfBits(BigRational.valueOf("0.125").halfBitsValue
     * ()).equals(BigRational.valueOf("0.125"))); assert
     * (BigRational.valueOfSingleBits(BigRational.valueOf
     * ("0.125").singleBitsValue()).equals(BigRational.valueOf("0.125"))); assert
     * (BigRational.valueOfDoubleBits
     * (BigRational.valueOf("0.125").doubleBitsValue()).equals(BigRational.valueOf("0.125")));
     * assert
     * (BigRational.valueOfQuadBits(BigRational.valueOf("0.125").quadBitsValue()).equals(BigRational
     * .valueOf("0.125"))); assert
     * (!BigRational.valueOfHalfBits(BigRational.valueOf("0.1").halfBitsValue
     * ()).equals(BigRational.valueOf("0.1"))); assert
     * (!BigRational.valueOfSingleBits(BigRational.valueOf
     * ("0.1").singleBitsValue()).equals(BigRational.valueOf("0.1"))); assert
     * (!BigRational.valueOfDoubleBits
     * (BigRational.valueOf("0.1").doubleBitsValue()).equals(BigRational.valueOf("0.1"))); assert
     * (!BigRational
     * .valueOfQuadBits(BigRational.valueOf("0.1").quadBitsValue()).equals(BigRational.valueOf
     * ("0.1")));
     * 
     * // done.
     */

  }

}
