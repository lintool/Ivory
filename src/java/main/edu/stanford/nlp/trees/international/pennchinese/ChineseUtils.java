package edu.stanford.nlp.trees.international.pennchinese;

import edu.stanford.nlp.io.EncodingPrintWriter;
import edu.stanford.nlp.util.StringUtils;

import java.io.*;

/** This class contains a few String constants and
 *  static methods for dealing with Chinese text.
 *  <p/>
 *  <b>Warning:</b> The code contains a version that uses codePoint methods
 *  to handle full Unicode.  But it seems to tickle some bugs in
 *  Sun's JDK 1.5.  It works correctly with JDK 1.6.  By default it is
 *  disabled and a version that only handles BMP characters is used.  The
 *  latter prints a warning message if it sees a high-surrogate character.
 *
 *  @author Christopher Manning
 */
public class ChineseUtils {

  /** Whether to only support BMP character normalization.
   *  This is more limited, but avoids bugs in JDK 1.5.
   */
  private static final boolean ONLY_BMP = true;

  // These are good Unicode whitespace regexes for any language!
  public static final String ONEWHITE = "[\\s\\p{Zs}]";
  public static final String WHITE = ONEWHITE + "*";
  public static final String WHITEPLUS = ONEWHITE + "+";

  // Chinese numbers 1-10
  public static final String NUMBERS = "[\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341]";

  // List of characters similar to \u00B7 listed in the Unicode 5.0 manual
  public static final String MID_DOT_REGEX_STR = "[\u00B7\u0387\u2022\u2024\u2027\u2219\u22C5\u30FB]";


  // These are the constants for the normalize method
  public static final int LEAVE = 0;
  public static final int ASCII = 1;
  public static final int NORMALIZE = 1; // Unicode normalization moves to low
  public static final int FULLWIDTH = 2;
  public static final int DELETE = 3;
  public static final int DELETE_EXCEPT_BETWEEN_ASCII = 4;
  public static final int MAX_LEGAL = 4;

  private int[] puaChars = { 0xE005 };
  private int[] uniChars = { 0x42B5 };


  // not instantiable
  private ChineseUtils() {}

  public static boolean isNumber(char c) {
    return (StringUtils.matches(String.valueOf(c), NUMBERS) || Character.isDigit(c));
  }

  public static String normalize(String in) {
    return normalize(in, FULLWIDTH, ASCII);
  }

  public static String normalize(String in, int ascii, int spaceChar) {
    return normalize(in, ascii, spaceChar, LEAVE);
  }
  /** This will normalize a Unicode String in various ways.  This routine
   *  correctly handles characters outside the basic multilingual plane.
   *
   *  @param in The String to be normalized
   *  @param ascii For characters conceptually in the ASCII range of
   *      ! through ~ (U+0021 through U+007E or U+FF01 through U+FF5E),
   *      if this is ChineseUtils.LEAVE, then do nothing,
   *      if it is ASCII then map them from the Chinese Full Width range
   *      to ASCII values, and if it is FULLWIDTH then do the reverse.
   *  @param spaceChar For characters that satisfy Character.isSpaceChar(),
   *      if this is ChineseUtils.LEAVE, then do nothing,
   *      if it is ASCII then map them to the space character U+0020, and
   *      if it is FULLWIDTH then map them to U+3000.
   *  @param midDot For characters that satisfy Character.isSpaceChar(),
   *      if this is ChineseUtils.LEAVE, then do nothing,
   *      if it is NORMALIZE then map them to the extended Latin character U+00B7, and
   *      if it is FULLWIDTH then map them to U+30FB.
   *  @return The in String normalized according to the other arguments.
   */
  public static String normalize(String in,
                                 int ascii,
                                 int spaceChar,
                                 int midDot) {
    if (ascii < 0 || ascii > MAX_LEGAL ||
        spaceChar < 0 || spaceChar > MAX_LEGAL) {
      throw new IllegalArgumentException("ChineseUtils: Unknown parameter option");
    }
    StringBuilder out = new StringBuilder();
    int len = in.length();
    boolean delete;
    if (ONLY_BMP) {
      for (int i = 0; i < len; i++) {
        char cp = in.charAt(i);
        if (Character.isHighSurrogate(cp)) {
          if (i + 1 < len) {
            EncodingPrintWriter.err.println("ChineseUtils.normalize warning: non-BMP codepoint U+" +
                    Integer.toHexString(Character.codePointAt(in, i)) + " in " + in);
          } else {
            EncodingPrintWriter.err.println("ChineseUtils.normalize warning: unmatched high surrogate character U+" +
                    Integer.toHexString(Character.codePointAt(in, i)) + " in " + in);

          }
        }
        Character.UnicodeBlock cub = Character.UnicodeBlock.of(cp);
        if (cub == Character.UnicodeBlock.PRIVATE_USE_AREA ||
                cub == Character.UnicodeBlock.SUPPLEMENTARY_PRIVATE_USE_AREA_A ||
                cub == Character.UnicodeBlock.SUPPLEMENTARY_PRIVATE_USE_AREA_A) {
          EncodingPrintWriter.err.println("ChineseUtils.normalize warning: private use area codepoint U+" + Integer.toHexString(cp) + " in " + in);
        }
        delete = false;
        switch (ascii) {
          case LEAVE:
            break;
          case ASCII:
            if (cp >= '\uFF01' && cp <= '\uFF5E') {
              cp -= (0xFF00 - 0x0020);
            }
            break;
          case FULLWIDTH:
            if (cp >= '\u0021' && cp <= '\u007E') {
              cp += (0xFF00 - 0x0020);
            }
            break;
          default:
            throw new IllegalArgumentException("ChineseUtils: Unsupported parameter option: ascii=" + ascii);
        }
        switch (spaceChar) {
          case LEAVE:
            break;
          case ASCII:
            if (Character.isSpaceChar(cp)) {
              cp = ' ';
            }
            break;
          case FULLWIDTH:
            if (Character.isSpaceChar(cp)) {
              cp = '\u3000';
            }
            break;
          case DELETE:
            if (Character.isSpaceChar(cp)) {
              delete = true;
            }
            break;
          case DELETE_EXCEPT_BETWEEN_ASCII:
            char cpp = 0;
            if (i > 0) { cpp = in.charAt(i - 1); }
            char cpn = 0;
            if (i < (len - 1)) { cpn = in.charAt(i + 1); }
            // EncodingPrintWriter.out.println("cp: " + cp + "; cpp: " + cpp + "cpn: " + cpn +
            //      "; isSpace: " + Character.isSpaceChar(cp) + "; isAsciiLHL: " + isAsciiLowHigh(cpp) +
            //      "; isAsciiLHR: " + isAsciiLowHigh(cpn), "UTF-8");
            if (Character.isSpaceChar(cp) && ! (isAsciiLowHigh(cpp) && isAsciiLowHigh(cpn))) {
              delete = true;
            }
        }
        switch (midDot) {
          case LEAVE:
            break;
          case NORMALIZE:
            if (cp == '\u00B7' || cp == '\u0387' || cp == '\u2022' ||
                cp == '\u2024' || cp == '\u2027' || cp == '\u2219' ||
                cp == '\u22C5' || cp == '\u30FB') {
              cp = '\u00B7';
            }
            break;
          case FULLWIDTH:
            if (cp == '\u00B7' || cp == '\u0387' || cp == '\u2022' ||
                cp == '\u2024' || cp == '\u2027' || cp == '\u2219' ||
                cp == '\u22C5' || cp == '\u30FB') {
              cp = '\u30FB';
            }
            break;
          case DELETE:
            if (cp == '\u00B7' || cp == '\u0387' || cp == '\u2022' ||
                cp == '\u2024' || cp == '\u2027' || cp == '\u2219' ||
                cp == '\u22C5' || cp == '\u30FB') {
              delete = true;
            }
            break;
          default:
            throw new IllegalArgumentException("ChineseUtils: Unsupported parameter option: midDot=" + midDot);
        }
        if ( ! delete) {
          out.append(cp);
        }
      } // end for
    } else {
      // Do it properly with codepoints, for non-BMP Unicode as well
      int numCP = in.codePointCount(0, len);
      for (int i = 0; i < numCP; i++) {
        int offset = in.offsetByCodePoints(0, i);
        int cp = in.codePointAt(offset);
        Character.UnicodeBlock cub = Character.UnicodeBlock.of(cp);
        if (cub == Character.UnicodeBlock.PRIVATE_USE_AREA ||
                cub == Character.UnicodeBlock.SUPPLEMENTARY_PRIVATE_USE_AREA_A ||
                cub == Character.UnicodeBlock.SUPPLEMENTARY_PRIVATE_USE_AREA_A) {
          EncodingPrintWriter.err.println("ChineseUtils.normalize warning: private use area codepoint U+" + Integer.toHexString(cp) + " in " + in);
        }
        delete = false;
        switch (ascii) {
          case LEAVE:
            break;
          case ASCII:
            if (cp >= '\uFF01' && cp <= '\uFF5E') {
              cp -= (0xFF00 - 0x0020);
            }
            break;
          case FULLWIDTH:
            if (cp >= '\u0021' && cp <= '\u007E') {
              cp += (0xFF00 - 0x0020);
            }
            break;
          default:
            throw new IllegalArgumentException("ChineseUtils: Unsupported parameter option: ascii=" + ascii);
        }
        switch (spaceChar) {
          case LEAVE:
            break;
          case ASCII:
            if (Character.isSpaceChar(cp)) {
              cp = ' ';
            }
            break;
          case FULLWIDTH:
            if (Character.isSpaceChar(cp)) {
              cp = '\u3000';
            }
            break;
          case DELETE:
            if (Character.isSpaceChar(cp)) {
              delete = true;
            }
            break;
          case DELETE_EXCEPT_BETWEEN_ASCII:
            int cpp = 0;
            if (i > 0) { cpp = in.codePointAt(i - 1); }
            int cpn = 0;
            if (i < (numCP - 1)) { cpn = in.codePointAt(i + 1); }
            if (Character.isSpaceChar(cp) && ! (isAsciiLowHigh(cpp) && isAsciiLowHigh(cpn))) {
              delete = true;
            }
        }
        switch (midDot) {
          case LEAVE:
            break;
          case NORMALIZE:
            if (cp == '\u00B7' || cp == '\u0387' || cp == '\u2022' ||
                cp == '\u2024' || cp == '\u2027' || cp == '\u2219' ||
                cp == '\u22C5' || cp == '\u30FB') {
              cp = '\u00B7';
            }
            break;
          case FULLWIDTH:
            if (cp == '\u00B7' || cp == '\u0387' || cp == '\u2022' ||
                cp == '\u2024' || cp == '\u2027' || cp == '\u2219' ||
                cp == '\u22C5' || cp == '\u30FB') {
              cp = '\u30FB';
            }
            break;
          case DELETE:
            if (cp == '\u00B7' || cp == '\u0387' || cp == '\u2022' ||
                cp == '\u2024' || cp == '\u2027' || cp == '\u2219' ||
                cp == '\u22C5' || cp == '\u30FB') {
              delete = true;
            }
            break;
          default:
            throw new IllegalArgumentException("ChineseUtils: Unsupported parameter option: midDot=" + midDot);
        }
        if ( ! delete) {
          out.appendCodePoint(cp);
        }
      } // end for
    }
    return out.toString();
  }

  private static boolean isAsciiLowHigh(int cp) {
    return cp >= '\uFF01' && cp <= '\uFF5E' ||
        cp >= '\u0021' && cp <= '\u007E';
  }

  /** Mainly for testing.  Usage:
   *  <code>ChineseUtils ascii spaceChar word*</code>
   *  <p>
   *  ascii and spaceChar are integers: 0 = leave, 1 = ascii, 2 = fullwidth.
   *  The words listed are then normalized and sent to stdout.
   *  If no words are given, the program reads from and normalizes stdin.
   *  Input is assumed to be in UTF-8.
   *
   *  @param args Command line arguments as above
   *  @throws IOException If any problems accessing command-line files
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.err.println("usage: ChineseUtils ascii space midDot word*");
      System.err.println("  First 3 args are int flags; a filter or maps args as words; assumes UTF-8");
      return;
    }
    int i = Integer.parseInt(args[0]);
    int j = Integer.parseInt(args[1]);
    int midDot = Integer.parseInt(args[2]);
    if (args.length > 3) {
      for (int k = 3; k < args.length; k++) {
        EncodingPrintWriter.out.println(normalize(args[k], i, j, midDot));
      }
    } else {
      BufferedReader r =
        new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
      String line;
      while ((line = r.readLine()) != null) {
        EncodingPrintWriter.out.println(normalize(line, i, j, midDot));
      }
    }
  }

}
