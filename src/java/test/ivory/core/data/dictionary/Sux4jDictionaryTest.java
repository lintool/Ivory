package ivory.core.data.dictionary;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.sux4j.mph.MinimalPerfectHashFunction;
import it.unimi.dsi.sux4j.mph.TwoStepsLcpMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.collect.Lists;

public class Sux4jDictionaryTest extends TestCase {

  @Test
  public void test1() throws IOException {
    List<String> fruits = Lists.newArrayList("apple", "bananna", "cherry", "grape", "watermelon");

    TwoStepsLcpMonotoneMinimalPerfectHashFunction<CharSequence> dictionary =
        new TwoStepsLcpMonotoneMinimalPerfectHashFunction<CharSequence>(fruits,
            TransformationStrategies.prefixFreeIso());

    System.out.println(dictionary.getLong("apple"));
    System.out.println(dictionary.getLong("bananna"));
    System.out.println(dictionary.getLong("cherry"));
    System.out.println(dictionary.getLong("grape"));
    System.out.println(dictionary.getLong("watermelon"));

    List<String> fruits2 = Lists.newArrayList("watermelon", "bananna", "cherry", "apple", "grape");

    MinimalPerfectHashFunction<CharSequence> dictionary2 =
        new MinimalPerfectHashFunction<CharSequence>(fruits2,
            TransformationStrategies.prefixFreeIso());

    System.out.println(dictionary2.getLong("watermelon"));
    System.out.println(dictionary2.getLong("bananna"));
    System.out.println(dictionary2.getLong("cherry"));
    System.out.println(dictionary2.getLong("apple"));
    System.out.println(dictionary2.getLong("grape"));
  }

  @Test
  public void test2() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Dictionary dictionary =
        PrefixEncodedLexicographicallySortedDictionary.load(
            new Path("etc/trec-index-terms.dat"), fs);

    String[] testTerms = new String[] {
    "apple", "appl", "bannana", "banan", "cherry", "cherri", "grape", "watermelon" };

    for (String testTerm : testTerms) {
      System.out.println(String.format("Term %s, termid=%d",
          testTerm, dictionary.getId(testTerm)));
    }

    long start;
    Random rand = new Random();
    List<String> randomTerms = Lists.newArrayList();
    for (int i = 0; i < 10000; i++) {
      randomTerms.add(dictionary.getTerm(rand.nextInt(dictionary.size())));
    }

    start = System.currentTimeMillis();
    for (String t : randomTerms) {
      dictionary.getId(t);
    }
    System.out.println("Total time: " + (System.currentTimeMillis() - start) + "ms");

    ShiftAddXorSignedStringMap dict = new ShiftAddXorSignedStringMap(dictionary.iterator(),
          new TwoStepsLcpMonotoneMinimalPerfectHashFunction<CharSequence>(dictionary,
              TransformationStrategies.prefixFreeIso()));

    for (String testTerm : testTerms) {
      System.out.println(String.format("Term %s, termid=%d", testTerm, dict.getLong(testTerm)));
    }

    start = System.currentTimeMillis();
    for (String t : randomTerms) {
      dict.getLong(t);
    }
    System.out.println("Total time: " + (System.currentTimeMillis() - start) + "ms");
  }
}
