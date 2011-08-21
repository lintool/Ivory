package ivory.data;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.sux4j.mph.MinimalPerfectHashFunction;
import it.unimi.dsi.sux4j.mph.TwoStepsLcpMonotoneMinimalPerfectHashFunction;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

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
}
