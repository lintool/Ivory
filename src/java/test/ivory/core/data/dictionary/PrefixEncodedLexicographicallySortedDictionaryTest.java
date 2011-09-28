/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.core.data.dictionary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.collect.Maps;

public class PrefixEncodedLexicographicallySortedDictionaryTest {

  @Test
  public void test1() throws IOException {
    // This is the ground truth mapping between term and term id.
    Map<String, Integer> data = Maps.newLinkedHashMap();
    data.put("a", 0);
    data.put("aa", 1);
    data.put("aaa", 2);
    data.put("aaaa", 3);
    data.put("aaaaaa", 4);
    data.put("aab", 5);
    data.put("aabb", 6);
    data.put("aaabcb", 7);
    data.put("aad", 8);
    data.put("abd", 9);
    data.put("abde", 10);

    PrefixEncodedLexicographicallySortedDictionary m =
        new PrefixEncodedLexicographicallySortedDictionary(8);

    // Add entries, in order.
    for (String key : data.keySet()) {
      m.add(key);
    }

    // Verify size.
    assertEquals(data.size(), m.size());
    // Verify bidirectional mapping.
    for ( Map.Entry<String, Integer> entry : data.entrySet()) {
      assertEquals((int) entry.getValue(), m.getId(entry.getKey()));
      assertEquals(entry.getKey(), m.getTerm(entry.getValue()));
    }

    Iterator<String> iter1 = m.iterator();
    Iterator<String> iter2 = data.keySet().iterator();
    for (int i=0; i< m.size(); i++) {
      assertTrue(iter1.hasNext());
      assertTrue(iter2.hasNext());

      assertEquals(iter2.next(), iter1.next());
    }
    assertFalse(iter1.hasNext());
    assertFalse(iter2.hasNext());
    
    assertEquals(0.6923077, m.getCompresssionRatio(), 10e-6);

    FileSystem fs = FileSystem.getLocal(new Configuration());
    m.store("tmp.dat", fs);

    PrefixEncodedLexicographicallySortedDictionary n =
        PrefixEncodedLexicographicallySortedDictionary.load(new Path("tmp.dat"), fs);

    // Verify size.
    assertEquals(data.size(), n.size());
    // Verify bidirectional mapping.
    for ( Map.Entry<String, Integer> entry : data.entrySet()) {
      assertEquals((int) entry.getValue(), n.getId(entry.getKey()));
      assertEquals(entry.getKey(), n.getTerm(entry.getValue()));
    }

    iter1 = m.iterator();
    iter2 = data.keySet().iterator();
    for (int i=0; i< m.size(); i++) {
      assertEquals(iter2.next(), iter1.next());
    }
    assertFalse(iter1.hasNext());
    assertFalse(iter2.hasNext());

    fs.delete(new Path("tmp.dat"), true);
  }

  @Test
  public void test2() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    PrefixEncodedLexicographicallySortedDictionary m = 
        PrefixEncodedLexicographicallySortedDictionary.loadFromPlainTextFile(
            new Path("etc/dictionary-test.txt"), fs, 8);

    assertEquals(0, m.getId("a"));
    assertEquals(1, m.getId("a1"));
    assertEquals(248, m.getId("aardvark"));
    assertEquals(2291, m.getId("affair"));
    assertEquals(3273, m.getId("airwolf"));
    assertEquals(6845, m.getId("anntaylor"));
    assertEquals(11187, m.getId("augustus"));
    assertEquals(12339, m.getId("azzuz"));

    assertEquals(0.5631129, m.getCompresssionRatio(), 10e-6);

    m.store("tmp.dat", fs);

    PrefixEncodedLexicographicallySortedDictionary n =
        PrefixEncodedLexicographicallySortedDictionary.load(
            new Path("tmp.dat"), fs);

    assertEquals(0, n.getId("a"));
    assertEquals(1, n.getId("a1"));
    assertEquals(248, n.getId("aardvark"));
    assertEquals(2291, n.getId("affair"));
    assertEquals(3273, n.getId("airwolf"));
    assertEquals(6845, n.getId("anntaylor"));
    assertEquals(11187, n.getId("augustus"));
    assertEquals(12339, n.getId("azzuz"));

    fs.delete(new Path("tmp.dat"), true);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PrefixEncodedLexicographicallySortedDictionaryTest.class);
  }
}
