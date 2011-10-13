/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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
import static org.junit.Assert.assertTrue;

import ivory.core.data.dictionary.DefaultFrequencySortedDictionary;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class DefaultFrequencySortedDictionaryTest {

  // Test the actual dictionary for the TREC corpus.
  @Test
  public void test1() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path termsFilePath = new Path("etc/trec-index-terms.dat");
    Path termIDsFilePath = new Path("etc/trec-index-termids.dat");
    Path idToTermFilePath = new Path("etc/trec-index-termid-mapping.dat");

    DefaultFrequencySortedDictionary dictionary =
        new DefaultFrequencySortedDictionary(termsFilePath, termIDsFilePath, idToTermFilePath, fs);

    assertEquals(312232, dictionary.size());
    assertEquals("page", dictionary.getTerm(1));
    assertEquals("time", dictionary.getTerm(2));
    assertEquals("will", dictionary.getTerm(3));
    assertEquals("year", dictionary.getTerm(4));
    assertEquals("nikaan", dictionary.getTerm(100000));

    assertEquals(1, dictionary.getId("page"));
    assertEquals(2, dictionary.getId("time"));
    assertEquals(3, dictionary.getId("will"));
    assertEquals(4, dictionary.getId("year"));
    assertEquals(100000, dictionary.getId("nikaan"));
    
    assertEquals(null, dictionary.getTerm(312233));

    Iterator<String> iter = dictionary.iterator();
    assertTrue(iter.hasNext());
    assertEquals("page", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("time", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("will", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("year", iter.next());
    assertTrue(iter.hasNext());

    int cnt = 0;
    for (@SuppressWarnings("unused") String s : dictionary) {
      cnt++;
    }
    assertEquals(dictionary.size(), cnt);

    cnt = 0;
    iter = dictionary.iterator();
    while(iter.hasNext()) {
      cnt++;
      iter.next();
    }
    assertEquals(dictionary.size(), cnt);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(DefaultFrequencySortedDictionaryTest.class);
  }
}
