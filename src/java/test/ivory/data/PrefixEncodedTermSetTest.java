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

package ivory.data;

import static org.junit.Assert.assertEquals;

import ivory.core.data.dictionary.PrefixEncodedLexicographicallySortedDictionary;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class PrefixEncodedTermSetTest {

	@Test
	public void test1() throws IOException {
		PrefixEncodedLexicographicallySortedDictionary m = new PrefixEncodedLexicographicallySortedDictionary(8);
		m.add("a");
		m.add("aa");
		m.add("aaa");
		m.add("aaaa");
		m.add("aaaaaa");
		m.add("aab");
		m.add("aabb");
		m.add("aaabcb");
		m.add("aad");
		m.add("abd");
		m.add("abde");

		assertEquals(0, m.getIndex("a"));
		assertEquals(1, m.getIndex("aa"));
		assertEquals(2, m.getIndex("aaa"));
		assertEquals(3, m.getIndex("aaaa"));
		assertEquals(4, m.getIndex("aaaaaa"));
		assertEquals(5, m.getIndex("aab"));
		assertEquals(6, m.getIndex("aabb"));
		assertEquals(7, m.getIndex("aaabcb"));
		assertEquals(8, m.getIndex("aad"));
		assertEquals(9, m.getIndex("abd"));
		assertEquals(10, m.getIndex("abde"));
		
		Set<String> set = m.getKeySet();
		Iterator<String> iter = set.iterator();
		
		assertEquals(iter.next(), "a");
		assertEquals(iter.next(), "aa");
		assertEquals(iter.next(), "aaa");
		assertEquals(iter.next(), "aaaa");
		assertEquals(iter.next(), "aaaaaa");
		assertEquals(iter.next(), "aab");
		assertEquals(iter.next(), "aabb");
		assertEquals(iter.next(), "aaabcb");
		assertEquals(iter.next(), "aad");
		assertEquals(iter.next(), "abd");
		assertEquals(iter.next(), "abde");
		
		assertEquals(0.6923077, m.getCompresssionRatio(), 10e-6);
		
		FileSystem fs = FileSystem.getLocal(new Configuration());
		m.store("tmp.dat", fs);
		
		PrefixEncodedLexicographicallySortedDictionary n = PrefixEncodedLexicographicallySortedDictionary.load("tmp.dat", fs);
		assertEquals(0, n.getIndex("a"));
		assertEquals(1, n.getIndex("aa"));
		assertEquals(2, n.getIndex("aaa"));
		assertEquals(3, n.getIndex("aaaa"));
		assertEquals(4, n.getIndex("aaaaaa"));
		assertEquals(5, n.getIndex("aab"));
		assertEquals(6, n.getIndex("aabb"));
		assertEquals(7, n.getIndex("aaabcb"));
		assertEquals(8, n.getIndex("aad"));
		assertEquals(9, n.getIndex("abd"));
		assertEquals(10, n.getIndex("abde"));
		
		set = n.getKeySet();
		iter = set.iterator();
		
		assertEquals(iter.next(), "a");
		assertEquals(iter.next(), "aa");
		assertEquals(iter.next(), "aaa");
		assertEquals(iter.next(), "aaaa");
		assertEquals(iter.next(), "aaaaaa");
		assertEquals(iter.next(), "aab");
		assertEquals(iter.next(), "aabb");
		assertEquals(iter.next(), "aaabcb");
		assertEquals(iter.next(), "aad");
		assertEquals(iter.next(), "abd");
		assertEquals(iter.next(), "abde");
		
		fs.delete(new Path("tmp.dat"), true);
	}
	
	@Test
	public void test2() throws IOException {
		FileSystem fs = FileSystem.getLocal(new Configuration());
		PrefixEncodedLexicographicallySortedDictionary m = PrefixEncodedLexicographicallySortedDictionary.loadFromPlainTextFile("etc/dictionary-test.txt", fs, 8);

		assertEquals(0, m.getIndex("a"));
		assertEquals(1, m.getIndex("a1"));
		assertEquals(248, m.getIndex("aardvark"));
		assertEquals(2291, m.getIndex("affair"));
		assertEquals(3273, m.getIndex("airwolf"));
		assertEquals(6845, m.getIndex("anntaylor"));
		assertEquals(11187, m.getIndex("augustus"));
		assertEquals(12339, m.getIndex("azzuz"));
		
		assertEquals(0.5631129, m.getCompresssionRatio(), 10e-6);

		m.store("tmp.dat", fs);

		PrefixEncodedLexicographicallySortedDictionary n = PrefixEncodedLexicographicallySortedDictionary.load("tmp.dat", fs);

		assertEquals(0, n.getIndex("a"));
		assertEquals(1, n.getIndex("a1"));
		assertEquals(248, n.getIndex("aardvark"));
		assertEquals(2291, n.getIndex("affair"));
		assertEquals(3273, n.getIndex("airwolf"));
		assertEquals(6845, n.getIndex("anntaylor"));
		assertEquals(11187, n.getIndex("augustus"));
		assertEquals(12339, n.getIndex("azzuz"));

		fs.delete(new Path("tmp.dat"), true);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PrefixEncodedTermSetTest.class);
	}
}