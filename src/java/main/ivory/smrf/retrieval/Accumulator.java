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

package ivory.smrf.retrieval;

import java.io.Serializable;
import java.util.Comparator;

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 */
public class Accumulator implements Comparable<Accumulator>, Serializable {
	private static final long serialVersionUID = -2003009119471096383L;

	public int docno = 0;
	public float score = 0.0f;

	public Accumulator(int docno, float score) {
		this.docno = docno;
		this.score = score;
	}

	public int compareTo(Accumulator a) {
		Preconditions.checkNotNull(a);
		
		if (score > a.score) {
			return 1;
		} else if (score < a.score) {
			return -1;
		} else if (score == a.score) {
			if (docno > a.docno) {
				return 1;
			} else if (docno < a.docno) {
				return -1;
			}
		}
		return 0;
	}

	@Override
	public String toString() {
		return "<accumulator docno=\"" + docno + "\" score=\"" + score + "\" />\n";
	}

	public static int[] accumulatorsToDocnos(Accumulator[] results) {
		Preconditions.checkNotNull(results);
		
		int[] ids = new int[results.length];
		for (int i = 0; i < results.length; i++) {
			ids[i] = results[i].docno;
		}
		return ids;
	}
	
	public static class DocnoComparator implements Comparator<Accumulator> {
		public int compare(Accumulator x, Accumulator y) {
			Preconditions.checkNotNull(x);
			Preconditions.checkNotNull(y);
			
			if( x.docno < y.docno ) {
				return -1;
			}
			else if( x.docno > y.docno ) {
				return 1;
			}
			
			return 0;
		}
	}
}
