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

package ivory.smrf.retrieval;

import java.io.Serializable;

/**
 * @author Don Metzler
 *
 */
public class Accumulator implements Comparable, Serializable {

	/**
	 * serialization unique id
	 */
	private static final long serialVersionUID = -2003009119471096383L;

	/**
	 * docid associated with this accumulator
	 */
	public int docid = 0;
	
	/**
	 * score associated with this accumulator 
	 */
	public double score = 0.0;
	
	/**
	 * @param docid
	 * @param score
	 */
	public Accumulator( int docid, double score ) {
		this.docid = docid;
		this.score = score;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Object o) {
		Accumulator a = (Accumulator)o;
		if( score > a.score ) {
			return 1;
		}
		else if( score < a.score ) {
			return -1;
		}
		else if( score == a.score ) {
			if( docid > a.docid ) {
				return 1;
			}
			else if( docid < a.docid ) {
				return -1;
			}
		}
		return 0;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "<accumulator docid=\"" + docid + "\" score=\"" + score + "\" />\n";
	}

	/**
	 * @param results
	 */
	public static int [] accumulatorsToDocIDS( Accumulator[] results ) {
		int [] ids = new int[ results.length ];
		for( int i = 0; i < results.length; i++ ) {
			ids[ i ] = results[ i ].docid;
		}
		return ids;
	}
}
