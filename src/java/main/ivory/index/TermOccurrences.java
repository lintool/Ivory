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

package ivory.index;

import java.util.ArrayList;

import edu.umd.cloud9.util.array.ArrayListOfInts;
/**
 * @author Tamer
 *
 */
public class TermOccurrences {
	ArrayListOfInts docnos = new ArrayListOfInts();
	ArrayList<int[]> positions = new ArrayList<int[]>();
	
	public void add(int docno, int[] tp){
		docnos.add(docno);
		positions.add(tp);
	}
	
	public int size(){
		return positions.size();
	}
	
	public int[] getDocnos(){
		return docnos.getArray();
	}
	
	public int[][] getPositions(){
		int[][] p = new int[positions.size()][];
		positions.toArray(p);
		return p;
	}
	
}
