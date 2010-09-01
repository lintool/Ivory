/**
 * 
 */
package ivory.index;

import edu.umd.cloud9.util.ArrayListOfInts;

import java.util.ArrayList;
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
