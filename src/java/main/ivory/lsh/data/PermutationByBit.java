package ivory.lsh.data;

import static org.junit.Assert.*;
import ivory.lsh.data.Permutation;

import java.util.Random;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

public class PermutationByBit extends Permutation {

	private Random rand;
	private ArrayListOfIntsWritable randPerm;
	private int length;

	/**
	 * @param i
	 * 		length of permutation
	 */
	public PermutationByBit(int i){
		rand = new Random();
		randPerm = new ArrayListOfIntsWritable();
		for(int j = 0; j < i; j++){
			randPerm.add(j);
		}
		length = i;
	}

	public ArrayListOfIntsWritable nextPermutation() {
		for(int k = 0; k < length; k++)
		{
			int i = rand.nextInt(length);
			int j = randPerm.get(i);
			randPerm.set(i, randPerm.get(k));
			randPerm.set(k, j);
		}
		return new ArrayListOfIntsWritable(randPerm);
	}	

	public static void main(String[] args){
		int SIZE = 1000;
		PermutationByBit p = new PermutationByBit(SIZE);
		for(int k=0;k<100;k++){
			ArrayListOfIntsWritable a = p.nextPermutation();

			//make sure the permutation is not out of bounds
			for(int i=0;i<SIZE;i++){
				assertTrue(i+"-->"+a.get(i),a.get(i)<SIZE && a.get(i)>=0);
			}
			
			//make sure each position is included in the permutation exactly once
			int[] positions = new int[SIZE];
			for(int i=0;i<SIZE;i++){
				if(positions[a.get(i)]==1){
					fail("Same position included twice: "+a.get(i));
				}
				positions[a.get(i)]=1;
			}
			for(int i=0;i<SIZE;i++){
				if(positions[i]==0){
//					System.out.println(java.util.Arrays.binarySearch(positions, i));
					fail("Position not included: "+i);
				}
			}
			assertTrue(sum(positions)+"",sum(positions)==SIZE);
		}
		System.out.println("done");
	}
	
	private static int sum(int[] positions){
		int sum=0;
		for(int i=0;i<positions.length;i++){
			sum+=positions[i];
		}
		return sum;
	}
}
