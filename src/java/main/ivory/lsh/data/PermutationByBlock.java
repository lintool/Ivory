package ivory.lsh.data;

import ivory.lsh.data.Permutation;

import java.util.ArrayList;
import java.util.Collections;


import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.util.array.ArrayListOfInts;

/**
 * Permute a list of ints, block-by-block, as described in WWW07 paper by Manku et al
 * (output permutation array uses a 0-index)
 * 
 * @author ferhanture
 *
 */
public class PermutationByBlock extends Permutation {

  private ArrayListOfInts[] blocks;
  private int length, keyLen;
  private static final int NUM_BLOCKS = 5, BLOCK_SIZE = 13;
  private int combStart, combEnd;


  /**
   * @param i
   * 		length of permutation
   */
  public PermutationByBlock(int i){		
    blocks = new ArrayListOfInts[NUM_BLOCKS];
    for(int k=0;k<5;k++){
      blocks[k] = new ArrayListOfInts();
      int len=BLOCK_SIZE;
      if(k==(NUM_BLOCKS-1)){
        len = BLOCK_SIZE-1;
      }
      for(int j = (k*BLOCK_SIZE); j < (k*BLOCK_SIZE)+len; j++){
        blocks[k].add(j);
      }
    }

    length = i;
    combStart = 0;
    combEnd = 1;
  }
  public ArrayListOfIntsWritable nextPermutation() {
    if(combEnd==NUM_BLOCKS){
      throw new RuntimeException("Too many calls! All permutations done!");
    }
    System.err.println(combStart+","+combEnd);
    ArrayListOfIntsWritable perm = new ArrayListOfIntsWritable();
    addAll(perm, blocks[combStart].getArray());
    addAll(perm, blocks[combEnd].getArray());
    //		if(combStart==(BLOCK_SIZE-1) ||combEnd==(BLOCK_SIZE-1)){
    //			keyLen = 25;
    //		}else{
    //			keyLen = 26;
    //		}
    ArrayList<Integer> rest = new ArrayList<Integer>();
    for(int i=0;i<NUM_BLOCKS;i++){
      if(i!=combStart && i!=combEnd){
        rest.add(i);
      }
    }
    Collections.shuffle(rest);

    for(int nextBlock : rest){
      addAll(perm, blocks[nextBlock].getArray());
    }
    if(perm.size()!=length){
      throw new RuntimeException("Number of elements not correct!");
    }
    if(combEnd==(NUM_BLOCKS-1)){
      combStart++;
      combEnd = combStart+1;
    }else{
      combEnd++;
    }
    return perm;

  }

  public int getKeyLen(){
    return keyLen;
  }

  /**
   * Add all ints in the specified array into this object. Check for duplicates.
   * @param arr
   * 		array of ints to add to this object
   */
  public void addAll(ArrayListOfIntsWritable lst, int[] arr) {
    for(int i=0;i<arr.length;i++){
      int elt = arr[i];
      if(!lst.contains(elt)){
        lst.add(elt);
      }
    }
  }

  public static void main(String[] args){
    Permutation p = new PermutationByBlock(64);
    for(int i=0;i<10;i++){
      System.out.println(p.nextPermutation());
    }
  }
}
