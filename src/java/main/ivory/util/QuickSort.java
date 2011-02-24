package ivory.util;

import edu.umd.cloud9.util.StackOfInts;

public class QuickSort {

    // quicksort a[left] to a[right]
    public static void quicksort(short[] keys, int[] counts, int left, int right) {
        if (right <= left) return;
        int i = partition(keys, counts, left, right);
        quicksort(keys, counts, left, i-1);
        quicksort(keys, counts, i+1, right);
    }
    
    public static void quicksortWithStack(short[] keys, int[] counts, int left, int right) {
        if (right <= left) return;
    	StackOfInts lStack = new StackOfInts(), rStack = new StackOfInts();
    	
    	lStack.push(left);
    	rStack.push(right);
        while(!lStack.isEmpty()){
        	left = lStack.pop();
        	right = rStack.pop();
        	int i = partition(keys, counts, left, right);
        	if (i-1 > left){
        		lStack.push(left);
        		rStack.push(i-1);
        	}
        	if(right > i+1){
        		lStack.push(i+1);
        		rStack.push(right);
        	}
        }
    }
    
    public static void quicksortWithStack(Object[] keys, int[] counts, int left, int right) {
        if (right <= left) return;
    	StackOfInts lStack = new StackOfInts(), rStack = new StackOfInts();
    	
    	lStack.push(left);
    	rStack.push(right);
        while(!lStack.isEmpty()){
        	left = lStack.pop();
        	right = rStack.pop();
        	int i = partition(keys, counts, left, right);
        	if (i-1 > left){
        		lStack.push(left);
        		rStack.push(i-1);
        	}
        	if(right > i+1){
        		lStack.push(i+1);
        		rStack.push(right);
        	}
        }
    }
    
 // quicksort a[left] to a[right]
    public static void quicksort(int[] keys, int[] counts, short[] counts2, int left, int right) {
        if (right <= left) return;
        int i = partition(keys, counts, counts2, left, right);
        quicksort(keys, counts, counts2, left, i-1);
        quicksort(keys, counts, counts2, i+1, right);
    }
    
    
    public static void quicksort(short[] keys, int[] counts, Object[] counts2, int left, int right) {
        if (right <= left) return;
        int i = partition(keys, counts, counts2, left, right);
        quicksort(keys, counts, counts2, left, i-1);
        quicksort(keys, counts, counts2, i+1, right);
    }
    
    public static void quicksortWithSecondary(int[] keys, int[] counts, short[] counts2, int left, int right) {
        if (right <= left) return;
        int i = partitionWithSecondary(keys, counts, counts2, left, right);
        quicksortWithSecondary(keys, counts, counts2, left, i-1);
        quicksortWithSecondary(keys, counts, counts2, i+1, right);
    }
    
    public static void quicksortWithSecondary(int[] keys, int[] counts, long[] counts2, int left, int right) {
        if (right <= left) return;
        int i = partitionWithSecondary(keys, counts, counts2, left, right);
        quicksortWithSecondary(keys, counts, counts2, left, i-1);
        quicksortWithSecondary(keys, counts, counts2, i+1, right);
    }
    
    public static void quicksortWithSecondaryAndStack(int[] keys, int[] counts, short[] counts2, int left, int right) {
    	
    	if (right <= left) return;
    	StackOfInts lStack = new StackOfInts(), rStack = new StackOfInts();
    	
    	lStack.push(left);
    	rStack.push(right);
        while(!lStack.isEmpty()){
        	left = lStack.pop();
        	right = rStack.pop();
        	int i = partitionWithSecondary(keys, counts, counts2, left, right);
        	if (i-1 > left){
        		lStack.push(left);
        		rStack.push(i-1);
        	}
        	if(right > i+1){
        		lStack.push(i+1);
        		rStack.push(right);
        	}
        }
    }
    
    public static void quicksort(int[] keys, int[] counts, int left, int right) {
        if (right <= left) return;
        int i = partition(keys, counts, left, right);
        quicksort(keys, counts, left, i-1);
        quicksort(keys, counts, i+1, right);
    }
    
    public static void quicksort(Object[] keys, int[] counts, int left, int right) {
        if (right <= left) return;
        int i = partition(keys, counts, left, right);
        quicksort(keys, counts, left, i-1);
        quicksort(keys, counts, i+1, right);
    }

    // partition a[left] to a[right], assumes left < right
    private static int partition(short[] keys, int[] counts, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (counts[++i] < counts[right])      // find item on left to swap
                ;                               // a[right] acts as sentinel
            while (counts[right] < counts[--j])      // find item on right to swap
                if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            short tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        short tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        return i;
    }
    
    private static int partition(Object[] keys, int[] counts, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (counts[++i] < counts[right])      // find item on left to swap
                ;                               // a[right] acts as sentinel
            while (counts[right] < counts[--j])      // find item on right to swap
                if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            Object tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        Object tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        return i;
    }
    
    private static int partition(int[] keys, int[] counts, short[] counts2, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (counts[++i] < counts[right])      // find item on left to swap
                ;                               // a[right] acts as sentinel
            while (counts[right] < counts[--j])      // find item on right to swap
                if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            int tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
            
            short tmp2 = counts2[i];
            counts2[i] = counts2[j];
            counts2[j] = tmp2;
            
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        
        int tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        
        short tmp2 = counts2[i];
        counts2[i] = counts2[right];
        counts2[right] = tmp2;
        return i;
    }
    
    private static int partition(short[] keys, int[] counts, Object[] counts2, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (counts[++i] < counts[right])      // find item on left to swap
                ;                               // a[right] acts as sentinel
            while (counts[right] < counts[--j])      // find item on right to swap
                if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            short tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
            
            Object tmp2 = counts2[i];
            counts2[i] = counts2[j];
            counts2[j] = tmp2;
            
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        
        short tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        
        Object tmp2 = counts2[i];
        counts2[i] = counts2[right];
        counts2[right] = tmp2;
        return i;
    }
    
    private static int partitionWithSecondary(int[] keys, int[] counts, short[] counts2, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
        	do{
        		i++;
        	}while (counts[i] < counts[right] || (counts[i] == counts[right] && keys[i] < keys[right]))      // find item on left to swap
                ;                               // a[right] acts as sentinel
        	
            //while (counts[++i] < counts[right])      // find item on left to swap
            //    ;                               // a[right] acts as sentinel
            do{
            	j--;
            }while (j!=left && (counts[right] < counts[j] || (counts[right] == counts[j] && keys[right]<keys[j] )));      // find item on right to swap
        	
        	//while (counts[right] < counts[--j])      // find item on right to swap
            //    if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            int tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
            
            short tmp2 = counts2[i];
            counts2[i] = counts2[j];
            counts2[j] = tmp2;
            
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        
        int tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        
        short tmp2 = counts2[i];
        counts2[i] = counts2[right];
        counts2[right] = tmp2;
        return i;
    }
    
    private static int partitionWithSecondary(int[] keys, int[] counts, long[] counts2, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
        	do{
        		i++;
        	}while (counts[i] < counts[right] || (counts[i] == counts[right] && keys[i] < keys[right]))      // find item on left to swap
                ;                               // a[right] acts as sentinel
        	
            //while (counts[++i] < counts[right])      // find item on left to swap
            //    ;                               // a[right] acts as sentinel
            do{
            	j--;
            }while (j!=left && (counts[right] < counts[j] || (counts[right] == counts[j] && keys[right]<keys[j] )));      // find item on right to swap
        	
        	//while (counts[right] < counts[--j])      // find item on right to swap
            //    if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            int tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
            
            long tmp2 = counts2[i];
            counts2[i] = counts2[j];
            counts2[j] = tmp2;
            
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        
        int tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        
        long tmp2 = counts2[i];
        counts2[i] = counts2[right];
        counts2[right] = tmp2;
        return i;
    }
    
    private static int partition(int[] keys, int[] counts, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (counts[++i] < counts[right])      // find item on left to swap
                ;                               // a[right] acts as sentinel
            while (counts[right] < counts[--j])      // find item on right to swap
                if (j == left) break;           // don't go out-of-bounds
            if (i >= j) break;                  // check if pointers cross
            int swap = counts[i];
            counts[i] = counts[j];
            counts[j] = swap;                      // swap two elements into place
            
            int tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp; 
        }
        int swap = counts[i];
        counts[i] = counts[right];
        counts[right] = swap;
        
        int tmp = keys[i];
        keys[i] = keys[right];
        keys[right] = tmp;
        
        return i;
    }
    public static void main(String[] args){
		//int[] values = {33452934, 31096615, 4670278, 7300698, 11189689, 21637692};
		int[] values = {5452455, 47426059, 48602024};

		short[] keys = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15, 16, 17,18, 19, 20};
		//NewQuickSort.quicksortShort(keys, values, 0, values.length-1);
		for(int i = 0; i< values.length; i++) System.out.print(values[i]+" ");
		
	}
}
