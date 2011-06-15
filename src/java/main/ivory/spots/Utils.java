package ivory.spots;

import ivory.spots.partition.SpotSigsPartition;

import java.util.ArrayList;

/**
 * @author Tamer
 *
 */
public class Utils {
	
	public static SpotSigsPartition[] computePartitions(int minAllowedSpotSigs, int spotSigsRange, float similarityThreshold){
		int id = 0, last = minAllowedSpotSigs;

		ArrayList<SpotSigsPartition> tempPartitions = new ArrayList<SpotSigsPartition>();

		for (int i = minAllowedSpotSigs; i <= spotSigsRange; i++) {
			if (i - last > (1 - similarityThreshold) * i) {
				SpotSigsPartition partition = new SpotSigsPartition(id, last, i);
				tempPartitions.add(partition);
				last = i + 1;
				id++;
			}
		}

		// add one last partition
		SpotSigsPartition partition = new SpotSigsPartition(id, last,
				Integer.MAX_VALUE);
		tempPartitions.add(partition);
		
		SpotSigsPartition[] partitions = new SpotSigsPartition[tempPartitions.size()];
		tempPartitions.toArray(partitions);
		return partitions;
	}

	public static SpotSigsPartition getPartition(int length, SpotSigsPartition[] partitions) {
		int i = 0;
		while (i < partitions.length && length > partitions[i].end)
			i++;
		if (i == partitions.length)
			return partitions[partitions.length - 1];
		return partitions[i];
	}

	/*public static double getJaccard(DocVector doc1, DocVector doc2, double threshold) {
		// assuming lists are sorted ascendingly
		double min, max, s_min = 0, s_max = 0, bound = 0;
		double upper_max = Math.max(doc1.docLength(), doc2.docLength());
		double upper_union = doc1.docLength() + doc2.docLength();
		int i = 0, c1, c2, s_c1 = 0, s_c2 = 0;
		int j = 0;
		int[] keys1 = doc1.keys;
		int[] keys2 = doc2.keys;
		while(true){
			if(keys1[i] == keys2[j]){
				c1 = doc1.values[i];
				c2 = doc2.values[j];
				min = Math.min(c1, c2);
				max = Math.max(c1, c2);
				s_min += min;
				s_max += max;
				s_c1 += c1;
				s_c2 += c2;
				
				// Early threshold break for pairwise counter comparison
				bound += max - min;
				if ((upper_max - bound) / upper_max < threshold)
					return 0;
				else if (s_min / upper_union >= threshold)
					return 1;
					
				
				if(i<keys1.length-1) i++; else break;
				if(j<keys2.length-1) j++; else break;
			}
			else{
				if(keys1[i] < keys2[j]){
					if(i<keys1.length-1) i++; else break;
				}
				else{
					if(j<keys2.length-1) j++; else break;
				}
			}

			
		}
		return s_min
		/ (s_max + (doc1.docLength() - s_c1) + (doc2.docLength() - s_c2));
	}*/
}
