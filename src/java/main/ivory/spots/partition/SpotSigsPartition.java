package ivory.spots.partition;

public class SpotSigsPartition {

	public int id, begin, end;
	public int totalDocs, maxDocLength;
	
	public SpotSigsPartition(int idx, int begin, int end) {
		this.id = idx;
		this.begin = begin;
		this.end = end;
		this.totalDocs = 0;
		this.maxDocLength = 0;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String s = "#"+id+"("+begin+", "+end+")";
		//- TotalDocs="+totalDocs+" MaxLength="+maxDocLength;
		return s;
	}
	
}
