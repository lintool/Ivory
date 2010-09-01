package ivory.pwsim.score;

public abstract class ScoringModel {

	protected float mAvgDocLength;
	protected int mDocCount;
	protected int mDF;

	public void setAvgDocLength(float avgdl) {
		mAvgDocLength = avgdl;
	}

	public void setDocCount(int n) {
		mDocCount = n;
	}

	public void setDF(int df) {
		mDF = df;
	}

	public abstract float computeScore(int qtf, int dtf, int qlen, int dlen);

	public float computeScore(String term, int qtf, int dtf, int qlen, int dlen) {
		return computeScore(qtf, dtf,  qlen, dlen);
	}

	public abstract float computeDocumentWeight(int dtf, int dlen);
	
	public float computeDocumentWeight(String term, int dtf, int dlen) {
		return computeDocumentWeight(dtf, dlen);
	}

	public abstract float computeQueryWeight(int qtf, int qlen);
	
	public float computeQueryWeight(String term, int qtf, int qlen) {
		return computeDocumentWeight(qtf, qlen);
	}

}
