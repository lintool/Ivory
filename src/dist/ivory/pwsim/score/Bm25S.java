package ivory.pwsim.score;

public class Bm25S extends ScoringModel {

	public float computeScore(int qtf, int dtf, int qlen, int dlen) {
		float idf = (float) Math.log((mDocCount - mDF + 0.5f) / (mDF + 0.5f));
		return qtf / (qtf + 0.5f + 1.5f * qlen / mAvgDocLength) * dtf
				/ (dtf + 0.5f + 1.5f * dlen / mAvgDocLength) * idf;
	}

	public float computeDocumentWeight(int dtf, int dlen) {
		float idf = (float) Math.sqrt(Math.log((mDocCount - mDF + 0.5f) / (mDF + 0.5f)));
		return dtf / (dtf + 0.5f + 1.5f * dlen / mAvgDocLength) * idf;
	}

	public float computeQueryWeight(int qtf, int qlen) {
		float idf = (float) Math.sqrt(Math.log((mDocCount - mDF + 0.5f) / (mDF + 0.5f)));
		return qtf / (qtf + 0.5f + 1.5f * qlen / mAvgDocLength) * idf;
	}

}
