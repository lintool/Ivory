package ivory.pwsim.score;

public class TfIdf extends ScoringModel {

	public float computeScore(int qtf, int dtf, int qlen, int dlen) {
		float idf = (float) Math.log((mDocCount - mDF + 0.5f) / (mDF + 0.5f));
		return qtf * dtf * idf;
	}

	public float computeDocumentWeight(int dtf, int dlen) {
		float idf = (float) Math.log((mDocCount - mDF + 0.5f) / (mDF + 0.5f));
		return dtf * idf;
	}

	public float computeQueryWeight(int qtf, int qlen) {
		return (float) qtf;
	}
}
