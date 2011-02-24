package ivory.pwsim.score;

public class Jaccard extends ScoringModel {

	public float computeScore(int qtf, int dtf, int qlen, int dlen) {
		return 1.0f / (qlen + dlen);
	}

	public float computeDocumentWeight(int dtf, int dlen) {
		return 1.0f;
	}
	
	public float computeQueryWeight(int qtf, int qlen) {
		return 1.0f;
	}

}
