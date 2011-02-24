package ivory.pwsim.score;

public class Pmra extends ScoringModel {

	private double lambda = 0.022;

	private double mu = 0.013;

	private double lmu = lambda - mu;

	private double mol = mu / lambda;

	public float computeScore(int q_tf, int d_tf, int q_len, int doc_len) {

		float e1 = 1 / (1 + (float) Math.exp(lmu * q_len) * (float) Math.pow(mol, q_tf - 1));
		float e2 = 1 / (1 + (float) Math.exp(lmu * doc_len) * (float) Math.pow(mol, d_tf - 1));

		float idf = (float) Math.log((mDocCount - mDF + 0.5f) / (mDF + 0.5f));

		return e1 * e2 * idf;
	}

	public float computeDocumentWeight(int tf, int len) {
		throw new UnsupportedOperationException();
	}
	
	public float computeQueryWeight(int tf, int len) {
		throw new UnsupportedOperationException();
	}

}