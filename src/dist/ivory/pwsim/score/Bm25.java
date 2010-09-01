package ivory.pwsim.score;

public class Bm25 extends ScoringModel {

	private float k_1 = 1.2f;

	private float k_3 = 1000f;

	private float b = 0.75f;

	public float computeScore(int q_tf, int doc_tf, int q_len, int doc_len) {

		// This definition of K is different from standard BM25: it has an
		// additional d_tf at the end. Empirically, the extra factor increases
		// effectiveness.

		float K = k_1 * ((1 - b) + b * (doc_len / mAvgDocLength)) + doc_tf;
		float rsj = (float) Math.log((mDocCount - mDF + 0.5f) / (mDF + 0.5f));
		float val = ((k_1 + 1.0f) * doc_tf / (K + doc_tf)) * ((k_3 + 1.0f) * q_tf) / (k_3 + q_tf);

		return rsj * val;
	}

	public float computeDocumentWeight(int doc_tf, int doc_len) {
		float K = k_1 * ((1 - b) + b * (doc_len / mAvgDocLength)) + doc_tf;
		float rsj = (float) Math.log((mDocCount - mDF + 0.5f) / (mDF + 0.5f));
		float val = ((k_1 + 1.0f) * doc_tf / (K + doc_tf));

		return rsj * val;
	}
	
	public float computeDocumentWeight(float doc_tf, float df, int doc_len) {
		float K = k_1 * ((1 - b) + b * (doc_len / mAvgDocLength)) + doc_tf;
		float rsj = (float) Math.log((mDocCount - df + 0.5f) / (df + 0.5f));
		float val = ((k_1 + 1.0f) * doc_tf / (K + doc_tf));

		return rsj * val;
	}

	public float computeQueryWeight(int q_tf, int q_len) {
		return ((k_3 + 1.0f) * q_tf) / (k_3 + q_tf);
	}

}
