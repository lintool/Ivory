package ivory.pwsim.score;

public class Lucene extends ScoringModel {

	public float computeScore(int qtf, int dtf, int qlen, int dlen) {
		return computeLuceneTF(dtf) * computeLuceneIDF() * (float) (1 / Math.sqrt(dlen));
	}

	public float computeDocumentWeight(int dtf, int dlen) {
		throw new UnsupportedOperationException();
	}

	public float computeQueryWeight(int qtf, int qlen) {
		throw new UnsupportedOperationException();
	}

	private float computeLuceneTF(int tf) {
		return (float) Math.sqrt(tf);
	}

	private float computeLuceneIDF() {
		return (float) Math.pow(1 + Math.log10(mDocCount / (mDF + 1)), 2.0d);
	}

}
