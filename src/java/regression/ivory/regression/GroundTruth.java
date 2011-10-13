package ivory.regression;

import static org.junit.Assert.assertEquals;
import ivory.core.eval.GradedQrels;
import ivory.core.eval.Qrels;
import ivory.core.eval.RankedListEvaluator;
import ivory.smrf.retrieval.Accumulator;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;


import edu.umd.cloud9.collection.DocnoMapping;

public class GroundTruth {
	private static final Logger sLogger = Logger.getLogger(GroundTruth.class);

	public static enum Metric {
		AP, P10, NDCG20
	}

	private String mModel;
	private Metric mMetric;
	private int mNumTopics;

	private Map<String, Float> scores;
	private float overallScore;

	public GroundTruth(Metric m, int numTopics, String[] arr, float s) {
		this(null, m, numTopics, arr, s);
	}

	public GroundTruth(String model, Metric m, int numTopics, String[] arr, float s) {
		mModel = model;
		mMetric = m;
		mNumTopics = numTopics;
		overallScore = s;

		loadScores(arr);
	}

	public Metric getMetric() {
		return mMetric;
	}

	public String getModel() {
		return mModel;
	}

	public void loadScores(String[] arr) {
		scores = new HashMap<String, Float>();

		for (int i = 0; i < arr.length; i += 2) {
			scores.put(arr[i], Float.parseFloat(arr[i + 1]));
		}
	}

	public void verify(Map<String, Accumulator[]> results, DocnoMapping mapping, Qrels qrels) {
		if (mMetric.equals(Metric.AP)) {
			verifyAP(results, mapping, qrels);
		} else if (mMetric.equals(Metric.P10)) {
			verifyP10(results, mapping, qrels);
		} else if (mMetric.equals(Metric.NDCG20)){
			verifyNDCG20(results, mapping, (GradedQrels)qrels);
		} 
		else {
			throw new RuntimeException("Unknown metric: Don't know how to verify!");
		}
	}

	private void verifyNDCG20(Map<String, Accumulator[]> results, DocnoMapping mapping, GradedQrels qrels) {
		float ndcgSum = 0;

		for (String qid : results.keySet()) {
			float ndcg = (float) RankedListEvaluator.computeNDCG(20, results.get(qid), mapping, qrels.getReldocsForQid(qid, true));

			ndcgSum += ndcg;

			String s = mModel == null ? "" : "model " + mModel + ": ";
			sLogger.info(s + "verifying ndcg for qid " + qid);

			assertEquals(scores.get(qid), ndcg, 10e-4);
		}		

		float NDCG =  (float) RankedListEvaluator.roundTo4SigFigs(ndcgSum / (float) mNumTopics);

		assertEquals(overallScore, NDCG, 10e-4);

	}
	
	private void verifyAP(Map<String, Accumulator[]> results, DocnoMapping mapping, Qrels qrels) {
		float apSum = 0;
		for (String qid : results.keySet()) {
			float ap = (float) RankedListEvaluator.computeAP(results.get(qid), mapping, qrels
					.getReldocsForQid(qid));

			apSum += ap;

			String s = mModel == null ? "" : "model " + mModel + ": ";
			sLogger.info(s + "verifying average precision for qid " + qid);

			assertEquals(scores.get(qid), ap, 10e-4);
		}

		float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / (float) mNumTopics);

		assertEquals(overallScore, MAP, 10e-4);
	}

	private void verifyP10(Map<String, Accumulator[]> results, DocnoMapping mapping, Qrels qrels) {
		float p10Sum = 0;
		for (String qid : results.keySet()) {
			float p10 = (float) RankedListEvaluator.computePN(10, results.get(qid), mapping,
					qrels.getReldocsForQid(qid));

			p10Sum += p10;

			String s = mModel == null ? "" : "model " + mModel + ": ";
			sLogger.info(s + "verifying average precision for qid " + qid);

			assertEquals(scores.get(qid), p10, 10e-4);
		}

		float P10 = (float) RankedListEvaluator.roundTo4SigFigs(p10Sum / (float) mNumTopics);

		assertEquals(overallScore, P10, 10e-4);
	}
}
