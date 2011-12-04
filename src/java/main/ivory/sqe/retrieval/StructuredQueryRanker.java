package ivory.sqe.retrieval;

import ivory.core.RetrievalEnvironment;
import ivory.core.exception.ConfigurationException;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.score.BM25ScoringFunction;
import ivory.smrf.retrieval.Accumulator;
import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;
import org.apache.hadoop.fs.FileSystem;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umd.cloud9.collection.DocnoMapping;

public class StructuredQueryRanker {
  private RetrievalEnvironment env;
  private Accumulator[] accumulators = null;
  private final PriorityQueue<Accumulator> sortedAccumulators = new PriorityQueue<Accumulator>();
  private final int numResults;
  private HashMap<String, Accumulator[]> results;

  public StructuredQueryRanker(String indexPath, FileSystem fs, int numResults) throws IOException,
      ConfigurationException {
    this.env = new RetrievalEnvironment(indexPath, fs);
    env.initialize(true);

    this.numResults = numResults;
    results = new HashMap<String, Accumulator[]>();
    
    // Create single pool of reusable accumulators.
    accumulators = new Accumulator[numResults + 1];
    for (int i = 0; i < numResults + 1; i++) {
      accumulators[i] = new Accumulator(0, 0.0f);
    }
  }
  
  

  public Accumulator[] rank(String qid, JSONObject query, int queryLength) {
    GlobalEvidence globalEvidence = new GlobalEvidence(env.getDocumentCount(), env.getCollectionSize(), queryLength);
	  
    PostingsReaderWrapper structureReader;
	try {
		structureReader = new PostingsReaderWrapper(query, env, new BM25ScoringFunction(), globalEvidence);
	} catch (JSONException e) {
		e.printStackTrace();
		throw new RuntimeException(e);
	}
	////System.out.println("Ranker created.");
    
    sortedAccumulators.clear();
    Accumulator a = accumulators[0];

    // Score that must be achieved to enter result set.
    double scoreThreshold = Double.NEGATIVE_INFINITY;

    int docno = Integer.MAX_VALUE;
    int nextDocno = structureReader.getNextCandidate(docno);
    if(nextDocno < docno){
    	docno = nextDocno;
    }
    ////System.out.println("Advance to docno " + docno);
    int cnt = 0;
    while (docno < Integer.MAX_VALUE) {
      float score = 0.0f;

      // Document-at-a-time scoring.
      score = structureReader.computeScore(docno);
      cnt++;
      if (cnt % 10000 == 0) {
    	  System.out.println(cnt + " docs processed = "+docno);
      }
      // Keep track of numResults best accumulators.
      if (score > scoreThreshold) {
        a.docno = docno;
        a.score = score;
        sortedAccumulators.add(a);

        if (sortedAccumulators.size() == numResults + 1) {
          a = sortedAccumulators.poll();
          scoreThreshold = sortedAccumulators.peek().score;
        } else {
          a = accumulators[sortedAccumulators.size()];
        }
      }

      // Advance to next document
      docno = Integer.MAX_VALUE;
      nextDocno = structureReader.getNextCandidate(docno);
      if(nextDocno < docno){
      	docno = nextDocno;
      }
    }

    // Grab the accumulators off the stack, in (reverse) order.
    Accumulator[] results = new Accumulator[Math.min(numResults, sortedAccumulators.size())];
    for (int i = 0; i < results.length; i++) {
      results[results.length - 1 - i] = sortedAccumulators.poll();
    }
    
    this.results.put(qid, results);

    return results;
  }

  public DocnoMapping getDocnoMapping() throws IOException {
    return env.getDocnoMapping();
  }



public Accumulator[] getResults(String queryID) {
	return results.get(queryID);
}


}
