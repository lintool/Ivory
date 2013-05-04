package ivory.regression.cikm2012;

import static org.junit.Assert.assertEquals;
import ivory.bloomir.ranker.BloomRanker;
import ivory.bloomir.ranker.SmallAdaptiveRanker;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class RelativeRecallUtil {
  private static final String IVORY_INDEX_PATH = "/scratch0/indexes/adhoc/clue.en.01.nopos/";

  // Index paths used in CIKM experiments
  private static final String SPAM_PATH = "/scratch0/indexes/adhoc/CIKM2012/docscores-spam.dat.en.01";
  private static final String CIKM_STANDARD_INDEX = "/scratch0/indexes/adhoc/CIKM2012/standard/";
  private static final String CIKM_BLOOM_INDEX_BASE_PATH = "/scratch0/indexes/adhoc/CIKM2012/bloom-";
  private static final String CIKM_QUERIES = "data/clue/queries.web09.xml";
  private static final String CIKM_QRELS = "data/clue/qrels.web09catB.txt";

  /**
   * @param r Number of bits per element
   * @param k Number of Hash functions
   * @param recall Relative recall value
   */
  public static void runRegression(int r, int k, int recall) throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    File postingOutput = File.createTempFile("bloomirPO", null);
    File bloomOutput = File.createTempFile("bloomirBO", null);

    // Load Qrels into a map
    Map<String, Set<String>> qrels = Maps.newHashMap();
    FSDataInputStream qrelsInput = fs.open(new Path(CIKM_QRELS));
    LineReader reader = new LineReader(qrelsInput);
    Text line = new Text();
    while (reader.readLine(line) > 0) {
      String[] tokens = line.toString().split("\\s+");
      String qid = tokens[0];
      String docid = tokens[2];
      int grade = Integer.parseInt(tokens[3]);

      if (grade <= 0) {
        continue;
      }

      if(!qrels.containsKey(qid)) {
        Set<String> ids = Sets.newHashSet();
        qrels.put(qid, ids);
      }
      qrels.get(qid).add(docid);
    }
    reader.close();
    qrelsInput.close();

    // Run Small Adaptive baseline
    String[] paramsSARanker = new String[] {
      "-index", RelativeRecallUtil.IVORY_INDEX_PATH,
      "-posting", RelativeRecallUtil.CIKM_STANDARD_INDEX,
      "-query", RelativeRecallUtil.CIKM_QUERIES,
      "-spam", RelativeRecallUtil.SPAM_PATH,
      "-output", postingOutput.getPath(),
      "-hits", "10000"
    };
    SmallAdaptiveRanker.main(paramsSARanker);
    System.gc();

    // Load the output into a Map
    Map<String, Set<String>> saRelOutput = Maps.newHashMap();
    FSDataInputStream saInput = fs.open(new Path(postingOutput.getPath()));
    reader = new LineReader(saInput);
    while (reader.readLine(line) > 0) {
      String s = line.toString();
      if (s.startsWith("<judgment")) {
        String[] tokens = s.split("\"");
        String docid = tokens[3];
        String qid = tokens[1];

        // Ignore topics with no qrels
        if(!qrels.containsKey(qid)) {
          continue;
        }

        // Discard non-relevant documents
        if(!qrels.get(qid).contains(docid)) {
          continue;
        }

        if(!saRelOutput.containsKey(qid)) {
          Set<String> ids = Sets.newHashSet();
          saRelOutput.put(qid, ids);
        }
        saRelOutput.get(qid).add(docid);
      }
    }
    reader.close();
    saInput.close();

    // Bloom Retrieval
    String[] paramsBloomRanker = new String[] {
      "-index", RelativeRecallUtil.IVORY_INDEX_PATH,
      "-posting", RelativeRecallUtil.CIKM_STANDARD_INDEX,
      "-bloom", RelativeRecallUtil.CIKM_BLOOM_INDEX_BASE_PATH + r + "-" + k + "/",
      "-query", RelativeRecallUtil.CIKM_QUERIES,
      "-spam", RelativeRecallUtil.SPAM_PATH,
      "-output", bloomOutput.getPath(),
      "-hits", "10000"
    };
    BloomRanker.main(paramsBloomRanker);
    System.gc();

    // Compute relative recall of relevant documents
    Map<String, Integer> counter = Maps.newHashMap();
    FSDataInputStream bInput = fs.open(new Path(bloomOutput.getPath()));
    reader = new LineReader(bInput);
    while (reader.readLine(line) > 0) {
      String s = line.toString();
      if (s.startsWith("<judgment")) {
        String[] tokens = s.split("\"");
        String docid = tokens[3];
        String qid = tokens[1];

        // Ignore topics with no qrels
        if(!saRelOutput.containsKey(qid)) {
          continue;
        }

        if(saRelOutput.get(qid).contains(docid)) {
          if(!counter.containsKey(qid)) {
            counter.put(qid, 0);
          }
          counter.put(qid, counter.get(qid) + 1);
        }
      }
    }
    bInput.close();

    double avg = 0.0;
    for(String qid: counter.keySet()) {
      avg += (counter.get(qid) /
              ((double) saRelOutput.get(qid).size()));
    }
    avg /= counter.size();
    assertEquals(recall, (int) (avg * 100));

    fs.delete(new Path(postingOutput.getPath()), true);
    fs.delete(new Path(bloomOutput.getPath()), true);
  }
}
