package ivory.bloomir.ranker;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.util.map.HMapIV;

import ivory.bloomir.data.CompressedPostings;
import ivory.bloomir.data.CompressedPostingsIO;
import ivory.bloomir.util.DocumentUtility;
import ivory.bloomir.util.QueryUtility;
import ivory.bloomir.util.OptionManager;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.stat.SpamPercentileScore;

public class LinearMergingRanker {
  private static final Logger LOGGER = Logger.getLogger(LinearMergingRanker.class);

  private CompressedPostings[] postings;  //Postings lists
  private int[] dfs;  //Df values

  public LinearMergingRanker(String postingsIndex, FileSystem fs)
    throws IOException, ClassNotFoundException {
    int numberOfTerms = CompressedPostingsIO.readNumberOfTerms(postingsIndex, fs);
    postings = new CompressedPostings[numberOfTerms + 1];
    dfs = new int[numberOfTerms + 1];
    CompressedPostingsIO.loadPostings(postingsIndex, fs, postings, dfs);
  }

  // Retrieves top documents for a given query
  private final int[] decomp = new int[CompressedPostings.getBlockSize()];
  public int[] rank(int[] query, int hits) throws IOException {
    int[] results = new int[hits];

    //If the lenght of the query is one, just return the first n documents
    //in the postings list.
    if(query.length == 1) {
      CompressedPostings ps = postings[query[0]];
      int df = dfs[query[0]];
      if(hits > df) {
        hits = df;
      }
      int nbBlocks = ps.getBlockCount();
      int cnt = 0;
      for(int i = 0; i < nbBlocks; i++) {
        int bSize = ps.decompressBlock(decomp, i);
        int docno = 0;

        for(int j = 0; j < bSize; j++) {
          docno += decomp[j];
          results[cnt] = docno;
          cnt++;
          if(cnt >= hits) {
            return results;
          }
        }
      }
      return results;
    }

    int[] values = new int[query.length];
    int[] position = new int[query.length];
    int[][] decomps = new int[query.length][CompressedPostings.getBlockSize()];

    for(int i = 0; i < query.length; i++) {
      postings[query[i]].decompressBlock(decomps[i], 0);
      values[i] = decomps[i][0];
    }

    int cnt = 0;
    int minimum = 0;
    boolean available = true;
    while(available) {
      minimum = Integer.MAX_VALUE;
      for(int i = 0; i < query.length; i++) {
        if(values[i] < minimum) {
          minimum = values[i];
        }
      }

      int found = 0;
      for(int i = 0; i < query.length; i++) {
        if(values[i] == minimum) {
          found++;
          position[i]++;

          if(position[i] >= dfs[query[i]]) {
            available = false;
          }

          if(postings[query[i]].isFirstElementInBlock(position[i])) {
            postings[query[i]].decompressBlock(decomps[i], postings[query[i]].getBlockNumber(position[i]));
            values[i] = decomps[i][0];
          } else {
            values[i] += decomps[i][postings[query[i]].getPositionInBlock(position[i])];
          }
        }
      }

      if(found == query.length) {
        results[cnt] = minimum;
        cnt++;
        if(cnt >= hits) {
          return results;
        }
      }
    }
    return results;
  }

  public static void main(String[] args) throws Exception {
    OptionManager options = new OptionManager(LinearMergingRanker.class.getName());
    options.addOption(OptionManager.INDEX_ROOT_PATH, "path", "index root", true);
    options.addOption(OptionManager.POSTINGS_ROOT_PATH, "path", "Non-positional postings root", true);
    options.addOption(OptionManager.QUERY_PATH, "path", "XML query", true);
    options.addOption(OptionManager.OUTPUT_PATH, "path", "output path (Optional)", false);
    options.addOption(OptionManager.SPAM_PATH, "path", "spam percentile score (Optional)", false);
    options.addOption(OptionManager.HITS, "integer", "number of hits (default: 10,000)", false);
    options.addDependency(OptionManager.OUTPUT_PATH, OptionManager.SPAM_PATH);

    try {
      options.parse(args);
    } catch(Exception exp) {
      return;
    }

    String indexPath = options.getOptionValue(OptionManager.INDEX_ROOT_PATH);
    String postingsIndexPath = options.getOptionValue(OptionManager.POSTINGS_ROOT_PATH);
    String queryPath = options.getOptionValue(OptionManager.QUERY_PATH);
    boolean writeOutput = options.foundOption(OptionManager.OUTPUT_PATH);
    int hits = 10000;
    if(options.foundOption(OptionManager.HITS)){
      hits = Integer.parseInt(options.getOptionValue(OptionManager.HITS));
    }

    FileSystem fs = FileSystem.get(new Configuration());
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    env.initialize(true);
    DocnoMapping docnoMapping = env.getDocnoMapping();

    LinearMergingRanker ranker = new LinearMergingRanker(postingsIndexPath, fs);

    //Parse queries and find integer codes for the query terms.
    HMapIV<int[]> queries = QueryUtility.queryToIntegerCode(env, queryPath);

    //Evaluate queries and/or write the results to an output file
    int[] newDocidsLookup = null;
    FSDataOutputStream output =null;
    if(writeOutput) {
      final SpamPercentileScore spamScores = new SpamPercentileScore();
      spamScores.initialize(options.getOptionValue(OptionManager.SPAM_PATH), fs);
      newDocidsLookup = DocumentUtility.reverseLookupSpamSortedDocids(DocumentUtility.spamSortDocids(spamScores));

      output = fs.create(new Path(options.getOptionValue(OptionManager.OUTPUT_PATH)));
      output.write(("<parameters>\n").getBytes());
    }

    for (int qid: queries.keySet()) {
      int[] qterms = queries.get(qid);
      if(qterms.length == 0) {
        continue;
      }
      long start = System.nanoTime();
      int[] docs = ranker.rank(qterms, hits);
      System.out.println(System.nanoTime() - start);
      if(writeOutput) {
        for(int i = 0; i < docs.length; i++) {
          if(docs[i] != 0) {
            output.write(("<judgment qid=\"" + qid +
                          "\" doc=\"" + docnoMapping.getDocid(newDocidsLookup[docs[i]]) +
                          "\" grade=\"0\" />\n").getBytes());
          }
        }
      }
    }

    if(writeOutput) {
      output.write(("</parameters>\n").getBytes());
      output.close();
    }
  }
}
