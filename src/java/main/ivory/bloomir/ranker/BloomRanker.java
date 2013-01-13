package ivory.bloomir.ranker;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.util.map.HMapIV;

import ivory.bloomir.data.CompressedPostings;
import ivory.bloomir.data.CompressedPostingsIO;
import ivory.bloomir.data.Signature;
import ivory.bloomir.data.SignatureIO;
import ivory.bloomir.util.QueryUtility;
import ivory.bloomir.util.DocumentUtility;
import ivory.bloomir.util.OptionManager;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.stat.SpamPercentileScore;

/**
 * A driver that loads the full PForDelta compressed postings lists as
 * well as Bloom filters into memory and evaluates a given set of
 * queries.
 *
 * @author Nima Asadi
 */
public class BloomRanker {
  private static final Logger LOGGER = Logger.getLogger(BloomRanker.class);
  private final int[] decomp = new int[CompressedPostings.getBlockSize()];

  private CompressedPostings[] postings;  //Postings lists
  private int[] dfs;  //Df values
  private Signature[] filters;  //Signatures

  public BloomRanker(String postingsIndex, String filterIndex, FileSystem fs)
    throws IOException, ClassNotFoundException {
    int numberOfTerms = CompressedPostingsIO.readNumberOfTerms(postingsIndex, fs);
    postings = new CompressedPostings[numberOfTerms + 1];
    dfs = new int[numberOfTerms + 1];
    filters = new Signature[numberOfTerms + 1];
    CompressedPostingsIO.loadPostings(postingsIndex, fs, postings, dfs);
    SignatureIO.loadSignatures(filterIndex, fs, filters);
  }

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

    //Find the minimum Df value
    int minDf = Integer.MAX_VALUE;
    int indexOfMinDf = -1;
    for (int i = 0; i < query.length; i++) {
      int df = dfs[query[i]];
      if (df < minDf) {
        minDf = df;
        indexOfMinDf = i;
      }
    }

    //Prepare a list of signatures to perform membership tests against
    Signature[] list = new Signature[query.length - 1];
    int pos = 0;
    for (int i = 0; i < query.length; i++) {
      if(i != indexOfMinDf) {
        list[pos] = filters[query[i]];
        pos++;
      }
    }

    CompressedPostings ps = postings[query[indexOfMinDf]];
    int cnt = 0;
    int nbBlocks = ps.getBlockCount();
    for (int i = 0; i < nbBlocks; i++) {
      int bSize = ps.decompressBlock(decomp, i);
      int docno = 0;
      boolean pick = true;

      for(int j = 0; j < bSize; j++) {
        docno += decomp[j];
        pick = true;
        for(int p = 0; p < list.length; p++) {
          if(!list[p].membershipTest(docno)) {
            pick = false;
            break;
          }
        }
        if(pick) {
          results[cnt] = docno;
          cnt++;
          if(cnt >= hits) {
            return results;
          }
        }
      }
    }
    return results;
  }

  public static void main(String[] args) throws Exception {
    OptionManager options = new OptionManager(BloomRanker.class.getName());
    options.addOption(OptionManager.INDEX_ROOT_PATH, "path", "index root", true);
    options.addOption(OptionManager.POSTINGS_ROOT_PATH, "path", "Non-positional postings root", true);
    options.addOption(OptionManager.BLOOM_ROOT_PATH, "path", "Bloom filters root", true);
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
    String bloomIndexPath = options.getOptionValue(OptionManager.BLOOM_ROOT_PATH);
    String queryPath = options.getOptionValue(OptionManager.QUERY_PATH);
    boolean writeOutput = options.foundOption(OptionManager.OUTPUT_PATH);
    int hits = 10000;
    if(options.foundOption(OptionManager.HITS)) {
      hits = Integer.parseInt(options.getOptionValue(OptionManager.HITS));
    }

    FileSystem fs = FileSystem.get(new Configuration());
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    env.initialize(true);
    DocnoMapping docnoMapping = env.getDocnoMapping();

    BloomRanker ranker = new BloomRanker(postingsIndexPath, bloomIndexPath, fs);

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
