package ivory.bloomir.ranker;

import ivory.bloomir.data.CompressedPostings;
import ivory.bloomir.data.CompressedPostingsIO;
import ivory.bloomir.util.DocumentUtility;
import ivory.bloomir.util.OptionManager;
import ivory.bloomir.util.QueryUtility;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.stat.SpamPercentileScore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import tl.lin.data.map.HMapIV;
import edu.umd.cloud9.collection.DocnoMapping;

public class SmallAdaptiveRanker {
  private CompressedPostings[] postings;  //Postings lists
  private int[] dfs;  //Df values

  public SmallAdaptiveRanker(String postingsIndex, FileSystem fs)
    throws IOException, ClassNotFoundException {
    int numberOfTerms = CompressedPostingsIO.readNumberOfTerms(postingsIndex, fs);
    postings = new CompressedPostings[numberOfTerms + 1];
    dfs = new int[numberOfTerms + 1];
    CompressedPostingsIO.loadPostings(postingsIndex, fs, postings, dfs);
  }

  /**
   * Performs a galloping-search followed by a standard binary search.
   *
   * @param post Compressed postings list
   * @param p An array that holds the decompressed postings for a term.
   *          This helps remember the decompressed postings and therefore
   *          avoids redundancy.
   * @param low Low index for binary search
   * @param high High index for binary search
   * @param key Value to search for
   * @return An integer array. The elements of this array are as follows:
   *         [0]: the new 'low' index
   *         [1]: the value of the key if it was found. This element
   *              is meaningless otherwise.
   */
  private int[] binarySearch(CompressedPostings post, int[] p, int low, int high, int key) {
    int lo = low;
    int hi = high;

    //Gallop
    int current = low;
    boolean first = true;
    while(true) {
      if(first) {
        first = false;
      } else {
        if(current == 0) {
          current = 1;
        } else {
          current *= 2;
        }
      }

      if(current > high) {
        hi = high;
        break;
      }

      // If the postings from this block are not decompressed
      if(p[current] == 0) {
        decompress(post, p, post.getBlockNumber(current));
      }

      if(p[current] < key) {
        lo = current;
      } else if(p[current] > key) {
        hi = current;
        break;
      } else {
        return new int[] {current + 1, p[current]};
      }
    }

    while (lo <= hi) {
      int mid = lo + (hi - lo) / 2;

      if(p[mid] == 0) {
        decompress(post, p, post.getBlockNumber(mid));
      }
      if (key < p[mid]) {
        hi = mid - 1;
      } else if (key > p[mid]) {
        lo = mid + 1;
      } else {
        return new int[]{mid + 1, key};
      }
    }
    if(lo <= high) {
      return new int[]{lo, p[lo]};
    }

    return new int[] {lo, p[high]};
  }

  private final int[] decomp = new int[CompressedPostings.getBlockSize()];
  public void decompress(CompressedPostings p, int[] ds, int blockNumber) {
    int blSize = p.decompressBlock(decomp, blockNumber);
    int pos = p.getBlockStartIndex(blockNumber);
    int val = 0;

    for(int k = 0; k < blSize; k++) {
      val += decomp[k];
      ds[pos++] = val;
    }
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

    int[] myLow = new int[query.length];
    int[] myHigh = new int[query.length];
    int[][] ds = new int[query.length][];

    for(int i = 0; i < query.length; i++) {
      myHigh[i] = dfs[query[i]] - 1;
      myLow[i] = 0;
      ds[i] = new int[myHigh[i] + 1];
    }

    decompress(postings[query[0]], ds[0], 0);
    int cnt = 0;
    int value = ds[0][0];
    int index = 1;
    int found = 1;
    while(true) {
      if(index >= query.length) {
        index = 0;
      }

      if(myLow[index] > myHigh[index]) {
        return results;
      }

      int[] r = binarySearch(postings[query[index]], ds[index], myLow[index], myHigh[index], value);
      if(r[1] == value) {
        found++;
        if(found == query.length) {
          results[cnt] = value;
          cnt++;
          if(cnt >= hits) {
            return results;
          }
          found = 1;
          int next = r[0];
          if(next > myHigh[index]) {
            return results;
          }
          if(ds[index][next] == 0) {
            decompress(postings[query[index]], ds[index], postings[query[index]].getBlockNumber(next));
          }
          value = ds[index][next];
        }
      } else {
        found = 1;
        value = r[1];
      }

      if((r[0] == myLow[index] && r[0] == myHigh[index]) || r[0] > myHigh[index]) {
        myLow[index] = myHigh[index] + 1;
      } else {
        myLow[index] = r[0];
      }
      index++;
    }
  }

  public static void main(String[] args) throws Exception {
    OptionManager options = new OptionManager(SmallAdaptiveRanker.class.getName());
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

    SmallAdaptiveRanker ranker = new SmallAdaptiveRanker(postingsIndexPath, fs);

    //Parse queries and find integer codes for the query terms.
    HMapIV<int[]> queries = QueryUtility.queryToIntegerCode(env, queryPath);

    //Evaluate queries and/or write the results to an output file
    int[] newDocidsLookup = null;
    FSDataOutputStream output = null;
    if(writeOutput) {
      final SpamPercentileScore spamScores = new SpamPercentileScore();
      spamScores.initialize(options.getOptionValue(OptionManager.SPAM_PATH), fs);
      newDocidsLookup = DocumentUtility.reverseLookupSpamSortedDocids(DocumentUtility.spamSortDocids(spamScores));

      output = fs.create(new Path(options.getOptionValue(OptionManager.OUTPUT_PATH)));
      output.write(("<parameters>\n").getBytes());
    }

    //    DocnoMapping docnoMapping = env.getDocnoMapping();
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
