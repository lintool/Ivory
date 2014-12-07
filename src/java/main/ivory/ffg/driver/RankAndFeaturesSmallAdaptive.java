package ivory.ffg.driver;

import ivory.bloomir.util.DocumentUtility;
import ivory.bloomir.util.OptionManager;
import ivory.bloomir.util.QueryUtility;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.stat.SpamPercentileScore;
import ivory.ffg.data.CompressedPositionalPostings;
import ivory.ffg.feature.Feature;
import ivory.ffg.stats.GlobalStats;
import ivory.ffg.util.FeatureUtility;
import ivory.ffg.util.QrelUtility;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import tl.lin.data.map.HMapIF;
import tl.lin.data.map.HMapII;
import tl.lin.data.map.HMapIV;

public class RankAndFeaturesSmallAdaptive {
  private static final Logger LOGGER = Logger.getLogger(RankAndFeaturesSmallAdaptive.class);

  private HMapIV<CompressedPositionalPostings> postings;  //Postings lists
  private GlobalStats stats;
  private HMapII dfs;  //Df values
  private HMapII docLengths;

  private RetrievalEnvironment env;
  private FileSystem fs;
  private int[] newDocids;

  public RankAndFeaturesSmallAdaptive(RetrievalEnvironment env, FileSystem fs) {
    this.env = env;
    this.fs = fs;
  }

  public void prepareStats(HMapIF idfs, HMapIF cfs) throws Exception {
    stats = new GlobalStats(idfs, cfs,
                            (int) env.getDocumentCount(), env.getCollectionSize(),
                            (float) env.getCollectionSize() / (float) env.getDocumentCount(),
                            (float) env.getDefaultDf(), (float) env.getDefaultCf());
  }

  private void preparePostings(String postingsPath) throws Exception {
    postings = new HMapIV<CompressedPositionalPostings>();
    dfs = new HMapII();
    docLengths = new HMapII();

    FSDataInputStream input = fs.open(new Path(postingsPath));
    int termid = input.readInt();
    while(termid != -1) {
      dfs.put(termid, input.readInt());
      postings.put(termid, CompressedPositionalPostings.readInstance(input));
      termid = input.readInt();
    }

    int nbDocLengths = input.readInt();
    for(int i = 0; i < nbDocLengths; i++) {
      docLengths.put(input.readInt(), input.readInt());
    }

    input.close();
  }

  public int[] binarySearch(CompressedPositionalPostings post, int[] p, int low, int high, int key) {
    int lo = low;
    int hi = high;

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

      if(p[current] == 0) {
        decompress(post, p, post.getBlockNumber(current));
      }

      if(p[current] < key) {
        lo = current;
      } else if(p[current] > key) {
        hi = current;
        break;
      } else {
        return new int[] {current + 1, p[current], current};
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
        return new int[]{mid + 1, key, mid};
      }
    }
    if(lo <= high) {
      return new int[]{lo, p[lo], lo};
    }

    return new int[] {lo, p[high], high};
  }

  private final int[] decomp = new int[CompressedPositionalPostings.getBlockSize()];
  public void decompress(CompressedPositionalPostings p, int[] ds, int blockNumber) {
    int blSize = p.decompressBlock(decomp, blockNumber);
    int pos = p.getBlockStartIndex(blockNumber);
    int val = 0;

    for(int k = 0; k < blSize; k++) {
      val += decomp[k];
      ds[pos++] = val;
    }
  }

  public float[][] extract(int[] query, int hits,Feature[] features,
                           int qid, boolean writeOutput, int[] docidLookup,
                           FSDataOutputStream output) throws IOException {
    float[][] fvalues = new float[hits][features.length];
    int[][] pos = new int[query.length][];

    //If the lenght of the query is one, just return the first n documents
    //in the postings list.
    if(query.length == 1) {
      CompressedPositionalPostings ps = postings.get(query[0]);
      int df = dfs.get(query[0]);
      if(hits > df) {
        hits = df;
      }
      int nbBlocks = ps.getBlockCount();
      int cnt = 0;
      for(int i = 0; i < nbBlocks; i++) {
        int bSize = ps.decompressBlock(decomp, i);
        int docno = 0;

        //extract features
        for(int j = 0; j < bSize; j++) {
          docno += decomp[j];

          pos[0] = ps.decompressPositions(cnt);
          int dl = docLengths.get(docno);
          for(int fid = 0; fid < features.length; fid++) {
            fvalues[cnt][fid] = features[fid].computeScoreWithMiniIndexes(pos, query, dl, stats);
          }
          if(writeOutput) {
            output.write((qid + "\t" + docidLookup[docno] + "\t").getBytes());
            for(int fid = 0; fid < fvalues[cnt].length; fid++) {
              output.write((fvalues[cnt][fid] + " ").getBytes());
            }
            output.write(("\n").getBytes());
          }

          cnt++;
          if(cnt >= hits) {
            ps.close();
            return fvalues;
          }
        }
      }
      ps.close();
      return fvalues;
    }

    int[] myLow = new int[query.length];
    int[] myHigh = new int[query.length];
    int[][] ds = new int[query.length][];

    for(int i = 0; i < query.length; i++) {
      myHigh[i] = dfs.get(query[i]) - 1;
      myLow[i] = 0;
      ds[i] = new int[myHigh[i] + 1];
    }

    decompress(postings.get(query[0]), ds[0], 0);
    int cnt = 0;
    int value = ds[0][0];
    pos[0] = postings.get(query[0]).decompressPositions(0);
    int index = 1;
    int found = 1;
    while(true) {
      if(index >= query.length) {
        index = 0;
      }

      if(myLow[index] > myHigh[index]) {
        break;
      }

      int[] r = binarySearch(postings.get(query[index]), ds[index], myLow[index], myHigh[index], value);
      if(r[1] == value) {
        found++;
        pos[index] = postings.get(query[index]).decompressPositions(r[2]);
        if(found == query.length) {
          int dl = docLengths.get(value);
          for(int fid = 0; fid < features.length; fid++) {
            fvalues[cnt][fid] = features[fid].computeScoreWithMiniIndexes(pos, query, dl, stats);
          }
          if(writeOutput) {
            output.write((qid + "\t" + docidLookup[value] + "\t").getBytes());
            for(int fid = 0; fid < fvalues[cnt].length; fid++) {
              output.write((fvalues[cnt][fid] + " ").getBytes());
            }
            output.write(("\n").getBytes());
          }
          cnt++;
          if(cnt >= hits) {
            break;
          }
          found = 1;
          int next = r[0];
          if(next > myHigh[index]) {
            break;
          }
          if(ds[index][next] == 0) {
            decompress(postings.get(query[index]), ds[index], postings.get(query[index]).getBlockNumber(next));
          }
          value = ds[index][next];
          pos[index] = postings.get(query[index]).decompressPositions(next);
        }
      } else {
        found = 1;
        value = r[1];
        pos[index] = postings.get(query[index]).decompressPositions(r[2]);
      }

      if((r[0] == myLow[index] && r[0] == myHigh[index]) || r[0] > myHigh[index]) {
        myLow[index] = myHigh[index] + 1;
      } else {
        myLow[index] = r[0];
      }
      index++;
    }

    for(int i = 0; i < query.length; i++) {
      postings.get(query[i]).close();
    }
    return fvalues;
  }

  public static void main(String[] args) throws Exception {
    OptionManager options = new OptionManager(RankAndFeaturesSmallAdaptive.class.getName());
    options.addOption(OptionManager.INDEX_ROOT_PATH, "path", "index root", true);
    options.addOption(OptionManager.POSTINGS_ROOT_PATH, "path", "Positional postings root", true);
    options.addOption(OptionManager.QUERY_PATH, "path", "XML query", true);
    options.addOption(OptionManager.JUDGMENT_PATH, "path", "Tab-Delimited documents", true);
    options.addOption(OptionManager.FEATURE_PATH, "path", "XML features", true);
    options.addOption(OptionManager.HITS, "integer", "number of hits (default: 10,000)", false);
    options.addOption(OptionManager.SPAM_PATH, "path", "spam percentile score", false);
    options.addOption(OptionManager.OUTPUT_PATH, "", "Print feature values", false);
    options.addDependency(OptionManager.OUTPUT_PATH, OptionManager.SPAM_PATH);

    try {
      options.parse(args);
    } catch(Exception exp) {
      return;
    }

    String indexPath = options.getOptionValue(OptionManager.INDEX_ROOT_PATH);
    String postingsPath = options.getOptionValue(OptionManager.POSTINGS_ROOT_PATH);
    String queryPath = options.getOptionValue(OptionManager.QUERY_PATH);
    String qrelPath = options.getOptionValue(OptionManager.JUDGMENT_PATH);
    String featurePath = options.getOptionValue(OptionManager.FEATURE_PATH);
    boolean writeOutput = options.foundOption(OptionManager.OUTPUT_PATH);
    int hits = 10000;
    if(options.foundOption(OptionManager.HITS)) {
      hits = Integer.parseInt(options.getOptionValue(OptionManager.HITS));
    }

    FileSystem fs = FileSystem.get(new Configuration());
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    env.initialize(true);

    RankAndFeaturesSmallAdaptive generator = new RankAndFeaturesSmallAdaptive(env, fs);

    //Parse queries and find integer codes for the query terms.
    HMapIV<String> parsedQueries = QueryUtility.loadQueries(queryPath);
    HMapIV<int[]> queries = QueryUtility.queryToIntegerCode(env, parsedQueries);
    HMapIF idfs = QueryUtility.loadIdf(env, parsedQueries);
    HMapIF cfs = QueryUtility.loadCf(env, parsedQueries);
    HMapIV<int[]> qrels = QrelUtility.parseQrelsFromTabDelimited(qrelPath);
    Map<String, Feature> featuresMap = FeatureUtility.parseFeatures(featurePath);
    Feature[] features = new Feature[featuresMap.size()];
    int index = 0;
    for(String key: featuresMap.keySet()) {
      features[index++] = featuresMap.get(key);
    }

    generator.prepareStats(idfs, cfs);
    generator.preparePostings(postingsPath);

    int[] newDocidsLookup = null;
    FSDataOutputStream output = null;
    if(writeOutput) {
      final SpamPercentileScore spamScores = new SpamPercentileScore();
      spamScores.initialize(options.getOptionValue(OptionManager.SPAM_PATH), fs);
      newDocidsLookup = DocumentUtility.reverseLookupSpamSortedDocids(DocumentUtility.spamSortDocids(spamScores));

      output = fs.create(new Path(options.getOptionValue(OptionManager.OUTPUT_PATH)));
    }

    System.gc();
    Thread.currentThread().sleep(20000);
    long cnt = 0;

    for (int qid: qrels.keySet()) {
      int[] qterms = queries.get(qid);
      if(qterms.length == 0) {
        continue;
      }

      long start = System.nanoTime();
      float[][] fvalues = generator.extract(qterms, hits, features,
                                            qid, writeOutput, newDocidsLookup, output);
      long end = System.nanoTime();
      System.out.println((end - start));

      if(++cnt % 50 == 0) {
        System.gc();
        Thread.currentThread().sleep(5000);
      }
    }

    if(writeOutput) {
      output.close();
    }
  }
}
