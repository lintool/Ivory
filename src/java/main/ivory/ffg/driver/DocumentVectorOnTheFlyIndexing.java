package ivory.ffg.driver;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.map.HMapIF;
import edu.umd.cloud9.util.map.HMapIV;

import ivory.bloomir.util.OptionManager;
import ivory.bloomir.util.QueryUtility;
import ivory.core.RetrievalEnvironment;
import ivory.ffg.data.DocumentVector;
import ivory.ffg.data.DocumentVectorUtility;
import ivory.ffg.feature.Feature;
import ivory.ffg.stats.GlobalStats;
import ivory.ffg.util.FeatureUtility;
import ivory.ffg.util.QrelUtility;

/**
 * @author Nima Asadi
 */
public class DocumentVectorOnTheFlyIndexing {
  private static final Logger LOGGER = Logger.getLogger(DocumentVectorOnTheFlyIndexing.class);
  private HMapIV<DocumentVector> documents;
  private GlobalStats stats;

  private RetrievalEnvironment env;
  private FileSystem fs;

  public DocumentVectorOnTheFlyIndexing(RetrievalEnvironment env, FileSystem fs) {
    this.env = env;
    this.fs = fs;
  }

  public void prepareStats(HMapIF idfs, HMapIF cfs) throws Exception {
    stats = new GlobalStats(idfs, cfs,
                            (int) env.getDocumentCount(), env.getCollectionSize(),
                            (float) env.getCollectionSize() / (float) env.getDocumentCount(),
                            (float) env.getDefaultDf(), (float) env.getDefaultCf());
  }

  private void prepareDocuments(String documentVectorClass, String documentsPath) throws Exception {
    documents = new HMapIV<DocumentVector>();

    FSDataInputStream input = fs.open(new Path(documentsPath));
    int docid = input.readInt();
    while(docid != -1) {
      documents.put(docid, DocumentVectorUtility.readInstance(documentVectorClass, input));
      docid = input.readInt();
    }
    input.close();
  }

  public float[][] extract(int[] qterms, int[] docids, Feature[] features) throws Exception {
    float[][] fvalues = new float[docids.length][features.length];

    for(int d = 0; d < docids.length; d++) {
      DocumentVector compDoc = documents.get(docids[d]);
      int[][] positions = compDoc.decompressPositions(qterms);
      computeFeatures(qterms, positions, compDoc.getDocumentLength(), features, fvalues[d]);
    }
    return fvalues;
  }

  private void computeFeatures(int[] qterms, int[][] positions, int dl, Feature[] features, float[] fvalues) {
    for(int i = 0; i < features.length; i++) {
      fvalues[i] = features[i].computeScoreWithMiniIndexes(positions, qterms, dl, stats);
    }
  }

  public static void main(String[] args) throws Exception {
    OptionManager options = new OptionManager(DocumentVectorOnTheFlyIndexing.class.getName());
    options.addOption(OptionManager.INDEX_ROOT_PATH, "path", "index root", true);
    options.addOption(OptionManager.DOCUMENT_VECTOR_CLASS, "class_name", "DocumentVector class", true);
    options.addOption(OptionManager.DOCUMENT_PATH, "path", "documents", true);
    options.addOption(OptionManager.QUERY_PATH, "path", "XML query", true);
    options.addOption(OptionManager.JUDGMENT_PATH, "path", "Tab-Delimited judgments", true);
    options.addOption(OptionManager.FEATURE_PATH, "path", "XML features", true);

    try {
      options.parse(args);
    } catch(Exception exp) {
      return;
    }

    String indexPath = options.getOptionValue(OptionManager.INDEX_ROOT_PATH);
    String documentVectorClass = options.getOptionValue(OptionManager.DOCUMENT_VECTOR_CLASS);
    String documentsPath = options.getOptionValue(OptionManager.DOCUMENT_PATH);
    String queryPath = options.getOptionValue(OptionManager.QUERY_PATH);
    String qrelPath = options.getOptionValue(OptionManager.JUDGMENT_PATH);
    String featurePath = options.getOptionValue(OptionManager.FEATURE_PATH);

    FileSystem fs = FileSystem.get(new Configuration());
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    env.initialize(true);

    DocumentVectorOnTheFlyIndexing generator = new DocumentVectorOnTheFlyIndexing(env, fs);

    //Parse queries, judgemnts and features
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

    //Prepare stats
    generator.prepareStats(idfs, cfs);
    generator.prepareDocuments(documentVectorClass, documentsPath);

    System.gc();
    Thread.currentThread().sleep(20000);
    long cnt = 0;

    //Evaluate queries and/or write the results to an output file
    for (int qid: qrels.keySet()) {
      int[] qterms = queries.get(qid);
      if(qterms.length == 0) {
        continue;
      }

      long start = System.nanoTime();
      float[][] fvalues = generator.extract(qterms, qrels.get(qid), features);
      long end = System.nanoTime();
      System.out.println((end - start));

      if(++cnt % 50 == 0) {
        System.gc();
        Thread.currentThread().sleep(5000);
      }
    }
  }
}
