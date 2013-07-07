package ivory.ffg.stats;

import edu.umd.cloud9.util.map.HMapIF;

/**
 * Global statistics used in computing features (e.g., idf and cf)
 *
 * @author Nima Asadi
 */

public class GlobalStats {
  private static HMapIF idf;
  private static HMapIF cf;
  private static int documentCount;
  private static long collectionLength;
  private static float avgDocumentLength;
  private static float defaultDf;
  private static float defaultCf;
  private static float defaultIdf;

  /**
   * Creates an instance of this class with the provided information
   *
   * @param idf Map of term ids to IDF values
   * @param cf Map of term ids to Collection Frequency (CF) values
   * @param documentCount Number of documents in the collection
   * @param collectionLength Length of the collection (i.e., number of terms)
   * @param avgDocumentLength Average length of documents (i.e., average number of term per document
   * @param defaultDf Default value for document frequency (used for phrase queries)
   * @param defaultCf Default value for collection frequency (used for phrase queries)
   */
  public GlobalStats(HMapIF idf, HMapIF cf,
                     int documentCount, long collectionLength, float avgDocumentLength,
                     float defaultDf, float defaultCf) {
    this.idf = idf;
    this.cf = cf;
    this.documentCount = documentCount;
    this.collectionLength = collectionLength;
    this.avgDocumentLength = avgDocumentLength;
    this.defaultDf = defaultDf;
    this.defaultCf = defaultCf;
    this.defaultIdf = (float) Math.log((documentCount - defaultDf + 0.5f) /
                                       (defaultDf + 0.5f));
  }

  /**
   * @param term Term id
   * @return idf value of a term
   */
  public float getIdf(int term) {
    return idf.get(term);
  }

  /**
   * @return idf values
   */
  public HMapIF getIdfs() {
    return idf;
  }

  /**
   * @param term Term id
   * @return cf value of a term
   */
  public float getCf(int term) {
    return cf.get(term);
  }

  /**
   * @return collection frequencies
   */
  public HMapIF getCfs() {
    return cf;
  }

  /**
   * @return Total number of documents in the collection
   */
  public int getDocumentCount() {
    return documentCount;
  }

  /**
   * @return Length of the collection (i.e., number of terms)
   */
  public long getCollectionLength() {
    return collectionLength;
  }

  /**
   * @return Average document length in the collection
   */
  public float getAvgDocumentLength() {
    return avgDocumentLength;
  }

  /**
   * @return Default document frequency
   */
  public float getDefaultDf() {
    return defaultDf;
  }

  /**
   * @return Default collection frequency
   */
  public float getDefaultCf() {
    return defaultCf;
  }

  /**
   * @return Default inverse-document frequency
   */
  public float getDefaultIdf() {
    return defaultIdf;
  }
}
