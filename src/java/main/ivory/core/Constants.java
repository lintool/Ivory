package ivory.core;

public class Constants {
  /**
   * Maximum heap size passed to {@code mapred.child.java.opts} via JVM option {@code -Xmx}, in MB;
   */
  public static final String MaxHeap = "Ivory.MaxHeap";

  public static final String NumMapTasks = "Ivory.NumMapTasks";
  public static final String NumReduceTasks = "Ivory.NumReduceTasks";
  public static final String CollectionName = "Ivory.CollectionName";
  public static final String CollectionPath = "Ivory.CollectionPath";
  public static final String CollectionVocab = "Ivory.CollectionVocab";
  public static final String CollectionDocumentCount = "Ivory.CollectionDocumentCount";
  public static final String CollectionTermCount = "Ivory.CollectionTermCount";
  public static final String IndexPath = "Ivory.IndexPath";
  public static final String TargetIndexPath = "Ivory.TargetIndexPath";
  public static final String InputFormat = "Ivory.InputFormat";
  public static final String Tokenizer = "Ivory.Tokenizer";
  public static final String TokenizerData = "Ivory.TokenizerModel";
  public static final String DocnoMappingClass = "Ivory.DocnoMappingClass";
  public static final String DocnoMappingFile = "Ivory.DocnoMappingFile";
  public static final String DocnoOffset = "Ivory.DocnoOffset";
  public static final String PostingsListsType = "Ivory.PostingsListsType";
  public static final String MaxDf = "Ivory.MaxDf";
  public static final String MinDf = "Ivory.MinDf";
  public static final String TermIndexWindow = "Ivory.TermIndexWindow";
  public static final String MinSplitSize = "Ivory.MinSplitSize";
  public static final String TermDocVectorSegments = "Ivory.TermDocVectorSegments";
  public static final String Language = "Ivory.Lang";
  public static final String Stemming = "Ivory.IsStemming";
  public static final String StopwordList = "Ivory.Stopwordlist";
  public static final String StemmedStopwordList = "Ivory.StemmedStopwordlist";
  public static final String TargetStopwordList = "Ivory.TargetStopwordlist";
  public static final String TargetTokenizer = "Ivory.TargetTokenizer";
  public static final String TargetLanguage = "Ivory.TargetLang";

  /**
   * Memory threshold for the LP indexing algorithm: spill in the map phase after memory fills up to
   * this fraction. Setting is a value between > 0.0 and < 1.0;
   */
  public static final String IndexingMapMemoryThreshold = "Ivory.IndexingMapMemoryThreshold";

  /**
   * Memory threshold for the LP indexing algorithm: spill in the reduce phase after memory fills up to
   * this fraction. Setting is a value between > 0.0 and < 1.0;
   */
  public static final String IndexingReduceMemoryThreshold = "Ivory.IndexingReduceMemoryThreshold";

  /**
   * In the LP indexing algorithm, maximum number of documents to process before forcing a flush,
   * regardless of available memory.
   */
  public static final String MaxNDocsBeforeFlush = "Ivory.MaxNDocsBeforeFlush";
}
