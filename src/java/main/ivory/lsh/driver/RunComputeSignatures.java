package ivory.lsh.driver;

import ivory.core.RetrievalEnvironment;
import ivory.lsh.projection.ComputeSignaturesMinhash;
import ivory.lsh.projection.ComputeSignaturesRandom;
import ivory.lsh.projection.ComputeSignaturesSimhash;
import ivory.lsh.projection.WriteRandomVectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * A Hadoop task to compute signatures from document vectors.
 * 
 * @author ferhanture
 * 
 * 
 */
public class RunComputeSignatures extends PwsimEnvironment implements Tool {

  public static final String[] RequiredParameters = {};
  private static final Logger sLogger = Logger.getLogger(RunComputeSignatures.class);

  private static int printUsage() {
    System.out
        .println("usage: [index-path] [num-of-bits] [type-of-signature] ([batch-size]) ([dot-prod-thresholds])\nIgnore last two arguments if you don't know what they are. If you want non-batch mode but want to include 'dot product thresholds' file, enter X for [batch-size]");
    return -1;
  }

  public int run(String[] args) throws Exception {
    if (args.length != 3 && args.length != 4 && args.length != 5) {
      printUsage();
      return -1;
    }
    boolean batchSizeGiven = (args.length >= 4);
    numOfBits = Integer.parseInt(args[1]);
    signatureType = args[2].toLowerCase();
    String dir = args[0];

    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(config);

    config.set("Ivory.IndexPath", dir);
    config.setInt("Ivory.NumOfBits", numOfBits);

    String type = (signatureType.charAt(0) + "").toUpperCase()
        + signatureType.substring(1, signatureType.length()); // capitalize first character
    RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
    String collName = env.readCollectionName();
    config.set("Ivory.CollectionName", collName);

    PwsimEnvironment.setClassTypes(config);
    int batchSize = -1;
    try {
      if (batchSizeGiven) {
        batchSize = Integer.parseInt(args[3]);
        if (batchSize > 0) {
          int numDocs = env.readCollectionDocumentCount();
          numBatchFiles = numDocs / batchSize;
          if (numDocs % batchSize > 0)
            numBatchFiles++;
          System.out.println("numBatchFiles: " + numBatchFiles);
          config.setInt("NumBatch", numBatchFiles);
        }
      }
    } catch (NumberFormatException e) {
      sLogger.info("Batch size not an integer! Running in regular (non-batch) mode...");
      batchSizeGiven = false;
    }

    if (type.equals("Random")) {
      WriteRandomVectors writeRandomTask = new WriteRandomVectors(config);
      writeRandomTask.run();
      if (args.length == 5) {
        config.set("Ivory.DotProdThreshFile", args[4]);
      }
      ComputeSignaturesRandom computeSignaturesTask = new ComputeSignaturesRandom(config);
      computeSignaturesTask.run();
    } else if (type.equals("Simhash")) {
      if (numOfBits != 64) {
        sLogger.info("Simhash signatures need to be 64 bits! Quitting...");
        System.exit(0);
      }
      ComputeSignaturesSimhash computeSignaturesTask = new ComputeSignaturesSimhash(config);
      computeSignaturesTask.run();
    } else { // minhash
      ComputeSignaturesMinhash computeSignaturesTask = new ComputeSignaturesMinhash(config);
      computeSignaturesTask.run();
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RunComputeSignatures(), args);
    System.exit(res);
  }

}
