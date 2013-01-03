package ivory.lsh.driver;

import ivory.core.RetrievalEnvironment;
import ivory.lsh.projection.ComputeSignaturesMinhash;
import ivory.lsh.projection.ComputeSignaturesRandom;
import ivory.lsh.projection.ComputeSignaturesSimhash;
import ivory.lsh.projection.WriteRandomVectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 *	A Hadoop task to compute signatures from document vectors.
 * 
 * @author ferhanture
 * 
 *
 */
public class RunComputeSignatures extends PwsimEnvironment implements Tool {

  public static final String[] RequiredParameters = {};
  private static final Logger sLogger = Logger.getLogger(RunComputeSignatures.class);
  private static Options options;

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "RunComputeSignatures", options );
    System.exit(-1);    
  }

  public int run(String[] args) throws Exception {
    CommandLine cmdline = parseArgs(args);
    if ( cmdline == null ) {
      printUsage();
      return -1;
    }

    numOfBits = Integer.parseInt(cmdline.getOptionValue(SIZE_OPTION));
    signatureType = cmdline.getOptionValue(TYPE_OPTION).toLowerCase();
    String dir = cmdline.getOptionValue(INDEX_OPTION);

    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(config);

    config.set("Ivory.IndexPath", dir);
    config.setInt("Ivory.NumOfBits", numOfBits);

    String type = (signatureType.charAt(0)+"").toUpperCase() + signatureType.substring(1, signatureType.length());		//capitalize first character
    RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
    String collName = env.readCollectionName();
    config.set("Ivory.CollectionName", collName);

    PwsimEnvironment.setClassTypes(config);
    int batchSize = -1;
    if (cmdline.hasOption(BATCH_OPTION)) {
      batchSize = Integer.parseInt(cmdline.getOptionValue(BATCH_OPTION));
      if (batchSize > 0) {
        int numDocs = env.readCollectionDocumentCount();
        numBatchFiles = numDocs / batchSize;
        if(numDocs % batchSize > 0) numBatchFiles++;
        System.out.println("numBatchFiles: "+numBatchFiles);
        config.setInt("NumBatch", numBatchFiles);
      }
    }

    if (type.equals("Random")) {
      WriteRandomVectors writeRandomTask = new WriteRandomVectors(config);
      writeRandomTask.run();
      ComputeSignaturesRandom computeSignaturesTask = new ComputeSignaturesRandom(config);
      computeSignaturesTask.run();
    } else if(type.equals("Simhash")) {
      if (numOfBits != 64) {
        sLogger.info("Simhash signatures need to be 64 bits! Quitting...");
        System.exit(0);
      }
      ComputeSignaturesSimhash computeSignaturesTask = new ComputeSignaturesSimhash(config);
      computeSignaturesTask.run();
    } else {	//minhash
      ComputeSignaturesMinhash computeSignaturesTask = new ComputeSignaturesMinhash(config);
      computeSignaturesTask.run();
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RunComputeSignatures(), args);
    System.exit(res);
  }

  private static final String INDEX_OPTION = "index";
  private static final String SIZE_OPTION = "num_bits";
  private static final String TYPE_OPTION = "type";
  private static final String BATCH_OPTION = "batch_size";
  private static final String LIBJARS_OPTION = "libjars";

  @SuppressWarnings("static-access")
  private static CommandLine parseArgs(String[] args) {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("path to collection index").withArgName("path").hasArg().isRequired().create(INDEX_OPTION));
    options.addOption(OptionBuilder.withDescription("number of bits per signature").withArgName("integer").hasArg().isRequired().create(SIZE_OPTION));
    options.addOption(OptionBuilder.withDescription("type of signature").withArgName("random|simhash|minhash").hasArg().isRequired().create(TYPE_OPTION));
    options.addOption(OptionBuilder.withDescription("batch size").withArgName("number of signatures in one output file").hasArg().create(BATCH_OPTION));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars").withArgName("jar packages").hasArg().create(LIBJARS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return null;
    }
    return cmdline;
  }
}
