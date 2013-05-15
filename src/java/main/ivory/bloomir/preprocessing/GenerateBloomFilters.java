package ivory.bloomir.preprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import ivory.bloomir.data.SignatureIO;
import ivory.bloomir.util.OptionManager;
import ivory.core.RetrievalEnvironment;

/**
 * Generates the Bloom filters for the original postings lists, given
 * a set of configuration parameters {@link ivory.bloomir.data.BloomConfig} for an
 * experiment.
 *
 * @author Nima Asadi
 */
public class GenerateBloomFilters {
  public static void main(String[] args) throws Exception {
    OptionManager options = new OptionManager(GenerateBloomFilters.class.getName());
    options.addOption(OptionManager.INDEX_ROOT_PATH, "path", "index root", true);
    options.addOption(OptionManager.OUTPUT_PATH, "path", "output root", true);
    options.addOption(OptionManager.SPAM_PATH, "path", "spam percentile score", true);
    options.addOption(OptionManager.BITS_PER_ELEMENT, "integer", "number of bits per element", true);
    options.addOption(OptionManager.NUMBER_OF_HASH, "integer", "number of hash functions", true);

    try {
      options.parse(args);
    } catch(Exception exp) {
      return;
    }

    final String input = options.getOptionValue(OptionManager.INDEX_ROOT_PATH);
    final String output = options.getOptionValue(OptionManager.OUTPUT_PATH);
    final String spamPath = options.getOptionValue(OptionManager.SPAM_PATH);
    final int bitsPerElement = Integer.parseInt(options.getOptionValue(OptionManager.BITS_PER_ELEMENT));
    final int nbHash = Integer.parseInt(options.getOptionValue(OptionManager.NUMBER_OF_HASH));

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    RetrievalEnvironment env = new RetrievalEnvironment(input, fs);
    env.initialize(false);

    SignatureIO.writeSignatures(output, fs, env, spamPath, bitsPerElement, nbHash);
  }
}
