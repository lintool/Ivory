package ivory.bloomir.preprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import ivory.bloomir.data.CompressedPostingsIO;
import ivory.bloomir.util.OptionManager;
import ivory.core.RetrievalEnvironment;

public class GenerateCompressedPostings {
  public static void main(String[] args) throws Exception {
    OptionManager options = new OptionManager(GenerateCompressedPostings.class.getName());
    options.addOption(OptionManager.INDEX_ROOT_PATH, "path", "index root", true);
    options.addOption(OptionManager.OUTPUT_PATH, "path", "output root", true);
    options.addOption(OptionManager.SPAM_PATH, "path", "spam percentile score", true);

    try {
      options.parse(args);
    } catch(Exception exp) {
      return;
    }

    final String input = options.getOptionValue(OptionManager.INDEX_ROOT_PATH);
    final String output = options.getOptionValue(OptionManager.OUTPUT_PATH);
    final String spam = options.getOptionValue(OptionManager.SPAM_PATH);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    RetrievalEnvironment env = new RetrievalEnvironment(input, fs);
    env.initialize(false);

    CompressedPostingsIO.writePostings(output, fs, env, spam);
  }
}
