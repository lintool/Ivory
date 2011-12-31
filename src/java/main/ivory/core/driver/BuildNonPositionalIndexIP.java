package ivory.core.driver;

import ivory.core.Constants;
import ivory.core.index.BuildIPInvertedIndexDocSorted;
import ivory.core.index.BuildIntPostingsForwardIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class BuildNonPositionalIndexIP  extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildNonPositionalIndexIP.class);

  private static int printUsage() {
    System.out.println("usage: [index-path] [num-of-reducers]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      printUsage();
      return -1;
    }

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    String indexPath = args[0];

    Path p = new Path(indexPath);
    if (!fs.exists(p)) {
      LOG.warn("Index path doesn't exist...");
      return -1;
    }

    int numReducers = Integer.parseInt(args[1]);

    LOG.info("Tool name: " + BuildPositionalIndexIP.class.getCanonicalName());
    LOG.info(" - Index path: " + indexPath);

    conf.set(Constants.IndexPath, indexPath);
    conf.setInt(Constants.NumReduceTasks, numReducers);
    conf.set(Constants.PostingsListsType,
        ivory.core.data.index.PostingsListDocSortedNonPositional.class.getCanonicalName());

    new BuildIPInvertedIndexDocSorted(conf).run();
    new BuildIntPostingsForwardIndex(conf).run();

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new BuildNonPositionalIndexIP(), args);
  }
}
