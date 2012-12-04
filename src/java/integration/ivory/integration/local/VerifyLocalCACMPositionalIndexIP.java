package ivory.integration.local;

import ivory.app.BuildIndex;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class VerifyLocalCACMPositionalIndexIP extends IntegrationTestBaseCACM {
  private static final Random RANDOM = new Random();

  @Test
  public void runBuildIndex() throws Exception {
    String index = this.getClass().getCanonicalName() + "-index-" + RANDOM.nextInt(10000);
    String[] args = new String[] { 
        "-" + BuildIndex.POSITIONAL_INDEX_IP,
        "-" + BuildIndex.INDEX_PATH, index,
        "-" + BuildIndex.INDEX_PARTITIONS, "1" };

    runBuildIndex(index, args);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyLocalCACMPositionalIndexIP.class);
  }
}
