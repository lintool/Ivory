package ivory.integration;

import static org.junit.Assert.assertTrue;
import ivory.core.driver.BuildIPIndex;
import ivory.core.driver.PreprocessTREC;

import java.io.File;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class VerifyTrecIndex {
  private Path collectionPath = new Path("/shared/collections/trec/trec4-5_noCRFR.xml");
  private String index = this.getClass().getCanonicalName() + "-index";

  @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    FileSystem.get(conf).delete(new Path(index), true);

    File lib = new File("lib");
    String cloud9Jar = IntegrationUtils.getJar(lib, "cloud9");
    String guavaJar = IntegrationUtils.getJar(lib, "guava");

    String libjars = String.format("-libjars=lib/%s,lib/%s", cloud9Jar, guavaJar);

    PreprocessTREC.main(new String[] { libjars, collectionPath.toString(), index });
    BuildIPIndex.main(new String[] { libjars, index, "10" });
  }

  @Test
  public void verifyResults() throws Exception {
    
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyTrecIndex.class);
  }
}
