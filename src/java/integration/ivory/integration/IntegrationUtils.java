package ivory.integration;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;

public class IntegrationUtils {
  public static String getJar(File path, final String prefix) {
    String[] arr = path.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(prefix) && !name.contains("javadoc") && !name.contains("sources");
      }
    });

    assertTrue(arr.length == 1);
    return arr[0];
  }
}
