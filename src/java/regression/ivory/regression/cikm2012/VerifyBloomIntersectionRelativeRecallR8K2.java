package ivory.regression.cikm2012;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class VerifyBloomIntersectionRelativeRecallR8K2 {
  @Test public void runRegression() throws Exception {
    RelativeRecallUtil.runRegression(8, 2, 85);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyBloomIntersectionRelativeRecallR8K2.class);
  }
}
