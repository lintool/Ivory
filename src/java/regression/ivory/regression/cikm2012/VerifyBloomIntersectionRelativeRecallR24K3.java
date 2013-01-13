package ivory.regression.cikm2012;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class VerifyBloomIntersectionRelativeRecallR24K3 {
  @Test public void runRegression() throws Exception {
    RelativeRecallUtil.runRegression(24, 3, 99);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyBloomIntersectionRelativeRecallR24K3.class);
  }
}
