package ivory.regression.cikm2012;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class VerifyBloomIntersectionRelativeRecallR8K3 {
  @Test public void runRegression() throws Exception {
    RelativeRecallUtil.runRegression(8, 3, 89);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyBloomIntersectionRelativeRecallR8K3.class);
  }
}
