package ivory.regression.cikm2012;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class VerifyBloomIntersectionRelativeRecallR16K3 {
  @Test public void runRegression() throws Exception {
    RelativeRecallUtil.runRegression(16, 3, 97);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyBloomIntersectionRelativeRecallR16K3.class);
  }
}
