package ivory.regression.cikm2012;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class VerifyBloomIntersectionRelativeRecallR16K1 {
  @Test public void runRegression() throws Exception {
    RelativeRecallUtil.runRegression(16, 1, 84);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyBloomIntersectionRelativeRecallR16K1.class);
  }
}
