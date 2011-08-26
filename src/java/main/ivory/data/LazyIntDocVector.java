package ivory.data;

import org.apache.log4j.Logger;

@Deprecated
public class LazyIntDocVector extends ivory.core.data.document.LazyIntDocVector {
  private static final Logger LOG = Logger.getLogger(LazyIntDocVector.class);
  static {
    LOG.warn("You are using a old version of the index! " +
        LazyIntDocVector.class.getCanonicalName() + " is depcrecated.");
  }
}
