package ivory.data;

import org.apache.log4j.Logger;

@Deprecated
public class PostingsListDocSortedPositional
    extends ivory.core.data.index.PostingsListDocSortedPositional {
  private static final Logger LOG = Logger.getLogger(PostingsListDocSortedPositional.class);
  static {
    LOG.warn("You are using a old version of the index! " +
        PostingsListDocSortedPositional.class.getCanonicalName() + " is depcrecated.");
  }
}
