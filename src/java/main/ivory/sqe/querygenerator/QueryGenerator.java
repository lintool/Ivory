package ivory.sqe.querygenerator;

import ivory.sqe.retrieval.StructuredQuery;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public interface QueryGenerator {
  public void init(FileSystem fs, Configuration conf) throws IOException;

  public StructuredQuery parseQuery(String query);
}
