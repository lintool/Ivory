package ivory.sqe.querygenerator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.gson.JsonObject;

public interface QueryGenerator {
	  public JsonObject parseQuery(String query);
	  public void init(FileSystem fs, Configuration conf) throws IOException;
    public void init(Configuration conf) throws IOException;
    public int getQueryLength();
}
