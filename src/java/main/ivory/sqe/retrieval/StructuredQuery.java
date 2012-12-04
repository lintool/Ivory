package ivory.sqe.retrieval;

import org.apache.hadoop.thirdparty.guava.common.base.Preconditions;

import com.google.gson.JsonObject;

public class StructuredQuery {
  private JsonObject query;
  private int queryLength;

  public StructuredQuery(JsonObject query, int queryLength) {
    this.query = Preconditions.checkNotNull(query);
    this.queryLength = queryLength;
  }

  public int getQueryLength() {
    return queryLength;
  }

  public JsonObject getQuery() {
    return query;
  }
}
