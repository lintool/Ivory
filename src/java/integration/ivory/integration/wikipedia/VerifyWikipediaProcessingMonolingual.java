package ivory.integration.wikipedia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import ivory.integration.IntegrationUtils;
import ivory.integration.VerifyWt10gPositionalIndexIP;
import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import edu.umd.cloud9.io.map.HMapSFW;

public class VerifyWikipediaProcessingMonolingual {

  private ImmutableMap<String, Float> testTermDocVector1 =
    ImmutableMap.of("total", 0.036282938f,
                    "posit", 0.047018476f,
                    "valid", 0.07093949f,
                    "formula", 0.06923077f);

  @Test
  public void verifyResults() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader = new SequenceFile.Reader(fs,
        new Path("en-wiki/wt-term-doc-vectors/part-00000"), fs.getConf());

    IntWritable key = new IntWritable();
    HMapSFW value = new HMapSFW();
    reader.next(key, value);
    for (Map.Entry<String, Float> entry : testTermDocVector1.entrySet()) {
      assertTrue(value.containsKey(entry.getKey()));
      assertEquals(entry.getValue(), value.get(entry.getKey()), 10e-6);
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyWt10gPositionalIndexIP.class);
  }
}
