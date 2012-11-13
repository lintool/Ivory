package ivory.core.tokenize;

import static org.junit.Assert.assertTrue;
import ivory.core.Constants;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import junit.framework.JUnit4TestAdapter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class LuceneArabicAnalyzerTest {
  String arabicRawLinesFile = "etc/ar-test.raw";
  String arabicTokenizedLinesFile = "etc/ar-test.tok";

  private List<String> readInput(String file) {
    List<String> lines = new ArrayList<String>();
    try {
      FileInputStream fis = new FileInputStream(file);
      InputStreamReader isr = new InputStreamReader(fis, "UTF8");
      BufferedReader in = new BufferedReader(isr);
      String line;

      while ((line = in.readLine()) != null) {
        lines.add(line);
      }
      in.close();
      return lines;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private List<String[]> readTokenizedInput(String file) {
    List<String[]> lines = new ArrayList<String[]>();
    try {
      FileInputStream fis = new FileInputStream(file);
      InputStreamReader isr = new InputStreamReader(fis, "UTF8");
      BufferedReader in = new BufferedReader(isr);
      String line;

      while ((line = in.readLine()) != null) {
        lines.add(line.split("\\s+"));
      }
      in.close();
      return lines;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Test
  public void testTokens1() throws IOException{
    ivory.core.tokenize.Tokenizer tokenizer = new LuceneArabicAnalyzer();
    Configuration conf = new Configuration();
    conf.set(Constants.StopwordList, "data/tokenizer/ar.stop");
    tokenizer.configure(conf, FileSystem.getLocal(conf));

    List<String> sentences = readInput(arabicRawLinesFile);
    List<String[]> expectedTokenArrays = readTokenizedInput(arabicTokenizedLinesFile);

    for (int i = 0; i < 4; i++) {
      String sentence = sentences.get(i);
      String[] expectedTokens = expectedTokenArrays.get(i);
      System.out.println("Testing sentence:"+sentence);

      String[] tokens = tokenizer.processContent(sentence);
      int tokenCnt = 0;
      for (String token : tokens) {
	System.out.println("Token "+tokenCnt+":"+token);
        assertTrue("token "+tokenCnt+":"+token+",expected="+expectedTokens[tokenCnt], token.equals(expectedTokens[tokenCnt]));
        tokenCnt++;
      }
    }
  }
  
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(LuceneArabicAnalyzerTest.class);
  }
}
