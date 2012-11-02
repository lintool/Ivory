package ivory.core.tokenize;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import junit.framework.JUnit4TestAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;

public class OpenNLPEnTokenizerTest {

  @Test
  public void testTokens1() throws IOException{
    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", false, false, null);

    String sentence = "This is a sentence, written in the U.S., which is \"un-tokenized\" (i.e., tokenization not performed).";
    String[] expectedTokens = new String[] {"this", "is", "a", "sentence", ",", "written", "in", "the", "u.s.", ",", "which", "is", "\"", "un", "-", "tokenized", "\"", "(", "i", ".e", ".", ",", "tokenization", "not", "performed", ")", "."};

    String[] tokens = tokenizer.processContent(sentence);
    int tokenCnt = 0;
    for (String token : tokens) {
      System.out.println("Token "+tokenCnt+":"+token);
      assertTrue("token "+tokenCnt+"="+token+"!="+expectedTokens[tokenCnt], token.equals(expectedTokens[tokenCnt]));
      tokenCnt++;
    }
  }
  
  @Test
  public void testTokens2() throws IOException{
    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", false, true, null);

    String sentence = "This is a sentence, written in the U.S., which is \"un-tokenized\" (i.e., tokenization not performed).";
    String[] expectedTokens = new String[] {"sentence", "written", "u.s.", "un", "tokenized", ".e", "tokenization", "performed"};

    String[] tokens = tokenizer.processContent(sentence);
    int tokenCnt = 0;
    for (String token : tokens) {
      System.out.println("Token "+tokenCnt+":"+token);
      assertTrue("token "+tokenCnt+"="+token+"!="+expectedTokens[tokenCnt], token.equals(expectedTokens[tokenCnt]));
      tokenCnt++;
    }
  }
  
  @Test
  public void testTokens3() throws IOException{
    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", true, true, null);

    String sentence = "This is a sentence, written in the U.S., which is \"un-tokenized\" (i.e., tokenization not performed).";
    String[] expectedTokens = new String[] {"sentenc", "written", "u.s.", "un", "token", ".e", "token", "perform"};

    String[] tokens = tokenizer.processContent(sentence);
    int tokenCnt = 0;
    for (String token : tokens) {
      System.out.println("Token "+tokenCnt+":"+token);
      assertTrue("token "+tokenCnt+"="+token+"!="+expectedTokens[tokenCnt], token.equals(expectedTokens[tokenCnt]));
      tokenCnt++;
    }
  }
  
  @Test
  public void testTokens4() throws IOException{
    VocabularyWritable v = new VocabularyWritable();
    v.addOrGet("sentenc");
    v.addOrGet("token");
    
    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", true, true, v);

    String sentence = "This is a sentence, written in the U.S., which is \"un-tokenized\" (i.e., tokenization not performed).";
    String[] expectedTokens = new String[] {"sentenc", "token", "token"};

    String[] tokens = tokenizer.processContent(sentence);
    int tokenCnt = 0;
    for (String token : tokens) {
      System.out.println("Token "+tokenCnt+":"+token);
      assertTrue("token "+tokenCnt+"="+token+"!="+expectedTokens[tokenCnt], token.equals(expectedTokens[tokenCnt]));
      tokenCnt++;
    }
  }
  
  @Test
  public void testPreNormalize() throws IOException{
    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", true, true, null);

    List<String> sentences = readInput("etc/tokenizer-normalize-test.txt");
    int cnt = 0;
    for (String sentence : sentences) {
      String[] tokens = tokenizer.processContent(sentence);
      System.out.println("Tokenizing line " + cnt);
      System.out.println("Original:" + sentence);
      System.out.println("Tokens:" + Arrays.asList(tokens));
      
      if (cnt < 4) {
        assertTrue("number of tokens < 2 => " + tokens.length, tokens.length == 2);
        assertEquals(tokens[0] + "!= world", tokens[0], "world");
        assertEquals(tokens[1] + "!= 's", tokens[1], "'s");
      }else {
        assertTrue("number of tokens < 1 => " + tokens.length, tokens.length == 1);
        assertEquals(tokens[0] + "!= world", tokens[0], "world");
      }
      cnt++;
    }
  }
  
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
  
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(OpenNLPEnTokenizerTest.class);
  }
}
