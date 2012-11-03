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
import org.junit.Test;
import edu.umd.hooka.VocabularyWritable;

public class OpenNLPEnTokenizerTest {

  @Test
  public void testTokensNoStemNoStopNoVocab() throws IOException{
//    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", false, false, null);
    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", false, null, null, null);

    String sentence = "This is a sentence, written in the U.S., which is \"un-tokenized\" (i.e., tokenization not performed).";
    String[] expectedTokens = new String[] {"this", "is", "a", "sentence", ",", "written", "in", "the", "u.s.", ",", "which", "is", "\"", "un", "-", "tokenized", "\"", "(", "i", ".e", ".", ",", "tokenization", "not", "performed", ")", "."};

    String[] tokens = tokenizer.processContent(sentence);
    verifyTokens(tokens, expectedTokens);
  }
  
  @Test
  public void testTokensNoStemNoVocab() throws IOException{
//    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", false, true, null);
    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", false, "data/tokenizer/en.stop", "data/tokenizer/en.stop.stemmed", null);

    String sentence = "This is a sentence, written in the U.S., which is \"un-tokenized\" (i.e., tokenization not performed).";
    String[] expectedTokens = new String[] {"sentence", "written", "u.s.", "un", "tokenized", ".e", "tokenization", "performed"};

    String[] tokens = tokenizer.processContent(sentence);
    verifyTokens(tokens, expectedTokens);
  }
  
  @Test
  public void testTokensNoVocab() throws IOException{
//    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", true, true, null);
    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", true, "data/tokenizer/en.stop", "data/tokenizer/en.stop.stemmed", null);

    String sentence = "This is a sentence, written in the U.S., which is \"un-tokenized\" (i.e., tokenization not performed).";
    String[] expectedTokens = new String[] {"sentenc", "written", "u.s.", "un", "token", ".e", "token", "perform"};

    String[] tokens = tokenizer.processContent(sentence);
    verifyTokens(tokens, expectedTokens);
  }
  
  @Test
  public void testTokensAllInclusive() throws IOException{
    VocabularyWritable v = new VocabularyWritable();
    v.addOrGet("sentenc");
    v.addOrGet("token");
    
//    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", true, true, v);
    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", true, "data/tokenizer/en.stop", "data/tokenizer/en.stop.stemmed", v);

    String sentence = "This is a sentence, written in the U.S., which is \"un-tokenized\" (i.e., tokenization not performed).";
    String[] expectedTokens = new String[] {"sentenc", "token", "token"};

    String[] tokens = tokenizer.processContent(sentence);
    verifyTokens(tokens, expectedTokens);
  }

  @Test
  public void testPreNormalize() throws IOException{
    Tokenizer tokenizer = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", true, "data/tokenizer/en.stop", "data/tokenizer/en.stop.stemmed", null);

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
  
  @Test
  public void testStopword() throws IOException{
    // ALl of the following should properly handle arguments and behave as if not stopword list was provided
    Tokenizer tokenizer1 = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", null);
    Tokenizer tokenizer2 = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", true, null, null, null);
    Tokenizer tokenizer3 = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", true, "xxxx", "yyyy", null);
    Tokenizer tokenizer4 = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", false, null, null, null);
    Tokenizer tokenizer5 = TokenizerFactory.createTokenizer("en", "data/tokenizer/en-token.bin", false, "xxxx", "yyyy", null);
    
    String sentence = "This is a sentence, written in the U.S., which is \"un-tokenized\" (i.e., tokenization not performed).";
    String[] expectedTokens1 = new String[] {"this", "is", "a", "sentenc", ",", "written", "in", "the", "u.s.", ",", "which", "is", "\"", "un", "-", "token", "\"", "(", "i", ".e", ".", ",", "token", "not", "perform", ")", "."};
    String[] expectedTokens2 = new String[] {"this", "is", "a", "sentence", ",", "written", "in", "the", "u.s.", ",", "which", "is", "\"", "un", "-", "tokenized", "\"", "(", "i", ".e", ".", ",", "tokenization", "not", "performed", ")", "."};

    String[] tokens1 = tokenizer1.processContent(sentence);
    String[] tokens2 = tokenizer2.processContent(sentence);
    String[] tokens3 = tokenizer3.processContent(sentence);
    String[] tokens4 = tokenizer4.processContent(sentence);
    String[] tokens5 = tokenizer5.processContent(sentence);

    verifyTokens(tokens1, expectedTokens1);
    verifyTokens(tokens2, expectedTokens1);
    verifyTokens(tokens3, expectedTokens1);
    verifyTokens(tokens4, expectedTokens2);
    verifyTokens(tokens5, expectedTokens2);
  }
  
  private void verifyTokens(String[] tokens, String[] expectedTokens) {
    int tokenCnt = 0;
    for (String token : tokens) {
      System.out.println("Token "+tokenCnt+":"+token);
      assertTrue("token "+tokenCnt+"="+token+"!="+expectedTokens[tokenCnt], token.equals(expectedTokens[tokenCnt]));
      tokenCnt++;
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
