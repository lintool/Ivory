package ivory.core.tokenize;

import static org.junit.Assert.assertTrue;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import junit.framework.JUnit4TestAdapter;
import org.junit.Assert;
import org.junit.Test;
import edu.umd.hooka.VocabularyWritable;

public class TokenizationTest {
  String dir = ""; // /Users/ferhanture/Documents/workspace/ivory-github/Ivory/";

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
      Assert.fail();
    }
    return null;
  }

  public void testTokens(String lang, String tokenizerModelFile, boolean isStem, String stopwordsFile, VocabularyWritable vocab, String inputFile, String expectedFile) throws IOException{
    Tokenizer tokenizer = TokenizerFactory.createTokenizer(lang, tokenizerModelFile, isStem, stopwordsFile, null, vocab);

    List<String> sentences = readInput(inputFile);
    List<String> expectedSentences = readInput(expectedFile);

    for (int i = 0; i < sentences.size(); i++) {
      String sentence = sentences.get(i);
      String[] expectedTokens = expectedSentences.get(i).split("\\s+");
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

  public long testTokenizationTime(String lang, String tokenizerModelFile, boolean isStem, String stopwordsFile, VocabularyWritable vocab, String sentence) throws IOException{
    Tokenizer tokenizer = TokenizerFactory.createTokenizer(lang, tokenizerModelFile, isStem, stopwordsFile, null, vocab);
    int i = 0;
    long time = System.currentTimeMillis();
    while (i++ < 1000) {
      tokenizer.processContent(sentence);
    }
    return (System.currentTimeMillis() - time);
  }

  @Test
  public void testTokenization() {
    String[] languages = {"ar", "tr", "cs", "es", "de", "en"}; //  "zh"
    try {
      for (String language : languages) {
        String rawFile = dir + "data/tokenizer/test/" + language + "-test.raw";
        String tokenizedFile = dir + "data/tokenizer/test/" + language + "-test.tok";
        String tokenizedStemmedFile = dir + "data/tokenizer/test/" + language + "-test.tok.stemmed";
        String tokenizedStopFile = dir + "data/tokenizer/test/" + language + "-test.tok.stop";
        String tokenizedStemmedStopFile = dir + "data/tokenizer/test/" + language + "-test.tok.stemmed.stop";
        String tokenizer = dir + "data/tokenizer/" + language + "-token.bin";
        String stopwords = dir + "data/tokenizer/" + language + ".stop";
        testTokens(language, tokenizer, false, null, null, rawFile, tokenizedFile);
        testTokens(language, tokenizer, true, null, null, rawFile, tokenizedStemmedFile);
        testTokens(language, tokenizer, false, stopwords, null, rawFile, tokenizedStopFile);
        testTokens(language, tokenizer, true, stopwords, null, rawFile, tokenizedStemmedStopFile);
      }
    } catch (IOException e) {
      Assert.fail("Error in tokenizer test: " + e.getMessage());
    }
  }

  @Test
  public void testTokenizationTime() {
    String[] languages = {"ar", "tr", "cs", "es", "de", "en"}; //  "zh"
    try {
      for (String language : languages) {
        String tokenizer = dir + "data/tokenizer/" + language + "-token.bin";
        String stopwords = dir + "data/tokenizer/" + language + ".stop";
        long time = testTokenizationTime(language, tokenizer, true, stopwords, null, "Although they are at temperatures of roughly 3000–4500 K (2727–4227 °C),");
        System.out.println("Tokenization for " + language + " : " + time);
      }
    } catch (IOException e) {
      Assert.fail("Error in tokenizer test: " + e.getMessage());
    }

  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(TokenizationTest.class);
  }
}
