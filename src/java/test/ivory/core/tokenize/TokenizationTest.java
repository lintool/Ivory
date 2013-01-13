package ivory.core.tokenize;

import static org.junit.Assert.assertTrue;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Test;
import edu.umd.hooka.VocabularyWritable;

public class TokenizationTest {
  private String dir = "./";
  private String[] languages = {"ar", "tr", "cs", "es", "de", "fr", "en"};//, "zh"};

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

  public void testTokenization(String lang, String tokenizerModelFile, boolean isStem, String stopwordsFile, VocabularyWritable vocab, String inputFile, String expectedFile) throws IOException{
    Tokenizer tokenizer = TokenizerFactory.createTokenizer(lang, tokenizerModelFile, isStem, stopwordsFile, null, vocab);
    // two classes are not aware of the stemming/stopword options (they use default option instead): StanfordChineseTokenizer, GalagoTokenizer
    assertTrue(tokenizer.isStemming() == isStem 
        || (tokenizer.getClass() == StanfordChineseTokenizer.class) 
        || (tokenizer.getClass() == GalagoTokenizer.class));
    assertTrue(tokenizer.isStopwordRemoval() == (stopwordsFile != null) 
        || (tokenizer.getClass() == StanfordChineseTokenizer.class) 
        || (tokenizer.getClass()==GalagoTokenizer.class));

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
  public void testAllTokenization() {
    try {
      for (String language : languages) {
        String rawFile = dir + "data/tokenizer/test/" + language + "-test.raw";
        String tokenizedFile = dir + "data/tokenizer/test/" + language + "-test.tok";
        String tokenizedStemmedFile = dir + "data/tokenizer/test/" + language + "-test.tok.stemmed";
        String tokenizedStopFile = dir + "data/tokenizer/test/" + language + "-test.tok.stop";
        String tokenizedStemmedStopFile = dir + "data/tokenizer/test/" + language + "-test.tok.stemmed.stop";
        String tokenizer = dir + "data/tokenizer/" + language + "-token.bin";
        String stopwords = dir + "data/tokenizer/" + language + ".stop";
        testTokenization(language, tokenizer, false, null, null, rawFile, tokenizedFile);
        testTokenization(language, tokenizer, true, null, null, rawFile, tokenizedStemmedFile);
        testTokenization(language, tokenizer, false, stopwords, null, rawFile, tokenizedStopFile);
        testTokenization(language, tokenizer, true, stopwords, null, rawFile, tokenizedStemmedStopFile);
        
        // for Lucene tokenizers, everything should work without model file
        if (language.equals("cs") || language.equals("ar") || language.equals("tr") || language.equals("es")) {
          testTokenization(language, null, false, null, null, rawFile, tokenizedFile);
          testTokenization(language, null, true, null, null, rawFile, tokenizedStemmedFile);
          testTokenization(language, null, false, stopwords, null, rawFile, tokenizedStopFile);
          testTokenization(language, null, true, stopwords, null, rawFile, tokenizedStemmedStopFile);          
        }
        
        if (language.equals("en")) {
          // stemming = true or false should have same output (since stemming is default)
          testTokenization(language, null, false, null, null, rawFile, tokenizedFile + "-galago");
          testTokenization(language, null, true, null, null, rawFile, tokenizedFile + "-galago");
        }
      }
    } catch (IOException e) {
      Assert.fail("Error in tokenizer test: " + e.getMessage());
    }
  }

  @Test
  public void testTokenizationTime() {
    String[] languages = {"ar", "tr", "cs", "es", "de", "en", "zh"};
    try {
      for (String language : languages) {
        String tokenizer = dir + "data/tokenizer/" + language + "-token.bin";
        String stopwords = dir + "data/tokenizer/" + language + ".stop";
        long time = testTokenizationTime(language, tokenizer, true, stopwords, null, "Although they are at temperatures of roughly 3000–4500 K (2727–4227 °C),");
        System.out.println("Tokenization for " + language + " : " + (time/1000f) + "ms/sentence");
      }
    } catch (IOException e) {
      Assert.fail("Error in tokenizer test: " + e.getMessage());
    }
  }

  public void testOOV(String language, VocabularyWritable vocab, boolean isStemming, boolean isStopwordRemoval, float[] expectedOOVRates) {
    Tokenizer tokenizer;
    Configuration conf = new Configuration();
    try {
      if (isStopwordRemoval) {
        tokenizer = TokenizerFactory.createTokenizer(FileSystem.getLocal(conf), conf, language, dir + "data/tokenizer/" + language + "-token.bin", isStemming, dir + "data/tokenizer/" + language + ".stop", dir + "data/tokenizer/" + language + ".stop.stemmed", null);
      }else {
        tokenizer = TokenizerFactory.createTokenizer(FileSystem.getLocal(conf), conf, language, dir + "data/tokenizer/" + language + "-token.bin", isStemming, null, null, null);      
      }
    } catch (IOException e) {
      Assert.fail("Unable to create tokenizer.");
      return;
    }
    List<String> sentences = readInput(dir + "data/tokenizer/test/" + language + "-test.raw");
    for (int i = 0; i < sentences.size(); i++) {
      String sentence = sentences.get(i);
      float oovRate = tokenizer.getOOVRate(sentence, vocab);
      assertTrue( "Sentence " + i + ":" + oovRate + "!=" + expectedOOVRates[i] , oovRate == expectedOOVRates[i] );
    }
  }

  @Test
  public void testChineseOOVs() {
    VocabularyWritable vocab = new VocabularyWritable();
    List<String> sentences = readInput(dir + "data/tokenizer/test/zh-test.tok.stemmed.stop");
    for (String token : sentences.get(3).split(" ")) {
      vocab.addOrGet(token);
    }
    vocab.addOrGet("1457");
    vocab.addOrGet("19");

    float[] zhExpectedOOVRates = {0.6666667f, 0.8666667f, 0.72727275f, 0f};     // all same since no stemming or stopword removal
 
    testOOV("zh", vocab, true, true, zhExpectedOOVRates);
    testOOV("zh", vocab, false, true, zhExpectedOOVRates);
    testOOV("zh", vocab, true, false, zhExpectedOOVRates);
    testOOV("zh", vocab, false, false, zhExpectedOOVRates);    
  }

  @Test
  public void testTurkishOOVs() {
    VocabularyWritable vocab = new VocabularyWritable();
    List<String> sentences = readInput(dir + "data/tokenizer/test/tr-test.tok.stemmed.stop");
    for (String token : sentences.get(3).split(" ")) {
      vocab.addOrGet(token);
    }
    vocab.addOrGet("ispanyol");
    vocab.addOrGet("isim");
    vocab.addOrGet("10");

    float[] trStopStemExpectedOOVRates = {0.85714287f, 1f, 0.6f, 0f};
    float[] trStopExpectedOOVRates = {1f, 1f, 0.8f, 0.5f};
    float[] trStemExpectedOOVRates = {0.85714287f, 1f, 0.71428573f, 0.33333334f};
    float[] trExpectedOOVRates = {1f, 1f, 0.85714287f, 0.6666667f};

    testOOV("tr", vocab, true, true, trStopStemExpectedOOVRates);
    testOOV("tr", vocab, false, true, trStopExpectedOOVRates);
    testOOV("tr", vocab, true, false, trStemExpectedOOVRates);
    testOOV("tr", vocab, false, false, trExpectedOOVRates);    
  }

  @Test
  public void testArabicOOVs() {
    VocabularyWritable vocab = new VocabularyWritable();
    List<String> sentences = readInput(dir + "data/tokenizer/test/ar-test.tok.stemmed.stop");
    for (String token : sentences.get(0).split(" ")) {
      vocab.addOrGet(token);
    }
    vocab.addOrGet("2011");
    float[] arStopStemExpectedOOVRates = {0f, 1f, 0.8181818f, 1f};
    float[] arStopExpectedOOVRates = {0.6666667f, 1f, 0.8181818f, 1f};
    float[] arStemExpectedOOVRates = {0f, 1f, 0.85714287f, 1f};
    float[] arExpectedOOVRates = {0.6666667f, 1f, 0.85714287f, 1f};

    testOOV("ar", vocab, true, true, arStopStemExpectedOOVRates);
    testOOV("ar", vocab, false, true, arStopExpectedOOVRates);
    testOOV("ar", vocab, true, false, arStemExpectedOOVRates);
    testOOV("ar", vocab, false, false, arExpectedOOVRates);
  }

  @Test
  public void testEnglishOOVs() {
    VocabularyWritable vocab = new VocabularyWritable();
    vocab.addOrGet("r.d.");
    vocab.addOrGet("craig");
    vocab.addOrGet("dictionari");
    vocab.addOrGet("polynesian");
    vocab.addOrGet("mytholog");
    vocab.addOrGet("greenwood");
    vocab.addOrGet("press");
    vocab.addOrGet("new");
    vocab.addOrGet("york");
    vocab.addOrGet("1989");
    vocab.addOrGet("24");
    vocab.addOrGet("26");
    vocab.addOrGet("english");
    vocab.addOrGet("tree");
    vocab.addOrGet("einbaum");

    float[] enStopStemExpectedOOVRates = {1f, 18/19f, 4/7.0f, 0f};
    float[] enStopExpectedOOVRates = {1f, 18/19f, 4/7.0f, 2/12f};
    float[] enStemExpectedOOVRates = {1f, 36/37f, 15/18.0f, 7/19f};
    float[] enExpectedOOVRates = {1f, 36/37f, 15/18.0f, 9/19f};

    testOOV("en", vocab, true, true, enStopStemExpectedOOVRates);
    testOOV("en", vocab, false, true, enStopExpectedOOVRates);
    testOOV("en", vocab, true, false, enStemExpectedOOVRates);
    testOOV("en", vocab, false, false, enExpectedOOVRates);    
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(TokenizationTest.class);
  }
}
