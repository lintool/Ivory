package ivory.core.tokenize;

import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.junit.Test;

public class LuceneArabicAnalyzerTest {

  @Test
  public void testTokens1() throws IOException{
    ivory.core.tokenize.Tokenizer tokenizer = new LuceneArabicAnalyzer();
    tokenizer.configure(null);

    String sentence = "قرأت كتابا أحمر";
    String[] expectedTokens = new String[] {"قر", "كتابا", "احمر"};

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
    ivory.core.tokenize.Tokenizer tokenizer = new LuceneArabicAnalyzer();
    tokenizer.configure(null);

    String sentence = "الأسبوع القادم سيكون مخصصاً مقالات الرياضيات من 4 أبريل 2011 إلى 10 أبريل 2011.";
    String[] expectedTokens = new String[] { "اسبوع", "قادم",  "سيك", "مخصصا", "مقال", "رياض", "4", "ابريل", "2011", "10", "ابريل", "2011"};

    String[] tokens = tokenizer.processContent(sentence);
    int tokenCnt = 0;
    for (String token : tokens) {
      System.out.println("Token "+tokenCnt+":"+token);
      assertTrue("token "+tokenCnt+"="+token+"NOT"+expectedTokens[tokenCnt], token.equals(expectedTokens[tokenCnt]));
      tokenCnt++;
    }
  }
  
  @Test
  public void testTokens3() throws IOException{
    ivory.core.tokenize.Tokenizer tokenizer = new LuceneArabicAnalyzer();
    tokenizer.configure(null);

    String sentence = "القوات الإثيوبية تسيطر على مدينة مَحَاس وسط الصومال بعد يوم من سيطرتها على مدينة عيل";
    String[] expectedTokens = new String[] { "قو","اثيوب",  "تسيطر", "مدين", "محاس", "وسط", "صومال", "يوم", "سيطرت", "مدين", "عيل"};

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
    ivory.core.tokenize.Tokenizer tokenizer = new LuceneArabicAnalyzer();
    tokenizer.configure(null);

    String sentence = "أين أنت ذاهب الليلة؟";
    String[] expectedTokens = new String[] { "اين", "ذاهب",  "ليل"};

    String[] tokens = tokenizer.processContent(sentence);
    int tokenCnt = 0;
    for (String token : tokens) {
      System.out.println("Token "+tokenCnt+":"+token);
      assertTrue("token "+tokenCnt+"="+token+"!="+expectedTokens[tokenCnt], token.equals(expectedTokens[tokenCnt]));
      tokenCnt++;
    }
  }
}
