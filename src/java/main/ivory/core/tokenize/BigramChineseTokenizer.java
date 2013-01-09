package ivory.core.tokenize;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class BigramChineseTokenizer extends Tokenizer {
  private static final Logger LOG = Logger.getLogger(BigramChineseTokenizer.class);
  static{
    LOG.setLevel(Level.INFO);
  }
  public BigramChineseTokenizer(){
    super();
  }

  @Override
  public void configure(Configuration conf) { }

  @Override
  public void configure(Configuration conf, FileSystem fs) {  }

  @Override
  public String[] processContent(String text) {
    int numTokens = 0;
    String[] chunks = text.split("\\s+");
    
    List<String> tokens = new ArrayList<String>();
    for (String chunk : chunks){
      chunk = chunk.toLowerCase();
//      LOG.info("chunk="+chunk.length());

      char prev = 0, cur;
      for (int i = 0; i < chunk.length(); i++) {
        cur = chunk.charAt(i);
        if (i > 0) {
          String bigram = "";
          bigram += prev;
          bigram += cur;
          tokens.add(bigram);
//          LOG.info("bigram="+bigram);
          numTokens++;
        }
        prev = cur;
      }
    }

    String[] tokensArr = new String[numTokens];
    return tokens.toArray(tokensArr); 
  }

  @Override
  public String removeBorderStopWords(String tokenizedText) {
    return tokenizedText;
  }

}