package ivory.core.tokenize;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import cern.colt.Arrays;

import com.google.common.collect.Lists;

public class OverlappingNGramTokenizer extends Tokenizer {
  public OverlappingNGramTokenizer() {
    super();
  }

  @Override
  public void configure(Configuration conf) {
  }

  @Override
  public void configure(Configuration conf, FileSystem fs) {
  }

  @Override
  public String[] processContent(String text) {
    if (text.length() < 4) {
      return new String[] {};
    }
    List<String> tokens = Lists.newArrayList();
    for (int i = 0; i < text.length() - 4 + 1; i++) {
      tokens.add(text.substring(i, i + 4).toLowerCase());
    }

    String[] tokensArr = new String[tokens.size()];
    return tokens.toArray(tokensArr);
  }

  public static void main(String[] args) throws Exception {
    Tokenizer tokenizer = new OverlappingNGramTokenizer();
    System.out.println(Arrays.toString(tokenizer.processContent("abcDefg")));
  }
}