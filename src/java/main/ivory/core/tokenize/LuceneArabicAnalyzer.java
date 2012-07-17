package ivory.core.tokenize;

import ivory.core.Constants;

import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class LuceneArabicAnalyzer extends ivory.core.tokenize.Tokenizer {
//  private ArabicAnalyzer analyzer;
//  private TokenStream tokenStream;
  private boolean isStemming, isStopwordRemoval;
  private org.apache.lucene.analysis.Tokenizer tokenizer;

  @Override
  public void configure(Configuration conf) {
    configure(conf, null);
  }

  @Override
  public void configure(Configuration conf, FileSystem fs) {
    isStopwordRemoval = conf.getBoolean(Constants.Stopword, true);      
    isStemming = conf.getBoolean(Constants.Stemming, true);    
  }

  @Override
  public String[] processContent(String text) {   
    tokenizer = new StandardTokenizer(Version.LUCENE_35, new StringReader(text));
    TokenStream tokenStream = new LowerCaseFilter(Version.LUCENE_35, tokenizer);
    if (isStopwordRemoval) {
      tokenStream = new StopFilter( Version.LUCENE_35, tokenStream, (CharArraySet) ArabicAnalyzer.getDefaultStopSet());
    }
    tokenStream = new ArabicNormalizationFilter(tokenStream);
    if (isStemming) {
      tokenStream = new ArabicStemFilter(tokenStream);
    }

    CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.clearAttributes();
    String tokenized = "";
    try {
      while (tokenStream.incrementToken()) {
        tokenized += ( termAtt.toString() + " " );
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return tokenized.trim().split(" ");
  }

}
