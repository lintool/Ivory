package ivory.core.tokenize;

import ivory.core.Constants;

import java.io.IOException;
import java.io.StringReader;
import java.text.Normalizer;
import java.text.Normalizer.Form;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;

public class LuceneArabicAnalyzer extends ivory.core.tokenize.Tokenizer {
  private static final Logger LOG = Logger.getLogger(LuceneArabicAnalyzer.class);
  static{
    LOG.setLevel(Level.WARN);
  }
  private org.apache.lucene.analysis.Tokenizer tokenizer;

  @Override
  public void configure(Configuration conf) {
    configure(conf, null);
  }

  @Override
  public void configure(Configuration conf, FileSystem fs) {    
    // read stopwords from file (stopwords will be empty set if file does not exist or is empty)
    String stopwordsFile = conf.get(Constants.StopwordList);
    stopwords = readInput(fs, stopwordsFile);      
    String stemmedStopwordsFile = conf.get(Constants.StemmedStopwordList);
    stemmedStopwords = readInput(fs, stemmedStopwordsFile);
    isStopwordRemoval = !stopwords.isEmpty();
    isStemming = conf.getBoolean(Constants.Stemming, true);

    VocabularyWritable vocab;
    try {
      vocab = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get(Constants.CollectionVocab)), fs);
      setVocab(vocab);
    } catch (Exception e) {
      LOG.warn("No vocabulary provided to tokenizer.");
      vocab = null;
    }

    LOG.warn("Stemming is " + isStemming + "; Stopword removal is " + isStopwordRemoval +"; number of stopwords: " + stopwords.size() +"; stemmed: " + stemmedStopwords.size());
  }

  @Override
  public String[] processContent(String text) {   
    text = preNormalize(text);
    tokenizer = new StandardTokenizer(Version.LUCENE_43, new StringReader(text));
    TokenStream tokenStream = new LowerCaseFilter(Version.LUCENE_43, tokenizer);
    String tokenized = postNormalize(streamToString(tokenStream));
    tokenized = Normalizer.normalize(tokenized, Form.NFKC);

    StringBuilder finalTokenized = new StringBuilder();
    for (String token : tokenized.split(" ")) {
      if ( isStopwordRemoval() && isDiscard(false, token) ) {
        continue;
      }

      finalTokenized.append( token + " " );
    }
    String stemmedTokenized = finalTokenized.toString().trim();
    if (isStemming()) {
      // then, run the Lucene normalization and stemming on the stopword-removed text
      stemmedTokenized = stem(stemmedTokenized);       
    }
    return stemmedTokenized.split(" ");
  }


  @Override
  public String stem(String token) {
    StringBuilder stemmed = new StringBuilder();

    try {
      tokenizer = new StandardTokenizer(Version.LUCENE_43, new StringReader(token));
      TokenStream tokenStream = new ArabicStemFilter(new ArabicNormalizationFilter(tokenizer));
      CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
      tokenStream.reset();

      while (tokenStream.incrementToken()) {
        String curToken = cattr.toString();
        if (vocab != null && vocab.get(curToken) <= 0) {
          continue;
        }
        stemmed.append(curToken + " ");
      }
      tokenStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return stemmed.toString().trim();
  }

  @Override
  public float getOOVRate(String text, VocabularyWritable vocab) {
    int countOOV = 0, countAll = 0;
    tokenizer = new StandardTokenizer(Version.LUCENE_43, new StringReader(text));
    TokenStream tokenStream = new StandardFilter(Version.LUCENE_43, tokenizer);
    tokenStream = new LowerCaseFilter(Version.LUCENE_43, tokenStream);
    String tokenized = postNormalize(streamToString(tokenStream));

    StringBuilder finalTokenized = new StringBuilder();
    for (String token : tokenized.split(" ")) {
      if ( isStopwordRemoval() && isDiscard(false, token) ) {
        continue;
      }
      if (!isStemming()) {
        if ( vocab != null && vocab.get(token) <= 0) {
          countOOV++;
        }
        countAll++;
      }else {
        finalTokenized.append( token + " " );
      }
    }
    
    if (isStemming()) {
      tokenizer = new StandardTokenizer(Version.LUCENE_43, new StringReader(finalTokenized.toString().trim()));
      try {
        tokenStream = new ArabicStemFilter(new ArabicNormalizationFilter(tokenizer));
        CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
        tokenStream.reset();

        while (tokenStream.incrementToken()) {
          String curToken = cattr.toString();
          if ( vocab != null && vocab.get(curToken) <= 0) {
            countOOV++;
          }
          countAll++;
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return (countOOV / (float) countAll);
  }
}
