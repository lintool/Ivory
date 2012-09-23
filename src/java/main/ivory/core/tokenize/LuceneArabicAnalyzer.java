package ivory.core.tokenize;

import ivory.core.Constants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
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

import com.google.common.collect.Sets;

import edu.umd.hooka.VocabularyWritable;

public class LuceneArabicAnalyzer extends ivory.core.tokenize.Tokenizer {
  private static final Logger LOG = Logger.getLogger(LuceneArabicAnalyzer.class);
  private boolean isStemming;
  private org.apache.lucene.analysis.Tokenizer tokenizer;
  private final Set<String> arabicStopwords = Sets.newHashSet(LUCENE_STOP_WORDS); // new HashSet<String>();//
  private final Set<String> arabicStemmedStopwords = Sets.newHashSet(LUCENE_STEMMED_STOP_WORDS);
  private static final String[] LUCENE_STOP_WORDS = { 
    "من",
    "ومن",
    "منها",
    "منه",
    "في",
    "وفي",
    "فيها",
    "فيه",
    "و",
    "ف",
    "ثم",
    "او",
    "أو",
    "ب",
    "بها",
    "به",
    "ا",
    "أ",
    "اى",
    "اي",
    "أي",
    "أى",
    "لا",
    "ولا",
    "الا",
    "ألا",
    "إلا",
    "لكن",
    "ما",
    "وما",
    "كما",
    "فما",
    "عن",
    "مع",
    "اذا",
    "إذا",
    "ان",
    "أن",
    "إن",
    "انها",
    "أنها",
    "إنها",
    "انه",
    "أنه",
    "إنه",
    "بان",
    "بأن",
    "فان",
    "فأن",
    "وان",
    "وأن",
    "وإن",
    "التى",
    "التي",
    "الذى",
    "الذي",
    "الذين",
    "الى",
    "الي",
    "إلى",
    "إلي",
    "على",
    "عليها",
    "عليه",
    "اما",
    "أما",
    "إما",
    "ايضا",
    "أيضا",
    "كل",
    "وكل",
    "لم",
    "ولم",
    "لن",
    "ولن",
    "هى",
    "هي",
    "هو",
    "وهى",
    "وهي",
    "وهو",
    "فهى",
    "فهي",
    "فهو",
    "انت",
    "أنت",
    "لك",
    "لها",
    "له",
    "هذه",
    "هذا",
    "تلك",
    "ذلك",
    "هناك",
    "كانت",
    "كان",
    "يكون",
    "تكون",
    "وكانت",
    "وكان",
    "غير",
    "بعض",
    "قد",
    "نحو",
    "بين",
    "بينما",
    "منذ",
    "ضمن",
    "حيث",
    "الان",
    "الآن",
    "خلال",
    "بعد",
    "قبل",
    "حتى",
    "عند",
    "عندما",
    "لدى",
    "جميع",
    "ضد",
    "ت",
    "ل"
  };
  private static final String[] LUCENE_STEMMED_STOP_WORDS =   {
    "ومن",
    "من",
    "في",
    "وف",
    "في",
    "و",
    "ف",
    "ثم",
    "او",
    "ب",
    "بها",
    "به",
    "ا",
    "اي",
    "لا",
    "ولا",
    "الا",
    "لكن",
    "ما",
    "وما",
    "كما",
    "فما",
    "عن",
    "مع",
    "اذا",
    "اذا",
    "ان",
    "بان",
    "فان",
    "وان",
    "تي",
    "ذي",
    "ذين",
    "ال",
    "عل",
    "اما",
    "ايضا",
    "كل",
    "وكل",
    "لم",
    "ولم",
    "لن",
    "ولن",
    "هي",
    "هو",
    "وه",
    "وهو",
    "فه",
    "فهو",
    "انت",
    "لك",
    "لها",
    "له",
    "هذ",
    "هذا",
    "تلك",
    "هناك",
    "كانت",
    "كان",
    "تك",
    "كانت",
    "كان",
    "غير",
    "بعض",
    "قد",
    "نحو",
    "بين",
    "بينما",
    "منذ",
    "ضمن",
    "حيث",
    "ان",
    "خلال",
    "بعد",
    "قبل",
    "حت",
    "عند",
    "عندما",
    "لد",
    "جميع",
    "ضد",
    "ت",
    "ل"
  };

  @Override
  public void configure(Configuration conf) {
    configure(conf, null);
  }

  @Override
  public void configure(Configuration conf, FileSystem fs) {
    isStopwordRemoval = conf.getBoolean(Constants.Stopword, true);      
    isStemming = conf.getBoolean(Constants.Stemming, true);
    
//    try {
//      BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(conf.get("stopword")), "UTF8"));
//      BufferedReader in2 = new BufferedReader(new InputStreamReader(new FileInputStream(conf.get("stopword")+".stemmed"), "UTF8"));
//      String stopword = null;
//      while ((stopword = in.readLine()) != null) {
//      for (String stopword : LUCENE_STOP_WORDS) {
//        arabicStopwords.add(stopword.trim());
//        LOG.info("added1\n"+stopword.trim()+"\n"+getUTF8(stopword.trim()));
//      }
//      for (String stopword : LUCENE_STEMMED_STOP_WORDS) {
//      while ((stopword = in2.readLine()) != null) {
//        arabicStemmedStopwords.add(stopword.trim());
//        LOG.info("added2\n"+stopword.trim()+"\n"+getUTF8(stopword.trim()));
//      }
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
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
        String token = termAtt.toString();
        if ( vocab != null && vocab.get(token) <= 0) {
          continue;
        }
        tokenized += ( token + " " );
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return tokenized.trim().split("\\s+");
  }

  @Override
  public boolean isStopWord(String token) {
    return arabicStopwords.contains(token) || delims.contains(token) || token.length()==1;
  }

  @Override
  public String stem(String token) {
    tokenizer = new StandardTokenizer(Version.LUCENE_35, new StringReader(token));
    TokenStream tokenStream = new LowerCaseFilter(Version.LUCENE_35, tokenizer);
    tokenStream = new ArabicNormalizationFilter(tokenStream);
    tokenStream = new ArabicStemFilter(tokenStream);

    CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.clearAttributes();
    try {
      while (tokenStream.incrementToken()) {
        return termAtt.toString();
      }
    }catch (IOException e) {
      e.printStackTrace();
    }
    return token;
  }

  @Override
  public boolean isStemmedStopWord(String token) {
//    LOG.info("tested\n"+token+"\n"+getUTF8(token)+"\n"+arabicStemmedStopwords.contains(getUTF8(token)));
    return arabicStemmedStopwords.contains(token) || delims.contains(token) || token.length()==1;
  }
}
