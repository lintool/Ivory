/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.core.tokenize;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import edu.umd.hooka.VocabularyWritable;
import java.lang.UnsupportedOperationException;

public abstract class Tokenizer {
  private static final Logger LOG = Logger.getLogger(Tokenizer.class);
  static{
    LOG.setLevel(Level.INFO);
  }
  public abstract void configure(Configuration conf);
  public abstract void configure(Configuration conf, FileSystem fs);
  public abstract String[] processContent(String text);
  
  /*
   A method to create a mapping from stemmed version of each token to non-stemmed version. Useful in IR tasks where we want to recover non-stemmed version.
   Implemented in some sub-classes.
   */
  public Map<String, String> getStem2NonStemMapping(String text) {
    throw new UnsupportedOperationException();
  }
  
  protected static String delims = "`~!@#^&*()-_=+]}[{\\|'\";:/?.>,<";
  protected static int MIN_LENGTH = 2, MAX_LENGTH = 50;
  protected VocabularyWritable vocab;
  protected boolean isStopwordRemoval = false, isStemming = false;  
  protected Set<String> stopwords;
  protected Set<String> stemmedStopwords;

  public boolean isStemming() {
    return isStemming;
  }

  public boolean isStopwordRemoval() {
    return isStopwordRemoval;
  }

  /**
   * Discard tokens not in the provided vocabulary.
   * 
   * @param v
   *    vocabulary for tokenizer
   */
  public void setVocab(VocabularyWritable v){
    vocab = v;
  }

  public VocabularyWritable getVocab(){
    return vocab;
  }

  protected Set<String> readInput(FileSystem fs, String file) {
    Set<String> lines = new HashSet<String>();
    try {
      if (file == null) {
        return lines;
      }
      LOG.info("File " + file + " exists? " + fs.exists(new Path(file)) + ", fs: "+fs);
      FSDataInputStream fis = fs.open(new Path(file));
      InputStreamReader isr = new InputStreamReader(fis, "UTF8");
      BufferedReader in = new BufferedReader(isr);
      String line;

      while ((line = in.readLine()) != null) {
        lines.add(line);
      }
      in.close();
      return lines;
    } catch (Exception e) {
      LOG.warn("Problem reading stopwords from " + file);
      throw new RuntimeException("Problem reading stopwords from " + file);
    }
  }

  /**
   * Method to return number of tokens in text. Subclasses may override for more efficient implementations.
   * 
   * @param text
   *    text to be processed.
   * @return
   *    number of tokens in text.
   */
  public int getNumberTokens(String text){
    return processContent(text).length;
  }

  public float getOOVRate(String text, VocabularyWritable vocab) {
    int countOOV = 0, countAll = 0;
    for (String token : processContent(text)) {
      countAll++;
      if ( vocab != null && vocab.get(token) <= 0) {
        countOOV++;
      } 
    }
    return (countOOV / (float)countAll);
  }

  /**
   * Method to remove non-unicode characters from token, to prevent errors in the preprocessing pipeline. Such cases exist in German Wikipedia. 
   * 
   * @param token
   *    token to check for non-unicode character
   * @return
   *    token without the non-unicode characters
   */
  public static String removeNonUnicodeChars(String token) {
    StringBuilder fixedToken = new StringBuilder();
    for (int i = 0; i < token.length(); i++) {
      char c = token.charAt(i);
      if (Character.getNumericValue(c) >= -1) {
        fixedToken.append(c);
      }
    }
    return fixedToken.toString();
  }

  /**
   * Check for the character (looks like reversed `) and normalize it to standard apostrophe
   * @param text French text
   * @return fixed version of the text 
   */
  public static String normalizeFrench(String text) {
    StringBuilder out = new StringBuilder();
    for (int i=0; i<text.length(); i++) {
      if (String.format("%04x", (int)text.charAt(i)).equals("2019")) {    // 
        out.append("' ");
      }else {      
        out.append(text.charAt(i));
      }
    }
    return out.toString();
  }  

  /**
   * Normalize apostrophe variations for better tokenization.
   *  
   * @param text
   *    text, before any tokenization
   * @return
   *    normalized text, ready to be run through tokenizer   
   */
  protected static String preNormalize(String text) {
    return text.replaceAll("\u2018", "'").replaceAll("\u2060", "'").replaceAll("\u201C", "\"").replaceAll("\u201D", "\"").replaceAll("\u201B", "'").replaceAll("\u201F", "\"").replaceAll("\u201E", "\"").replaceAll("\u00B4", "'").replaceAll("\u301F", "\"").replaceAll("\u2019", "'").replaceAll("\u0060", "'");
  }

  /**
   * Fix several common tokenization errors.
   *  
   * @param text
   *    text, after tokenization
   * @return
   *    text, after fixing possible errors
   */
  protected static String postNormalize(String text) {
    return text.replaceAll("\\((\\S)", "( $1").replaceAll("(\\S)\\)", "$1 )").replaceAll("''(\\S)", "'' $1").replaceAll("–", "-")
    .replaceAll("‑", "-").replaceAll("(\\S)-(\\S)", "$1 - $2").replaceAll("—", "——").replaceAll(" ' s ", " 's ").replaceAll(" l ' ", " l' ")
    .replaceAll("\"(\\S)", "\" $1").replaceAll("(\\S)\"", "$1 \"");
  }

  /**
   * Convert tokenStream object into a string.
   * 
   * @param tokenStream
   *    object returned by Lucene tokenizer
   * @return
   *    String corresponding to the tokens output by tokenStream
   */
  protected static String streamToString(TokenStream tokenStream) {
    CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.clearAttributes();
    StringBuilder tokenized = new StringBuilder();
    try {
      while (tokenStream.incrementToken()) {
        tokenized.append( termAtt.toString() + " " );
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return tokenized.toString().trim();
  }

  /**
   * Overrided by applicable implementing classes.
   * @param 
   *    token
   * @return
   *    true if parameter is a stopword, false otherwise
   */
  public boolean isStopWord(String token) {
    return delims.contains(token) || (isStemming() && stemmedStopwords.contains(token)) || (!isStemming() && stopwords.contains(token));
  }
  /**
   * Overrided by applicable implementing classes.
   * @param isStemmed
   *    true if token has been stemmed, false otherwise
   * @param token
   * @return
   *    true if token is a stopword, false otherwise
   */
  public boolean isStopWord(boolean isStemmed, String token) {
    return delims.contains(token) || (isStemmed && stemmedStopwords.contains(token)) || (!isStemmed && stopwords.contains(token));
  }

  public boolean isDiscard(String token) {
    return ( token.length() < MIN_LENGTH || token.length() > MAX_LENGTH || isStopWord(token) );
  }

  public boolean isDiscard(boolean isStemmed, String token) {
    return ( token.length() < MIN_LENGTH || token.length() > MAX_LENGTH || isStopWord(isStemmed, token) );
  }

  /**
   * Remove stop words from text that has been tokenized. Useful when postprocessing output of MT system, which is tokenized but not stopword'ed.
   *  
   * @param tokenizedText
   *    input text, assumed to be tokenized.
   * @return
   *    same text without the stop words.
   */
  @Deprecated
  public String removeBorderStopWords(String tokenizedText) {
    String[] tokens = tokenizedText.split(" ");
    int start = 0, end = tokens.length-1;

    for (int i = 0; i < tokens.length; i++) {
      if (!isStopWord(tokens[i])) {
        start = i;
        break;
      }
    }
    for (int i = tokens.length-1; i >= 0; i--) {
      if (!isStopWord(tokens[i])) {
        end = i;
        break;
      }
    }

    String output = "";
    for (int i = start; i <= end; i++) {
      output += ( tokens[i] + " " );
    }
    return output.trim();
  }

  public String stem(String token) {
    return token;
  }

  public String getUTF8(String token) {
    String utf8 = "";
    for (int i = 0; i < token.length(); i++){
      utf8 += String.format("%04x", (int)token.charAt(i))+" ";
    }
    return utf8.trim();
  }
  
  @SuppressWarnings("static-access")
  public static void main(String[] args) {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("full path to model file or directory").hasArg().withDescription("model file").create("model"));
    options.addOption(OptionBuilder.withArgName("full path to input file").hasArg().withDescription("input file").isRequired().create("input"));
    options.addOption(OptionBuilder.withArgName("full path to output file").hasArg().withDescription("output file").isRequired().create("output"));
    options.addOption(OptionBuilder.withArgName("en | zh | de | fr | ar | tr | es").hasArg().withDescription("2-character language code").isRequired().create("lang"));
    options.addOption(OptionBuilder.withArgName("path to stopwords list").hasArg().withDescription("one stopword per line").create("stopword"));
    options.addOption(OptionBuilder.withArgName("path to stemmed stopwords list").hasArg().withDescription("one stemmed stopword per line").create("stemmed_stopword"));
    options.addOption(OptionBuilder.withArgName("true|false").hasArg().withDescription("turn on/off stemming").create("stem"));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars").withArgName("jar packages").hasArg().create("libjars"));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      String stopwordList = null, stemmedStopwordList = null, modelFile = null;
      boolean isStem = true;
      cmdline = parser.parse(options, args);
      if(cmdline.hasOption("stopword")){
        stopwordList = cmdline.getOptionValue("stopword");
      }
      if(cmdline.hasOption("stemmed_stopword")){
        stemmedStopwordList = cmdline.getOptionValue("stemmed_stopword");
      }
      if(cmdline.hasOption("stem")){
        isStem = Boolean.parseBoolean(cmdline.getOptionValue("stem"));
      }
      if(cmdline.hasOption("model")){
        modelFile = cmdline.getOptionValue("model");
      }

      ivory.core.tokenize.Tokenizer tokenizer = TokenizerFactory.createTokenizer(
          cmdline.getOptionValue("lang"), 
          modelFile,
          isStem, 
          stopwordList, stemmedStopwordList,
          null);
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(cmdline.getOptionValue("output")), "UTF8"));
      BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(cmdline.getOptionValue("input")), "UTF8"));

      String line = null;
      while((line = in.readLine()) != null){
        String[] tokens = tokenizer.processContent(line);
        String s = "";
        for (String token : tokens) {
          s += token+" ";
        }
        out.write(s.trim() + "\n");
      }
      in.close();
      out.close();

    } catch (Exception exp) {
      System.out.println(exp);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "Tokenizer", options );
      System.exit(-1);   
    }
  }
}
