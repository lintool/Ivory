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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import edu.umd.cloud9.io.map.HMapSIW;

public abstract class Tokenizer {
  public abstract void configure(Configuration conf);
  public abstract String[] processContent(String text);

  /**
   * Method to return number of tokens in text. Subclasses may override for more efficient implementations.
   * 
   * @param text
   * 		text to be processed.
   * @return
   * 		number of tokens in text.
   */
  public int getNumberTokens(String text){
    return processContent(text).length;
  }

  /**
   * Method to remove non-unicode characters from token, to prevent errors in the preprocessing pipeline. Such cases exist in German Wikipedia. 
   * 
   * @param token
   * 		token to check for non-unicode character
   * @return
   * 		token without the non-unicode characters
   */
  public String removeNonUnicodeChars(String token) {
    StringBuffer fixedToken = new StringBuffer();
    for (int i = 0; i < token.length(); i++) {
      char c = token.charAt(i);
      if (Character.getNumericValue(c) >= -1) {
        fixedToken.append(c);
      }
    }
    return fixedToken.toString();
  }


  /**
   * Check for the character ` and separate determiners such as l` and d` from the word following it.
   * @param 
   *    French text
   * @return 
   *    fixed version of the text 
   */
  public String normalizeFrench(String text) {
    StringBuffer out = new StringBuffer();
    for (int i=0; i<text.length(); i++) {
      out.append(text.charAt(i));
      if (String.format("%04x", (int)text.charAt(1)).equals("0027")) {    // 
        out.append(" ");
      }      
    }
    return out.toString();
  }  

  /**
   * Overrided by applicable implementing classes.
   * @param 
   *    eTerm
   * @return
   *    true if parameter is a stopword, false otherwise
   */
  public boolean isStopWord(String eTerm) {
    return false;
  }

  public abstract void configure(Configuration mJobConf, FileSystem fs);

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException{
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("full path to model file or directory").hasArg().withDescription("model file").create("model"));
    options.addOption(OptionBuilder.withArgName("full path to input file").hasArg().withDescription("input file").create("input"));
    options.addOption(OptionBuilder.withArgName("full path to output file").hasArg().withDescription("output file").create("output"));
    options.addOption(OptionBuilder.withArgName("en | zh | de | fr").hasArg().withDescription("2-character language code").create("lang"));
    options.addOption(OptionBuilder.withArgName("true|false").hasArg().withDescription("turn on/off stopword removal").create("stopword"));
    options.addOption(OptionBuilder.withArgName("true|false").hasArg().withDescription("turn on/off stemming").create("stem"));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      boolean isStopword = true, isStem = true;
      cmdline = parser.parse(options, args);
      if(cmdline.hasOption("stopword")){
        isStopword = Boolean.parseBoolean(cmdline.getOptionValue("stopword"));
      }
      if(cmdline.hasOption("stem")){
        isStem = Boolean.parseBoolean(cmdline.getOptionValue("stem"));
      }
      ivory.core.tokenize.Tokenizer tokenizer = TokenizerFactory.createTokenizer(
          cmdline.getOptionValue("lang"), 
          cmdline.getOptionValue("model"), 
          isStem, 
          isStopword,
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
        out.write(s+"\n");
      }
      out.close();

    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }
  }
}
