package ivory.core.util;

import ivory.core.data.dictionary.FrequencySortedDictionary;
import ivory.core.data.document.TermDocVector;
import ivory.core.data.stat.DfTableArray;
import ivory.core.tokenize.Tokenizer;
import ivory.pwsim.score.Bm25;
import ivory.pwsim.score.ScoringModel;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.stanford.nlp.util.StringUtils;
import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfFloatString;
import edu.umd.cloud9.io.pair.PairOfFloats;
import edu.umd.cloud9.io.pair.PairOfIntFloat;
import edu.umd.cloud9.util.map.HMapIF;
import edu.umd.cloud9.util.map.MapKF.Entry;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.alignment.IndexedFloatArray;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

/**
 * Algorithms used in our CLIR approach to convert doc vectors from one language into another. See SIGIR'11 paper for details.<p>
 * <p>
 * F is the "foreign" language, the language in which non-translated documents are written.<p>
 * E is the "non-foreign" language, the language into which documents are translated.<p>
 * <p>
 * Required files: <p>
 *    ttable E-->F (i.e., Pr(f|e))<p>
 *    ttable F-->E (i.e., Pr(e|f))<p>
 *    Pair of vocabulary files for each ttable<p> 
 *      V_E & V_F for E-->F<p>
 *      V_E & V_F for F-->E<p>
 * 
 * @author ferhanture
 *
 */
public class CLIRUtils extends Configured {
  private static final Logger logger = Logger.getLogger(CLIRUtils.class);
  private static final String delims = "`~!@#^&*()-_=+]}[{\\|'\";:/?.>,<";
  public static final String BitextSeparator = "<F2ELANG>";
  public static final int MinVectorTerms = 3;
  public static final int MinSentenceLength = 5;
  public static final int E = -1, F = 1;
  public static Pattern isNumber = Pattern.compile("\\d+");

  /**
   * Read df mapping from file.
   * 
   * @param path
   *    path to df table
   * @param fs
   *    FileSystem object
   * @return
   *    mapping from term ids to df values
   */
  public static HMapIFW readTransDfTable(Path path, FileSystem fs) {
    HMapIFW transDfTable = new HMapIFW();
    try {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());

      IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
      FloatWritable value = (FloatWritable) reader.getValueClass().newInstance();

      while (reader.next(key, value)) {
        transDfTable.put(key.get(), value.get());
        //        logger.info(key.get()+"-->"+value.get());
        key = (IntWritable) reader.getKeyClass().newInstance();
        value = (FloatWritable) reader.getValueClass().newInstance();
      }
      reader.close();
    } catch (Exception e) {
      throw new RuntimeException("Exception reading file trans-df table file");
    }
    return transDfTable;    
  }

  /**
   * @param vectorA
   *    a term document vector
   * @param vectorB
   *    another term document vector
   * @return
   *    cosine score
   */
  public static float cosine(HMapIFW vectorA, HMapIFW vectorB) {
    float sum = 0, magA = 0, magB = 0;
    for(edu.umd.cloud9.util.map.MapIF.Entry e : vectorA.entrySet()){
      float value = e.getValue();
      magA += (value * value);
      if(vectorB.containsKey(e.getKey())){
        sum+= value*vectorB.get(e.getKey());
      }
    }
    for(edu.umd.cloud9.util.map.MapIF.Entry e : vectorB.entrySet()){
      float value = e.getValue();
      magB += (value * value);
    }
    if(magA==0 || magB==0){
      return 0.0f;
    }else{
      return (float) (sum/(Math.sqrt(magA) * Math.sqrt(magB)));
    }
  }

  /**
   * @param vectorA
   *    a term document vector
   * @param vectorB
   *    another term document vector
   * @return
   *    cosine score
   */
  public static float cosine(HMapSFW vectorA, HMapSFW vectorB) {
    float sum = 0, magA = 0, magB = 0;
    for(edu.umd.cloud9.util.map.MapKF.Entry<String> e : vectorA.entrySet()){
      float value = e.getValue();
      magA += (value * value);
      if(vectorB.containsKey(e.getKey())){
        sum+= value*vectorB.get(e.getKey());
      }
    }
    for(edu.umd.cloud9.util.map.MapKF.Entry<String> e : vectorB.entrySet()){
      float value = e.getValue();
      magB += (value * value);
    }
    if(magA==0 || magB==0){
      return 0.0f;
    }else{
      return (float) (sum/(Math.sqrt(magA) * Math.sqrt(magB)));
    }
  }

  /**
   * 
   * @param vectorA
   *    a normalized term document vector
   * @param vectorB
   *    another normalized term document vector
   * @return
   *    cosine score
   */
  public static float cosineNormalized(HMapSFW vectorA, HMapSFW vectorB) {
    float sum = 0;
    for(edu.umd.cloud9.util.map.MapKF.Entry<String> e : vectorA.entrySet()){
      float value = e.getValue();
      if(vectorB.containsKey(e.getKey())){
        sum += value*vectorB.get(e.getKey());
      }
    }
    return sum;
  }

  /**
   * Given a mapping from F-terms to their df values, compute a df value for each E-term using the CLIR algorithm: df(e) = sum_f{df(f)*prob(f|e)}
   * 
   * @param eVocabSrc
   *    source-side vocabulary of the ttable E-->F (i.e., Pr(f|e))
   * @param fVocabTrg
   *    target-side vocabulary of the ttable E-->F (i.e., Pr(f|e))
   * @param e2f_probs
   *    ttable E-->F (i.e., Pr(f|e))
   * @return
   *    mapping from E-terms to their computed df values
   */
  public static HMapIFW translateDFTable(Vocab eVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs e2f_probs, FrequencySortedDictionary dict, DfTableArray dfTable){
    HMapIFW transDfTable = new HMapIFW();
    for(int e=1;e<eVocabSrc.size();e++){
      int[] fS = e2f_probs.get(e).getTranslations(0.0f);
      float df=0;
      for(int f : fS){
        float probEF = e2f_probs.get(e, f);
        String fTerm = fVocabTrg.get(f);
        int id = dict.getId(fTerm); 
        if(id != -1){
          float df_f = dfTable.getDf(id);       
          df += (probEF*df_f);
        }else{
          logger.debug(fTerm+" not in dict");
        }
      }
      transDfTable.put(e, df);
    }
    return transDfTable;
  }

  /**
   * Given a mapping from F-terms to their df values, compute a df value for each E-term using the CLIR algorithm: df(e) = sum_f{df(f)*prob(f|e)}
   * 
   * @param eVocabSrc
   *    source-side vocabulary of the ttable E-->F (i.e., Pr(f|e))
   * @param fVocabTrg
   *    target-side vocabulary of the ttable E-->F (i.e., Pr(f|e))
   * @param e2f_probs
   *    ttable E-->F (i.e., Pr(f|e))
   * @param dfs
   *    mapping from F-terms to their df values
   * @return
   *    mapping from E-terms to their computed df values
   */
  public static HMapIFW translateDFTable(Vocab eVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs e2f_probs, HMapSIW dfs){
    HMapIFW transDfTable = new HMapIFW();
    for(int e=1;e<eVocabSrc.size();e++){
      int[] fS = null;
      try {
        fS = e2f_probs.get(e).getTranslations(0.0f);
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      float df=0;
      for(int f : fS){
        float probEF = e2f_probs.get(e, f);
        String fTerm = fVocabTrg.get(f);
        if(!dfs.containsKey(fTerm)){  //only if word is in the collection, can it contribute to the df values.
          continue;
        }     
        float df_f = dfs.get(fTerm);
        df+=(probEF*df_f);
      }
      transDfTable.put(e, df);
    }
    return transDfTable;
  }

  /**
   * Given a term in a document in F, and its tf value, update the computed tf value for each term in E using the CLIR algorithm: tf(e) = sum_f{tf(f)*prob(e|f)} <p>
   * Calling this method computes a single summand of the above equation.
   * 
   * @param fTerm
   *    term in a document in F
   * @param tf
   *    term frequency of fTerm
   * @param tfTable
   *    to be updated, a mapping from E-term ids to tf values
   * @param eVocabSrc
   *    source-side vocabulary of the ttable E-->F (i.e., Pr(f|e))
   * @param eVocabTrg
   *    target-side vocabulary of the ttable F-->E (i.e., Pr(f|e))
   * @param fVocabSrc
   *    source-side vocabulary of the ttable F-->E (i.e., Pr(e|f))
   * @param fVocabTrg
   *    target-side vocabulary of the ttable E-->F (i.e., Pr(f|e))
   * @param e2fProbs
   *    ttable E-->F (i.e., Pr(f|e))
   * @param f2eProbs
   *    ttable F-->E (i.e., Pr(e|f))
   * @param sLogger
   *    Logger object for log output
   * @return
   *    updated mapping from E-term ids to tf values
   * @throws IOException
   */
  public static HMapIFW updateTFsByTerm(String fTerm, int tf, HMapIFW tfTable, Vocab eVocabSrc, Vocab eVocabTrg, Vocab fVocabSrc, Vocab fVocabTrg, 
      TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs, Tokenizer tokenizer, Logger sLogger){
    int f = fVocabSrc.get(fTerm);
    if(f <= 0){
      return tfTable;
    }

    int[] eS = f2eProbs.get(f).getTranslations(0.0f);

    // tf(e) = sum_f{tf(f)*prob(e|f)}
    for(int e : eS){
      float probEF;
      String eTerm = eVocabTrg.get(e);
      if (tokenizer.isDiscard(eTerm)) continue;

      probEF = f2eProbs.get(f, e);
      sLogger.debug("Prob(" + eTerm + " | " + fTerm + ") = " + probEF);
      if(probEF > 0){
        if(tfTable.containsKey(e)){
          tfTable.put(e, tfTable.get(e)+tf*probEF);
        }else{
          tfTable.put(e, tf*probEF);
        }
      }
    }
    return tfTable;
  }

  /**
   * Given a document in F, and its tf mapping, compute a tf value for each term in E using the CLIR algorithm: tf(e) = sum_f{tf(f)*prob(e|f)}
   * 
   * @param doc
   *    mapping from F-term strings to tf values
   * @param tfTable
   *    to be returned, a mapping from E-term ids to tf values
   * @param eVocabSrc
   *    source-side vocabulary of the ttable E-->F (i.e., Pr(f|e))
   * @param eVocabTrg
   *    target-side vocabulary of the ttable F-->E (i.e., Pr(f|e))
   * @param fVocabSrc
   *    source-side vocabulary of the ttable F-->E (i.e., Pr(e|f))
   * @param fVocabTrg
   *    target-side vocabulary of the ttable E-->F (i.e., Pr(f|e))
   * @param e2fProbs
   *    ttable E-->F (i.e., Pr(f|e))
   * @param f2eProbs
   *    ttable F-->E (i.e., Pr(e|f))
   * @param sLogger
   *    Logger object for log output
   * @throws IOException
   */
  public static int translateTFs(TermDocVector doc, HMapIFW tfTable, Vocab eVocabSrc, Vocab eVocabTrg, Vocab fVocabSrc, Vocab fVocabTrg, 
      TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs, Tokenizer tokenizer, Logger sLogger) throws IOException{
    if(sLogger == null){
      sLogger = logger;
    }

    //sLogger.setLevel(Level.DEBUG);

    //translate doc vector    
    TermDocVector.Reader reader = doc.getReader();
    int docLen = 0;
    while (reader.hasMoreTerms()) {
      String fTerm = reader.nextTerm();
      int tf = reader.getTf();
      docLen+=tf;

      sLogger.debug("Read "+fTerm+","+tf);

      int f = fVocabSrc.get(fTerm);
      if(f <= 0){
        sLogger.debug("Warning: "+f+","+fTerm+": word not in aligner's vocab (source side of f2e)");
        continue;
      }
      int[] eS = f2eProbs.get(f).getTranslations(0.0f);

      // tf(e) = sum_f{tf(f)*prob(e|f)}
      for(int e : eS){
        if(e<=0){   //if eTerm is NULL, that means there were cases where fTerm was unaligned in a sentence pair. Just skip these cases, since the word NULL is not in our target vocab.
          continue;
        }
        String eTerm = eVocabTrg.get(e);
        if (tokenizer.isDiscard(eTerm))  continue;

        float probEF = f2eProbs.get(f, e);
        if(probEF > 0){
          //          sLogger.debug(eTerm+" ==> "+probEF);
          tfTable.increment(e, tf*probEF);
          //          sLogger.debug("updated weight to "+tfTable.get(e));
        }
      }
    }

    return docLen;
  }

  /**
   * Given a document in F, and its tf mapping, compute a tf value for each term in E using the CLIR algorithm: tf(e) = sum_f{tf(f)*prob(e|f)}
   * 
   * @param doc
   *    mapping from F-term strings to tf values
   * @param tfTable
   *    to be returned, a mapping from E-term ids to tf values
   * @param eVocabSrc
   *    source-side vocabulary of the ttable E-->F (i.e., Pr(f|e))
   * @param eVocabTrg
   *    target-side vocabulary of the ttable F-->E (i.e., Pr(f|e))
   * @param fVocabSrc
   *    source-side vocabulary of the ttable F-->E (i.e., Pr(e|f))
   * @param fVocabTrg
   *    target-side vocabulary of the ttable E-->F (i.e., Pr(f|e))
   * @param e2fProbs
   *    ttable E-->F (i.e., Pr(f|e))
   * @param f2eProbs
   *    ttable F-->E (i.e., Pr(e|f))
   * @param sLogger
   *    Logger object for log output
   * @throws IOException
   */
  public static int translateTFs(HMapSIW doc, HMapIFW tfTable, Vocab eVocabSrc, Vocab eVocabTrg, Vocab fVocabSrc, Vocab fVocabTrg, 
      TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs, Tokenizer tokenizer, Logger sLogger) throws IOException{
    if(sLogger == null){
      sLogger = logger;
    }

    int docLen = 0;
    for(edu.umd.cloud9.util.map.MapKI.Entry<String> item : doc.entrySet()){
      String fTerm = item.getKey();
      int tf = item.getValue();
      docLen += tf;
      int f = fVocabSrc.get(fTerm);
      if(f <= 0){
        //        sLogger.warn(f+","+fTerm+": word not in aligner's vocab (source side of f2e)");
        continue;
      }
      int[] eS = f2eProbs.get(f).getTranslations(0.0f);
      sLogger.debug(fTerm+" has "+eS.length+" translations");

      // tf(e) = sum_f{tf(f)*prob(e|f)}
      float prob;
      for (int e : eS) {
        String eTerm = eVocabTrg.get(e);
        if (tokenizer.isStopWord(eTerm))  continue;

        prob = f2eProbs.get(f, e);
        //              sLogger.info(eTerm+" --> "+prob+","+f2eProbs.get(f, e));
        if(prob > 0){
          tfTable.increment(e, tf*prob);
        }
      }
    }


    return docLen;
  }

  /**
   * Given the TF, DF values, doc length, scoring model, this method creates the term doc vector for a document.
   * 
   * @param docLen
   *    doc length
   * @param tfTable
   *    mapping from term id to tf values
   * @param eVocab
   *    vocabulary object for final doc vector language
   * @param scoringModel model
   * @param dfTable
   *    mapping from term id to df values
   * @param isNormalize
   *    indicating whether to normalize the doc vector weights or not
   * @param sLogger
   *    Logger object for log output
   * @return
   *    Term doc vector representing the document
   */
  public static HMapSFW createTermDocVector(int docLen, HMapIFW tfTable, Vocab eVocab, ScoringModel scoringModel, HMapIFW dfTable, boolean isNormalize, Logger sLogger) {
    if(sLogger == null){
      sLogger = logger;
    }

    //sLogger.setLevel(Level.DEBUG);

    HMapSFW v = new HMapSFW();
    float normalization=0;
    for(int e : tfTable.keySet()){
      // retrieve term string, tf and df
      String eTerm = eVocab.get(e);
      float tf = tfTable.get(e);
      float df = dfTable.get(e);

      // compute score via scoring model
      float score = ((Bm25) scoringModel).computeDocumentWeight(tf, df, docLen);

      sLogger.debug(eTerm+" "+tf+" "+df+" "+score);
      if(score>0){
        v.put(eTerm, score);
        if(isNormalize){
          normalization+=Math.pow(score, 2);
        }   
      }
    }

    // length-normalize doc vector
    if(isNormalize){
      normalization = (float) Math.sqrt(normalization);
      for(Entry<String> e : v.entrySet()){
        v.put(e.getKey(), e.getValue()/normalization);
      }
    }
    return v;
  }

  /** 
   * called by BitextClassifierUtils
   **/
  public static HMapSFW createTermDocVector(int docLen, HMapIFW tfTable, Vocab eVocab, ScoringModel scoringModel, HMapSIW dfTable, boolean isNormalize, Logger sLogger) {
    if(sLogger == null){
      sLogger = logger;
    }

    //sLogger.setLevel(Level.DEBUG);

    HMapSFW v = new HMapSFW();
    float normalization=0;
    for(int e : tfTable.keySet()){
      // retrieve term string, tf and df
      String eTerm = eVocab.get(e);
      float tf = tfTable.get(e);
      float df = dfTable.get(eTerm);

      // compute score via scoring model
      float score = ((Bm25) scoringModel).computeDocumentWeight(tf, df, docLen);

      sLogger.debug(eTerm+" "+tf+" "+df+" "+score);
      if(score>0){
        v.put(eTerm, score);
        if(isNormalize){
          normalization+=Math.pow(score, 2);
        }   
      }
    }

    // length-normalize doc vector
    if(isNormalize){
      normalization = (float) Math.sqrt(normalization);
      for(Entry<String> e : v.entrySet()){
        v.put(e.getKey(), e.getValue()/normalization);
      }
    }
    return v;
  }

  /**
   * Given the TF, DF values, doc length, scoring model, this method creates the term doc vector for a document.
   * 
   * @param docLen
   *    doc length
   * @param tfTable
   *    mapping from term id to tf values
   * @param eVocab
   *    vocabulary object for final doc vector language
   * @param scoringModel model
   * @param dfTable
   *    mapping from term id to df values
   * @param isNormalize
   *    indicating whether to normalize the doc vector weights or not
   * @param sLogger
   *    Logger object for log output
   * @return term doc vector representing the document
   */
  public static HMapSFW createTermDocVector(int docLen, HMapIFW tfTable, Vocab eVocab, ScoringModel scoringModel, FrequencySortedDictionary dict, DfTableArray dfTable, boolean isNormalize, Logger sLogger) {
    if(sLogger == null){
      sLogger = logger;
    }

    //    sLogger.setLevel(Level.DEBUG);

    HMapSFW v = new HMapSFW();
    float normalization=0;
    for(edu.umd.cloud9.util.map.MapIF.Entry entry : tfTable.entrySet()){
      // retrieve term string, tf and df
      String eTerm = eVocab.get(entry.getKey());
      float tf = entry.getValue();
      int eId = dict.getId(eTerm);
      if(eId < 1){    //OOV
        continue;
      }
      int df = dfTable.getDf(eId);
      // compute score via scoring model
      float score = ((Bm25) scoringModel).computeDocumentWeight(tf, df, docLen);
      if(df<1){
        sLogger.warn("Suspicious DF WARNING = "+eTerm+" "+tf+" "+df+" "+score);
      }

      sLogger.debug(eTerm+" "+tf+" "+df+" "+score);

      if(score>0){
        v.put(eTerm, score);
        if(isNormalize){
          normalization+=Math.pow(score, 2);
        }   
      }
    }

    // length-normalize doc vector
    if(isNormalize){
      normalization = (float) Math.sqrt(normalization);
      for(Entry<String> e : v.entrySet()){
        v.put(e.getKey(), e.getValue()/normalization);
      }
    }
    return v;
  }

  /**
   * Given the TF, DF values, doc length, scoring model, this method creates the term doc vector for a document.
   * 
   * @param docLen
   *    doc length
   * @param tfTable
   *    mapping from term string to tf values
   * @param scoringModel model
   * @param dfTable
   *    mapping from term id to df values
   * @param isNormalize
   *    indicating whether to normalize the doc vector weights or not
   * @param sLogger
   *    Logger object for log output
   * @return
   *    Term doc vector representing the document
   */
  public static HMapSFW createTermDocVector(int docLen, HMapSIW tfTable, ScoringModel scoringModel, FrequencySortedDictionary dict, DfTableArray dfTable, boolean isNormalize, Logger sLogger) {
    if(sLogger == null){
      sLogger = logger;
    }

    HMapSFW v = new HMapSFW();
    float normalization=0;
    for(edu.umd.cloud9.util.map.MapKI.Entry<String> entry : tfTable.entrySet()){
      // retrieve term string, tf and df
      String eTerm = entry.getKey();
      float tf = entry.getValue();
      int eId = dict.getId(eTerm);
      if(eId < 1){    //OOV
        continue;
      }
      int df = dfTable.getDf(eId);
      // compute score via scoring model
      float score = ((Bm25) scoringModel).computeDocumentWeight(tf, df, docLen);
      if(df<1){
        sLogger.warn("Suspicious DF WARNING = "+eTerm+" "+tf+" "+df+" "+score);
      }

      sLogger.debug(eTerm+" "+tf+" "+df+" "+score);

      if(score>0){
        v.put(eTerm, score);
        if(isNormalize){
          normalization+=Math.pow(score, 2);
        }   
      }
    }

    // length-normalize doc vector
    if(isNormalize){
      normalization = (float) Math.sqrt(normalization);
      for(Entry<String> e : v.entrySet()){
        v.put(e.getKey(), e.getValue()/normalization);
      }
    }
    return v;
  }

  /***
   * 
   * Hooka helper functions
   * 
   */


  /**
   * This method converts the output of BerkeleyAligner into a TTable_monolithic_IFAs object. 
   * For each source language term, top numTrans entries (with highest translation probability) are kept, unless the top K < numTrans entries have a cumulatite probability above PROB_THRESHOLD.
   * 
   * @param inputFile
   *    output of Berkeley Aligner (probability values from source language to target language). Format should be: 
   *      [source-word] entropy ... nTrans ... sum 1.000000
   *        [target-word1]: [prob1]
   *        [target-word2]: [prob2]
   *        ..
   * @param srcVocabFile
   *    path where created source vocabulary (VocabularyWritable) will be written
   * @param trgVocabFile
   *    path where created target vocabulary (VocabularyWritable) will be written
   * @param probsFile
   *    path where created probability table (TTable_monolithic_IFAs) will be written
   * @param fs
   *    FileSystem object
   * @throws IOException
   */
  public static void createTTableFromBerkeleyAligner(String inputFile, String srcVocabFile, String trgVocabFile, String probsFile, 
      float probThreshold, int numTrans, FileSystem fs) throws IOException{
    TTable_monolithic_IFAs table = new TTable_monolithic_IFAs();
    VocabularyWritable trgVocab = new VocabularyWritable(), srcVocab = new VocabularyWritable();
    int cnt = 0;    // for statistical purposes only
    HookaStats stats = new HookaStats(numTrans, probThreshold);

    //In BerkeleyAligner output, dictionary entries of each source term are already sorted by prob. value. 
    try {
      DataInputStream d = new DataInputStream(fs.open(new Path(inputFile)));
      BufferedReader inputReader = new BufferedReader(new InputStreamReader(d));
      String cur = null;
      boolean earlyTerminate = false;
      String line = "";
      while (true) {
        if(!earlyTerminate){
          line = inputReader.readLine();
          if (line == null)
            break;
          cnt++;
        }
        earlyTerminate = false;
        logger.debug("Line:"+line);

        Pattern p = Pattern.compile("(.+)\\tentropy .+nTrans"); 
        Matcher m = p.matcher(line);
        if ( m.find() ) {
          cur = m.group(1);

          int gerIndex = srcVocab.addOrGet(cur);  
          logger.debug("Found: "+cur+" with index: "+gerIndex);


          List<PairOfIntFloat> indexProbPairs = new ArrayList<PairOfIntFloat>();
          float sumOfProbs = 0.0f;
          int i = 0;
          while ( i++ < numTrans ) {
            line = inputReader.readLine(); 
            if ( line == null ) {
              break;
            }else {
              cnt++;
              // check if we've already consumed all translations of this term -- if so, terminate loop
              Pattern p2 = Pattern.compile("\\s*(\\S+): (.+)");
              Matcher m2 = p2.matcher(line);
              if ( !m2.find() ) {
                m = p.matcher(line);
                if ( m.find() ) {
                  logger.debug("Early terminate");
                  earlyTerminate = true;
                  i = numTrans;
                  break;
                }
                //                logger.debug("FFFF"+line);
              } else {
                String term = m2.group(1);
                if ( !term.equals("NULL") ) {
                  float prob = Float.parseFloat(m2.group(2));
                  int engIndex = trgVocab.addOrGet(term);
                  logger.debug("Added: "+term+" with index: "+engIndex+" and prob:"+prob);
                  indexProbPairs.add(new PairOfIntFloat(engIndex, prob));
                  sumOfProbs += prob;
                }
              }
            }
            // if number of translations not set, we never cut-off, so all cases are long tails 
            if ( numTrans != Integer.MAX_VALUE && sumOfProbs > probThreshold ){
              stats.incCntShortTail(1);
              stats.incSumShortTail(i);
              break;
            }
          }
          if ( sumOfProbs <= probThreshold ){
            // early cut-off
            stats.incCntLongTail(1);
            stats.incSumLongTail(i);
            stats.incSumCumProbs(sumOfProbs);
          }

          // to enable faster access with binary search, we sort entries by vocabulary index.
          Collections.sort(indexProbPairs);
          int numEntries = indexProbPairs.size();
          int[] indices = new int[numEntries];
          float[] probs = new float[numEntries];
          i=0;
          for ( PairOfIntFloat pair : indexProbPairs ) {
            indices[i] = pair.getLeftElement();
            probs[i++] = pair.getRightElement() / sumOfProbs;
          }         
          table.set(gerIndex, new IndexedFloatArray(indices, probs, true));
        }
      }

      // dispose all the resources after using them.
      inputReader.close();
    }catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.err.println("File "+inputFile+": read "+cnt+" lines");
    System.err.println("Vocabulary Target: "+trgVocab.size()+" elements");
    System.err.println("Vocabulary Source: "+srcVocab.size()+" elements");
    System.err.println(stats);

    FSDataOutputStream outputStream1 = fs.create(new Path(trgVocabFile));
    ((VocabularyWritable) trgVocab).write(outputStream1);
    outputStream1.close();
    FSDataOutputStream outputStream2 = fs.create(new Path(srcVocabFile));
    ((VocabularyWritable) srcVocab).write(outputStream2);
    outputStream2.close();
    FSDataOutputStream outputStream3 = fs.create(new Path(probsFile));
    table.write(outputStream3);
    outputStream3.close();
  }


  /**
   * This method converts the output of GIZA into a TTable_monolithic_IFAs object. 
   * For each source language term, top numTrans entries (with highest translation probability) are kept, unless the top K < numTrans entries have a cumulatite probability above probThreshold.
   * 
   * @param filename
   *    output of GIZA (probability values from source language to target language. In GIZA, format of each line should be: 
   *      [target-word1] [source-word] [prob1]
   *      [target-word2] [source-word] [prob2]
   *          ...
   * @param srcVocabFile
   *    path where created source vocabulary (VocabularyWritable) will be written
   * @param trgVocabFile
   *    path where created target vocabulary (VocabularyWritable) will be written
   * @param probsFile
   *    path where created probability table (TTable_monolithic_IFAs) will be written
   * @param fs
   *    FileSystem object
   * @throws IOException
   */
  public static void createTTableFromGIZA(String inputFile, String srcVocabFile, String trgVocabFile, String probsFile, 
      float probThreshold, int numTrans, FileSystem fs) throws IOException{
    TTable_monolithic_IFAs table = new TTable_monolithic_IFAs();
    VocabularyWritable trgVocab = new VocabularyWritable(), srcVocab = new VocabularyWritable();

    int cnt = 0;

    //In GIZA output, dictionary entries are in random order (w.r.t. prob value), so you need to keep a sorted list of top numTrans or less entries w/o exceeding <probThreshold> probability
    try {
      DataInputStream d = new DataInputStream(fs.open(new Path(inputFile)));
      BufferedReader inputReader = new BufferedReader(new InputStreamReader(d));

      String srcTerm = null, trgTerm = null, prev = null;
      int curIndex = -1;
      TreeSet<PairOfFloatString> topTrans = new TreeSet<PairOfFloatString>();
      String line = "";
      boolean earlyTerminate = false, skipTerm = false;
      float sumOfProbs = 0.0f, prob;
      HookaStats stats = new HookaStats(numTrans, probThreshold);

      while (true) {  
        //        line = bis.readLine();
        line = inputReader.readLine();
        if(line == null)  break;
        String[] parts = line.split(" ");
        if(parts.length != 3){
          throw new RuntimeException("Unknown format: "+cnt+" = \n"+line);
        }
        cnt++;
        trgTerm = parts[0];
        srcTerm = parts[1];
        prob = Float.parseFloat(parts[2]);

        if (trgTerm.equals("NULL")) {
          continue;   // skip alignments to imaginary NULL word
        }

        // new source term (ignore punctuation)
        if ((prev==null || !srcTerm.equals(prev)) && !delims.contains(srcTerm)){
          if(topTrans.size() > 0){
            // store previous term's top translations to ttable
            addToTable(curIndex, topTrans, sumOfProbs, table, trgVocab, probThreshold, stats);
          }

          logger.debug("Line:"+line);

          // initialize the translation distribution of the source term
          sumOfProbs = 0.0f;
          topTrans.clear();
          earlyTerminate = false;   // reset status
          skipTerm = false;
          prev = srcTerm;
          int prevIndex = curIndex;
          curIndex = srcVocab.addOrGet(srcTerm);
          if(curIndex <= prevIndex){
            // we've seen this foreign term before. probably due to tokenization or sorting error in aligner. just ignore.
            logger.debug("FLAG: "+line);
            curIndex = prevIndex;   // revert curIndex value since we're skipping this one
            skipTerm = true;
            continue;
          }
          logger.debug("Processing: "+srcTerm+" with index: "+curIndex);      
          topTrans.add(new PairOfFloatString(prob, trgTerm));
          sumOfProbs += prob;
          logger.debug("Added to queue: "+trgTerm+" with prob: "+prob+" (sum: "+sumOfProbs+")");      
        }else if(!earlyTerminate && !skipTerm && !delims.contains(srcTerm)){  //continue adding translation term,prob pairs (except if early termination is ON)
          topTrans.add(new PairOfFloatString(prob, trgTerm));
          sumOfProbs += prob;
          logger.debug("Added to queue: "+trgTerm+" with prob: "+prob+" (sum: "+sumOfProbs+")");      

          // keep top numTrans translations
          if(topTrans.size() > numTrans){
            PairOfFloatString pair = topTrans.pollFirst();
            float removedProb = pair.getLeftElement();
            sumOfProbs -= removedProb;
            logger.debug("Removed from queue: "+pair.getRightElement()+" (sum: "+sumOfProbs+")");      
          }
        }else{
          logger.debug("Skipped line: "+line);
        }
      }

      //last one
      if(topTrans.size()>0){
        //store previous term's top translations to ttable
        addToTable(curIndex, topTrans, sumOfProbs, table, trgVocab, probThreshold, stats);
      }

      // dispose all the resources after using them.
      inputReader.close();

      System.err.println("File " + inputFile + ": read " + cnt + " lines");
      System.err.println("Vocabulary Target: " + trgVocab.size() + " elements");
      System.err.println("Vocabulary Source: " + srcVocab.size() + " elements");
      System.err.println(stats);
    }catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    FSDataOutputStream outputStream1 = fs.create(new Path(trgVocabFile));
    ((VocabularyWritable) trgVocab).write(outputStream1);
    outputStream1.close();
    FSDataOutputStream outputStream2 = fs.create(new Path(srcVocabFile));
    ((VocabularyWritable) srcVocab).write(outputStream2);
    outputStream2.close();
    FSDataOutputStream outputStream3 = fs.create(new Path(probsFile));
    table.write(outputStream3);
    outputStream3.close();
  }

  /**
   * This method modifies the TTable_monolithic_IFAs object output by Hooka, to meet following criteria: 
   * For each source language term, top numTrans entries (with highest translation probability) are kept, unless the top K < numTrans entries have a cumulatite probability above probThreshold.
   * 
   * @param srcVocabFile
   *    path to source vocabulary file output by Hooka
   * @param trgVocabFile
   *    path to target vocabulary file output by Hooka
   * @param tableFile
   *    path to ttable file output by Hooka
   * @param finalSrcVocabFile
   *    path where created source vocabulary (VocabularyWritable) will be written
   * @param finalTrgVocabFile
   *    path where created target vocabulary (VocabularyWritable) will be written
   * @param finalTableFile
   *    path where created probability table (TTable_monolithic_IFAs) will be written
   * @param fs
   *    FileSystem object
   * @throws IOException
   */
  public static void createTTableFromHooka(String srcVocabFile, String trgVocabFile, String tableFile, String finalSrcVocabFile, 
      String finalTrgVocabFile, String finalTableFile, float probThreshold, int numTrans, FileSystem fs) throws IOException{
    logger.setLevel(Level.DEBUG);
    Vocab srcVocab = HadoopAlign.loadVocab(new Path(srcVocabFile), fs);
    Vocab trgVocab = HadoopAlign.loadVocab(new Path(trgVocabFile), fs);
    TTable_monolithic_IFAs ttable = new TTable_monolithic_IFAs(fs, new Path(tableFile), true);

    logger.debug(ttable.getMaxE() + "," + ttable.getMaxF());

    Vocab finalSrcVocab = new VocabularyWritable();
    Vocab finalTrgVocab = new VocabularyWritable();
    TTable_monolithic_IFAs finalTTable = new TTable_monolithic_IFAs();

    String srcTerm = null, trgTerm = null;
    int curIndex = -1;
    TreeSet<PairOfFloatString> topTrans = new TreeSet<PairOfFloatString>();
    float sumOfProbs = 0.0f, prob;
    //    int cntLongTail = 0, cntShortTail = 0, sumShortTail = 0;    // for statistical purposes only
    HookaStats stats = new HookaStats(numTrans, probThreshold);

    //modify current ttable wrt foll. criteria: top numTrans translations per source term, unless cumulative prob. distr. exceeds probThreshold before that.
    for (int srcIndex = 1; srcIndex < srcVocab.size(); srcIndex++) {
      int[] translations;
      try {
        translations = ttable.get(srcIndex).getTranslations(0f);
      } catch (Exception e) {
        logger.warn("No translations found for "+srcVocab.get(srcIndex)+". Ignoring...");
        continue;
      }

      srcTerm = srcVocab.get(srcIndex);
      curIndex = finalSrcVocab.addOrGet(srcTerm);

      //initialize this term
      topTrans.clear();
      sumOfProbs = 0.0f;
      logger.debug("Processing: " + srcTerm + " with index: " + curIndex + " ("+srcIndex+"); " + translations.length + " translations");
      for (int trgIndex : translations) {
        try {
          trgTerm = trgVocab.get(trgIndex);
        } catch (Exception e) {
          logger.debug("Skipping " + trgIndex);
          continue;
        }
        prob = ttable.get(srcIndex, trgIndex);
        logger.debug("Found: " + trgTerm + " with " + prob);

        topTrans.add(new PairOfFloatString(prob, trgTerm));
        // keep top numTrans translations
        if (topTrans.size() > numTrans) {
          float removedProb = topTrans.pollFirst().getLeftElement();
          sumOfProbs -= removedProb;
        }
        sumOfProbs += prob;

        if (sumOfProbs > probThreshold) {
          logger.debug("Sum of probs > "+probThreshold+", early termination.");
          break;
        } 
      }

      //store previous term's top translations to ttable
      if(topTrans.size() > 0){
        addToTable(curIndex, topTrans, sumOfProbs, finalTTable, finalTrgVocab, probThreshold, stats);
      }
    }
    System.err.println("Vocabulary Target: "+finalTrgVocab.size()+" elements");
    System.err.println("Vocabulary Source: "+finalSrcVocab.size()+" elements");
    System.err.println(stats);

    FSDataOutputStream outputStream1 = fs.create(new Path(finalTrgVocabFile));
    ((VocabularyWritable) finalTrgVocab).write(outputStream1);
    outputStream1.close();
    FSDataOutputStream outputStream2 = fs.create(new Path(finalSrcVocabFile));
    ((VocabularyWritable) finalSrcVocab).write(outputStream2);
    outputStream2.close();
    FSDataOutputStream outputStream3 = fs.create(new Path(finalTableFile));
    finalTTable.write(outputStream3);
    outputStream3.close();
  }


  public static void addToTable(int curIndex, TreeSet<PairOfFloatString> topTrans, float cumProb, TTable_monolithic_IFAs table, 
      Vocab trgVocab, float cumProbThreshold, HookaStats stats) {
    List<Integer> sortedIndices = new ArrayList<Integer>();
    HMapIF index2ProbMap = new HMapIF();

    float sumOfProbs = 0.0f;    //only extract the top K<15 if the mass prob. exceeds MAX_probThreshold
    while(!topTrans.isEmpty() && sumOfProbs < cumProbThreshold){
      PairOfFloatString e = topTrans.pollLast();
      String term = e.getRightElement();
      float pr = e.getLeftElement()/cumProb;    // normalize
      logger.debug(term+"-->"+pr);
      int trgIndex = trgVocab.addOrGet(term);
      sumOfProbs += e.getLeftElement();         // keep track of unnormalized cumulative prob for determining cutoff
      sortedIndices.add(trgIndex);
      index2ProbMap.put(trgIndex, pr);
    }

    // to enable faster access with binary search, we sort entries by vocabulary index.
    Collections.sort(sortedIndices);
    int numEntries = sortedIndices.size();

    // for statistics only
    stats.update(numEntries, sumOfProbs);

    // write translation list to TTable object
    int[] indices = new int[numEntries];
    float[] probs = new float[numEntries];
    int i=0;
    for(int sortedIndex : sortedIndices){
      indices[i]=sortedIndex;
      probs[i]=index2ProbMap.get(sortedIndex);
      i++;
    }      
    table.set(curIndex, new IndexedFloatArray(indices, probs, true));
  }

  /**
   * A work in progress: can we use bidirectional translation probabilities to improve the translation table and vocabularies?
   */
  private static void combineTTables(String ttableFile, String srcEVocabFile, String trgFVocabFile, String ttableE2FFile, 
      String srcFVocabFile, String trgEVocabFile, String ttableF2EFile){
    TTable_monolithic_IFAs table = new TTable_monolithic_IFAs();
    Configuration conf = new Configuration();
    HookaStats stats = new HookaStats(-1, -1);
    try {
      FileSystem fs = FileSystem.get(conf);
      Vocab eVocabTrg = HadoopAlign.loadVocab(new Path(trgEVocabFile), conf);
      Vocab fVocabSrc = HadoopAlign.loadVocab(new Path(srcFVocabFile), conf);
      TTable_monolithic_IFAs f2e_Probs = new TTable_monolithic_IFAs(fs, new Path(ttableF2EFile), true);
      Vocab eVocabSrc = HadoopAlign.loadVocab(new Path(srcEVocabFile), conf);
      Vocab fVocabTrg = HadoopAlign.loadVocab(new Path(trgFVocabFile), conf);
      TTable_monolithic_IFAs e2f_Probs = new TTable_monolithic_IFAs(fs, new Path(ttableE2FFile), true);

      TreeSet<PairOfFloatString> topTrans = new TreeSet<PairOfFloatString>();
      for (int e1 = 1; e1 < eVocabSrc.size(); e1++) {
        String eTerm = eVocabSrc.get(e1);

        float sumOfProbs = 0;
        int[] fS = e2f_Probs.get(e1).getTranslations(0.0f);
        for (int f1 : fS) {
          float prob1 = e2f_Probs.get(e1, f1);

          String fTerm = fVocabTrg.get(f1);         
          int f2 = fVocabSrc.get(fTerm);
          int e2 = eVocabTrg.get(eTerm);         

          float prob2 = f2e_Probs.get(f2, e2);
          float prob = prob1*prob2;
          sumOfProbs += prob;
          topTrans.add(new PairOfFloatString(prob, fTerm));
        }
        logger.info("Adding "+eTerm);
        addToTable(e1, topTrans, sumOfProbs, table, fVocabTrg, 1.0f, stats);      
      }
      logger.info(stats);
      DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fs.create(new Path(ttableFile))));
      table.write(dos);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  /***
   * 
   * Bitext extraction helper functions
   * 
   */

  public static String[] computeFeaturesF1(HMapSFW eVector, HMapSFW translatedFVector, float eSentLength, float fSentLength) {
    return computeFeatures(1, null, null, null, null, null, eVector, null, translatedFVector, eSentLength, fSentLength, null, null, null, null, null, null, 0);
  }

  public static String[] computeFeaturesF2(HMapSIW eSrcTfs, HMapSFW eVector, HMapSIW fSrcTfs, HMapSFW translatedFVector, float eSentLength, float fSentLength,
      Vocab eVocabSrc, Vocab eVocabTrg, Vocab fVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs e2f_Probs, TTable_monolithic_IFAs f2e_Probs, float prob){
    return computeFeatures(2, null, null, null, null, eSrcTfs, eVector, fSrcTfs, translatedFVector, 
        eSentLength, fSentLength, eVocabSrc, eVocabTrg, fVocabSrc, fVocabTrg, e2f_Probs, f2e_Probs, prob); 
  }

  public static String[] computeFeaturesF3(String fSentence, String eSentence, Tokenizer fTokenizer, Tokenizer eTokenizer, 
      HMapSIW eSrcTfs, HMapSFW eVector, HMapSIW fSrcTfs, HMapSFW translatedFVector, float eSentLength, float fSentLength,
      Vocab eVocabSrc, Vocab eVocabTrg, Vocab fVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs e2f_Probs, TTable_monolithic_IFAs f2e_Probs, float prob){
    return computeFeatures(3, fSentence, eSentence, fTokenizer, eTokenizer, eSrcTfs, eVector, fSrcTfs, translatedFVector, 
        eSentLength, fSentLength, eVocabSrc, eVocabTrg, fVocabSrc, fVocabTrg, e2f_Probs, f2e_Probs, prob); 
  }

  public static String[] computeFeatures(int featSet, String fSentence, String eSentence, Tokenizer fTokenizer, Tokenizer eTokenizer, 
      HMapSIW eSrcTfs, HMapSFW eVector, HMapSIW fSrcTfs, HMapSFW translatedFVector, float eSentLength, float fSentLength,
      Vocab eVocabSrc, Vocab eVocabTrg, Vocab fVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs e2f_Probs, TTable_monolithic_IFAs f2e_Probs, float prob){
    return computeFeatures(featSet, fSentence, eSentence, fTokenizer, eTokenizer, eSrcTfs, eVector, fSrcTfs, translatedFVector, 
        eSentLength, fSentLength, eVocabSrc, eVocabTrg, fVocabSrc, fVocabTrg, e2f_Probs, f2e_Probs, prob, logger);
  }

  public static String[] computeFeatures(int featSet, String fSentence, String eSentence, Tokenizer fTokenizer, Tokenizer eTokenizer,
      HMapSIW eSrcTfs, HMapSFW eVector, HMapSIW fSrcTfs, HMapSFW translatedFVector, float eSentLength, float fSentLength,
      Vocab eVocabSrc, Vocab eVocabTrg, Vocab fVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs e2f_Probs, TTable_monolithic_IFAs f2e_Probs, float prob, Logger sLogger) {
    List<String> features = new ArrayList<String>();
    if(fSentLength == 0 || eSentLength == 0){
      return null;
    }

    features.add("cosine=" + CLIRUtils.cosineNormalized(eVector, translatedFVector));    

    ////////////////////////////////////////////////

    if (featSet > 1) {
      float lengthratio1, lengthratio2;
      lengthratio1 = eSentLength/fSentLength;
      lengthratio2 = fSentLength/eSentLength;
      features.add("lengthratio1="+lengthratio1);
      features.add("lengthratio2="+lengthratio2);       

      // src-->trg and trg-->src token translation ratio
      features.add("wordtransratio1=" + getWordTransRatio(fSrcTfs, eSrcTfs, fVocabSrc, eVocabTrg, f2e_Probs, prob) );
      features.add("wordtransratio2=" + getWordTransRatio(eSrcTfs, fSrcTfs, eVocabSrc, fVocabTrg, e2f_Probs, prob) );
      ////////////////////////////////////
      
      if (featSet > 2) {
        fSentence = fSentence.replaceAll("([',:;.?%!])", " $1 ");
        eSentence = eSentence.replaceAll("([',:;.?%!])", " $1 ");
        
        String[] fTokens = fSentence.split("\\s+");
        String[] eTokens = eSentence.split("\\s+");
        
        // future work = count number of single/double letter words in src and trg side
        float fSingleCnt = 1 + getNumberOfWordsWithNDigits(1, fTokens);
        int eSingleCnt = 1 + getNumberOfWordsWithNDigits(1, eTokens);
        float fDoubleCnt = 1 + getNumberOfWordsWithNDigits(2, fTokens);
        int eDoubleCnt = 1 + getNumberOfWordsWithNDigits(2, eTokens);
        features.add("Ndigitratio=" + (fSingleCnt/eSingleCnt)*(fDoubleCnt/eDoubleCnt));
        
        // future work = binary feature if there is same multi-digit number on both sides (e.g. 4-digit for years)
        getNumberRatio(fTokens, eTokens);
        if (featSet > 3) {
          // uppercase token matching features : find uppercased tokens that exactly appear on both sides
          // lack of this evidence does not imply anything, but its existence might indicate parallel
          PairOfFloats pair = getUppercaseRatio(fTokens, eTokens);
          features.add("uppercaseratio=" + Math.max(pair.getLeftElement(), pair.getRightElement()));
        }
      }
    }

    return features.toArray(new String[features.size()]);
  }

  private static PairOfFloats getNumberRatio(String[] tokens1, String[] tokens2) {
    HashSet<String> numberMap1 = getNumbers(tokens1);
    HashSet<String> numberMap2 = getNumbers(tokens2);
    return getRatio(numberMap1, numberMap2);
  }

  private static HashSet<String> getNumbers(String[] tokens) {
    HashSet<String> numbers = new HashSet<String>();
    for (String token : tokens) {
      if (isNumber.matcher(token).find()) {
        numbers.add(token);
      }
    }
    return numbers;
  }

  private static int getNumberOfWordsWithNDigits(int N, String[] tokens) {
    int cnt = 0;
    for (String token : tokens) {
      if (token.length() == N) {
        cnt++;
      }
    }
    return cnt;
  }

  private static float getWordTransRatio(HMapSIW eSrcTfs, HMapSIW fSrcTfs, Vocab eVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs e2fProbs, float probThreshold) {
    // if there are k occurences of a term w on source side, and m occurrences of a possible translation of w on target side, 
    // instead of saying that w has a translation on target side, we say w has max(1,m/k) translations to downweight cases where m<k
    float cntAll = 0, cntMatch = 0, cntAltAll = 0, cntAltMatch = 0;
    // trg-->src term translation ratio 
    for(String eTerm : eSrcTfs.keySet()){
      int e = eVocabSrc.get(eTerm);
      int srcCnt = eSrcTfs.get(eTerm);
      cntAll += srcCnt;      // consider OOVs as well since they are part of the sentence

      Matcher m = isNumber.matcher(eTerm);
      if(e < 0 || m.find()){   // only non-number terms since numbers might have noisy translation prob.s
        continue;
      }

      int[] fS = e2fProbs.get(e).getTranslations(probThreshold);

      int trgCnt = 0;
      for(int f : fS){
        String fTerm = fVocabTrg.get(f);
        if(fSrcTfs.containsKey(fTerm)){
          trgCnt += fSrcTfs.get(fTerm);
          cntAltMatch++;

          if (trgCnt >= srcCnt) break;
        }
      }
      cntMatch += (trgCnt >= srcCnt) ? srcCnt : trgCnt;
      cntAltAll++;
    }
    //when there are terms in fSent but none of them has a translation or vocab entry, set trans ratio to 0
    return cntAll == 0 ? 0 : cntMatch/cntAll;
  }

  private static PairOfFloats getUppercaseRatio(String[] tokens1, String[] tokens2) {
    //ěáčé
    // now, read tokens in first sentence and keep track of sequences of uppercased tokens in buffer
    HashSet<String> upperCaseMap1 = getDoubleUppercaseParts(tokens1);
    HashSet<String> upperCaseMap2 = getDoubleUppercaseParts(tokens2);
    return getRatio(upperCaseMap1, upperCaseMap2);
  }

  private static PairOfFloats getRatio(HashSet<String> upperCaseMap1, HashSet<String> upperCaseMap2) {
    float cntUpperMatch = 0;
    for (String uppercase : upperCaseMap1) {
      int length = uppercase.length(); 
      for (String uppercase2 : upperCaseMap2) {
        int length2 = uppercase2.length();
        if (length2 < length) {
          length = length2; 
        }
        if (upperCaseMap2.contains(uppercase)) {
          //        if (org.apache.commons.lang.StringUtils.getLevenshteinDistance(uppercase, uppercase2) < length * 0.25f) {
          cntUpperMatch++;          
          //          System.out.println("DEBUG["+uppercase+"]["+uppercase2+"]");
        }
      }
    }
    PairOfFloats pair = new PairOfFloats(
        upperCaseMap1.isEmpty() ? 0f : cntUpperMatch / upperCaseMap1.size(), 
            upperCaseMap2.isEmpty() ? 0f : cntUpperMatch / upperCaseMap2.size());
    return pair;
  }

  private static HashSet<String> getUppercaseParts(String[] tokens) {
    HashSet<String> set = new HashSet<String>();
    StringBuilder buffer = new StringBuilder(" ");

    int numUppercaseTokensInBuffer = 0;
    String uppercaseEntity = "";
    for (int i=0; i < tokens.length; i++) {
      String token = tokens[i].trim();
      //      System.out.println("DEBUG[Token=" + token);
      // discard single-char upper-case tokens
      if (token.length() > 0 && Character.isUpperCase(token.charAt(0))) {
        buffer.append(token + " ");
        numUppercaseTokensInBuffer++;
      }else {
        uppercaseEntity = buffer.toString().trim();
        //        System.out.println("DEBUG[Uppercase=" + uppercaseEntity);
        if (!uppercaseEntity.equals("") && uppercaseEntity.length() > 2 && numUppercaseTokensInBuffer > 1) {
          set.add(uppercaseEntity);
          buffer.delete(1, buffer.length());    // clear buffer
          numUppercaseTokensInBuffer = 0;
        }
      }
    } 
    return set;
  }

  private static HashSet<String> getDoubleUppercaseParts(String[] tokens) {
    HashSet<String> set = new HashSet<String>();
    StringBuilder buffer = new StringBuilder(" ");

    int numUppercaseTokensInBuffer = 0;
    String uppercaseEntity = "";
    for (int i=0; i < tokens.length; i++) {
      String token = tokens[i].trim();
      if (token.length() > 0 && Character.isUpperCase(token.charAt(0))) {
        buffer.append(token + " ");
        numUppercaseTokensInBuffer++;
        if (numUppercaseTokensInBuffer == 2) {
          uppercaseEntity = buffer.toString().trim();
          //        System.out.println("DEBUG[Uppercase=" + uppercaseEntity);
          if (!uppercaseEntity.equals("") && uppercaseEntity.length() > 2) {
            set.add(uppercaseEntity);
            buffer.delete(1, buffer.length());    // clear buffer
            buffer.append(token + " ");            
            numUppercaseTokensInBuffer = 1;
          }
        }
      }else {
        buffer.delete(1, buffer.length());    // clear buffer
        numUppercaseTokensInBuffer = 0;        
      }
    } 
    return set;
  }

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( CLIRUtils.class.getCanonicalName(), options );
  }

  private static final String ALIGNEROUT_OPTION = "aligner_out";
  private static final String TYPE_OPTION = "type";
  private static final String RAWSRCVOCAB_OPTION = "hooka_src_vocab";
  private static final String RAWTRGVOCAB_OPTION = "hooka_trg_vocab";
  private static final String RAWTTABLE_OPTION = "hooka_ttable";
  private static final String SRCVOCAB_OPTION = "src_vocab";
  private static final String TRGVOCAB_OPTION = "trg_vocab";
  private static final String TTABLE_OPTION = "ttable";
  private static final String F_OPTION = "f";
  private static final String E_OPTION = "e";
  private static final String NUMTRANS_OPTION = "k";
  private static final String CUMPROB_OPTION = "C";
  private static final String HDFS_OPTION = "hdfs";
  private static final String LIBJARS_OPTION = "libjars";
  private static Options options;

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("path to lexical translation table output by GIZA or BerkeleyAligner").withArgName("path").hasArg().create(ALIGNEROUT_OPTION));
    options.addOption(OptionBuilder.withDescription("type of the lexical translation table").withArgName("giza|berkeley").hasArg().create(TYPE_OPTION));
    options.addOption(OptionBuilder.withDescription("path to source-side vocab file output by Hooka word alignment").withArgName("path").hasArg().create(RAWSRCVOCAB_OPTION));
    options.addOption(OptionBuilder.withDescription("path to target-side vocab file output by Hooka word alignment").withArgName("path").hasArg().create(RAWTRGVOCAB_OPTION));
    options.addOption(OptionBuilder.withDescription("path to source-to-target translation table output by Hooka word alignment").withArgName("path").hasArg().create(RAWTTABLE_OPTION));
    options.addOption(OptionBuilder.withDescription("path to source-side vocab file of translation table").withArgName("path").hasArg().isRequired().create(SRCVOCAB_OPTION));
    options.addOption(OptionBuilder.withDescription("path to target-side vocab file of translation table").withArgName("path").hasArg().isRequired().create(TRGVOCAB_OPTION));
    options.addOption(OptionBuilder.withDescription("path to source-to-target translation table").withArgName("path").hasArg().isRequired().create(TTABLE_OPTION));
    options.addOption(OptionBuilder.withDescription("number of translations to keep per word").withArgName("positive integer").hasArg().create(NUMTRANS_OPTION));
    options.addOption(OptionBuilder.withDescription("cut-off point for cumulative probability").withArgName("0-1").hasArg().create(CUMPROB_OPTION));
    options.addOption(OptionBuilder.withDescription("source-language word").withArgName("word").hasArg().create(F_OPTION));
    options.addOption(OptionBuilder.withDescription("target-language word, or enter ALL for all translations").withArgName("word|ALL").hasArg().create(E_OPTION));
    options.addOption(OptionBuilder.withDescription("files are searched on HDFS if this option is set").create(HDFS_OPTION));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars").withArgName("jar packages").hasArg().create(LIBJARS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      printUsage();
      System.err.println("Error parsing command line: " + exp.getMessage());
      return;
    }

    String alignerLexFile = cmdline.getOptionValue(ALIGNEROUT_OPTION);
    String type = cmdline.getOptionValue(TYPE_OPTION);
    String hookaSrcVocab = cmdline.getOptionValue(RAWSRCVOCAB_OPTION);
    String hookaTrgVocab = cmdline.getOptionValue(RAWTRGVOCAB_OPTION);
    String hookaTTable = cmdline.getOptionValue(RAWTTABLE_OPTION);
    String srcVocabFile = cmdline.getOptionValue(SRCVOCAB_OPTION);
    String trgVocabFile = cmdline.getOptionValue(TRGVOCAB_OPTION);
    String ttableFile = cmdline.getOptionValue(TTABLE_OPTION);
    String srcWord = cmdline.getOptionValue(F_OPTION);
    String trgWord = cmdline.getOptionValue(E_OPTION);
    int numTrans = cmdline.hasOption(NUMTRANS_OPTION) ? Integer.parseInt(cmdline.getOptionValue(NUMTRANS_OPTION)) : Integer.MAX_VALUE;
    float cumProbThreshold = cmdline.hasOption(CUMPROB_OPTION) ? Float.parseFloat(cmdline.getOptionValue(CUMPROB_OPTION)) : 1;

    Configuration conf = new Configuration();
    FileSystem fs = cmdline.hasOption(HDFS_OPTION) ? FileSystem.get(conf) : FileSystem.getLocal(conf);

    if (alignerLexFile != null && type != null) {
      // conversion mode
      if (type.equals("giza")){
        createTTableFromGIZA(alignerLexFile, srcVocabFile, trgVocabFile, ttableFile, cumProbThreshold, numTrans, fs);
      } else if (type.equals("berkeley")) {
        createTTableFromBerkeleyAligner(alignerLexFile, srcVocabFile, trgVocabFile, ttableFile, cumProbThreshold, numTrans, fs);
      } else {
        System.err.println("Incorrect argument for type: " + type);
        printUsage();
        return;
      }
    } else if (hookaSrcVocab != null && hookaTrgVocab != null && hookaTTable != null) {
      // simplification mode
      createTTableFromHooka(hookaSrcVocab, hookaTrgVocab, hookaTTable, srcVocabFile, trgVocabFile, ttableFile, cumProbThreshold, numTrans, fs);
    } else if (srcWord != null && trgWord != null) {
      // query mode
      try {
        Vocab srcVocab = HadoopAlign.loadVocab(new Path(srcVocabFile), fs);
        Vocab trgVocab = HadoopAlign.loadVocab(new Path(trgVocabFile), fs);
        TTable_monolithic_IFAs src2trgProbs = new TTable_monolithic_IFAs(fs, new Path(ttableFile), true);
        System.out.println("Source vocab size: " + srcVocab.size());
        System.out.println("Target vocab size: " + trgVocab.size());
        int srcId = -1;
        try {
          srcId = srcVocab.get(srcWord);
        } catch (Exception e) {
          System.err.println(srcWord + " not found in source-side vocabulary " + srcVocabFile);
          System.exit(-1);
        }
        if (trgWord.equals("ALL")) {
          int[] trgs = src2trgProbs.get(srcId).getTranslations(0.0f);
          System.out.println("(" + srcId + "," + srcWord + ") has "+ trgs.length + " translations:");
          for (int i = 0; i < trgs.length; i++) {
            trgWord = trgVocab.get(trgs[i]);
            System.out.println("Prob("+trgWord+"|"+srcWord+")="+src2trgProbs.get(srcId, trgs[i]));
          }
        }else {
          int trgId = -1;
          try {
            trgId = trgVocab.get(trgWord);
          } catch (Exception e) {
            System.err.println(trgWord + " not found in target-side vocabulary " + trgVocabFile);
            System.exit(-1);  
          }
          System.out.println("Prob("+trgWord+"|"+srcWord+")="+src2trgProbs.get(srcId, trgId));
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    } else {
      System.err.println("Undefined option combination");
      printUsage();
      return;
    }

    return;
  }
}
