package ivory.lsh.eval;

import ivory.core.tokenize.OpenNLPTokenizer;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.core.util.CLIRUtils;
import ivory.pwsim.score.Bm25;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.util.array.ArrayListOfInts;
import edu.umd.cloud9.util.map.MapKI.Entry;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

/**
 * Train and test a bitext classifier.
 * 
 * @author ferhanture
 * 
 */
public class BitextClassifierUtils {
  static List<HMapSIW> fDocs = new ArrayList<HMapSIW>();
  static List<HMapSIW> fSentTfs = new ArrayList<HMapSIW>();
  static List<String> fSents = new ArrayList<String>();
  static List<HMapSIW> eDocs = new ArrayList<HMapSIW>();
  static List<HMapSIW> eSentTfs = new ArrayList<HMapSIW>();
  static List<String> eSents = new ArrayList<String>();

  static ArrayListOfInts enSentLengths = new ArrayListOfInts();
  static ArrayListOfInts deSentLengths = new ArrayListOfInts();

  static HMapSIW numSentencesPerDocE;
  static HMapSIW numSentencesPerDocF;

  static List<HMapSIW> gDocs = new ArrayList<HMapSIW>();
  static HMapSIW dfE = new HMapSIW();
  static HMapSIW dfD = new HMapSIW();
  static HMapSIW dfG = new HMapSIW();

  float avgDeDocLeng;
  static float avgEnDocLeng;
  static float avgGDocLeng;
  static Vocab eVocabSrc, eVocabTrg;
  static Vocab fVocabSrc, fVocabTrg;
  static TTable_monolithic_IFAs f2e_Probs, e2f_Probs;
  private static Options options;

  private List<HMapSFW> translateDocVectors(List<HMapSIW> docs, HMapSIW transDfTable) {
    Bm25 mModel = new Bm25();
    // set number of docs
    mModel.setDocCount(docs.size());

    // set average doc length
    mModel.setAvgDocLength(avgDeDocLeng);

    List<HMapSFW> transDocs = new ArrayList<HMapSFW>();

    // translate doc texts here
    for (HMapSIW deDoc : docs) {
      HMapIFW tfS = new HMapIFW();
      int docLen = 0;
      try {
        docLen = CLIRUtils.translateTFs(deDoc, tfS, eVocabSrc, eVocabTrg, fVocabSrc,
            fVocabTrg, e2f_Probs, f2e_Probs, new OpenNLPTokenizer(), null);   // tokenizer just for stopword list
      } catch (IOException e) {
        e.printStackTrace();
      }
      HMapSFW v = CLIRUtils.createTermDocVector(docLen, tfS, eVocabTrg, mModel, dfE, true, null);
      // System.out.println("f"+(n++)+" : " + v);

      transDocs.add(v);
    }

    return transDocs;
  }

  // regular 1 sentence per line format
  private void readSentences(String eReadFile, String fReadFile, String eLang, String fLang,
      Vocab eVocab, Vocab fVocab, String fToken, String eToken) throws IOException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    File eFile = new File(eReadFile);
    File fFile = new File(fReadFile);

    FileInputStream fis1 = null, fis2 = null;
    BufferedReader dis1 = null, dis2 = null;

    Tokenizer eTokenizer = TokenizerFactory.createTokenizer(eLang, eToken, false, null, null, null);
    Tokenizer fTokenizer = TokenizerFactory.createTokenizer(fLang, fToken, false, null, null, null);

    float sumFLengs = 0, sumELengs = 0;

    try {
      fis1 = new FileInputStream(eFile);
      fis2 = new FileInputStream(fFile);
      dis1 = new BufferedReader(new InputStreamReader(fis1, "UTF-8"));
      dis2 = new BufferedReader(new InputStreamReader(fis2, "UTF-8"));
      HMapSIW fSent = new HMapSIW();
      HMapSIW eSent = new HMapSIW();
      String eLine = null, fLine = null;
      int cntE = 0, cntF = 0, lastSentLenE = 0, lastSentLenF = 0;

      while ((eLine = dis1.readLine()) != null) {
        fLine = dis2.readLine().trim();
        eLine = eLine.trim();

        String[] tokens;
        if (fTokenizer == null) {
          tokens = fLine.split(" ");
        } else {
          tokens = fTokenizer.processContent(fLine);
        }
        lastSentLenF = tokens.length;

        for (String token : tokens) {
          if (!fSent.containsKey(token)) { // if this is first time we saw token in this sentence
            dfD.increment(token);
          }
          fSent.increment(token);

        }

        tokens = eTokenizer.processContent(eLine);
        lastSentLenE = tokens.length;

        for (String token : tokens) {
          if (!eSent.containsKey(token)) {
            dfE.increment(token);
          }
          eSent.increment(token);
        }

        sumFLengs += lastSentLenF;
        sumELengs += lastSentLenE;

        enSentLengths.add(lastSentLenE);
        deSentLengths.add(lastSentLenF);

        eSentTfs.add(eSent);
        fSentTfs.add(fSent);

        eSents.add(eLine);
        fSents.add(fLine);

        cntE++;
        cntF++;
        fSent = new HMapSIW();
        eSent = new HMapSIW();
      }

      // dispose all the resources after using them.
      fis1.close();
      dis1.close();
      fis2.close();
      dis2.close();

      avgDeDocLeng = sumFLengs / cntF;
      avgEnDocLeng = sumELengs / cntE;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private List<HMapSFW> buildDocVectors(List<HMapSIW> term2tfVectors, float avgLen,
      HMapSIW dfTable) {
    Bm25 mModel = new Bm25();
    // set number of docs
    mModel.setDocCount(term2tfVectors.size());

    // set average doc length
    mModel.setAvgDocLength(avgLen);

    List<HMapSFW> docVectors = new ArrayList<HMapSFW>();
    for (HMapSIW enDoc : term2tfVectors) {
      HMapSFW v = new HMapSFW();
      int docLen = 0;
      for (Entry<String> item : enDoc.entrySet()) {
        int tf = item.getValue();
        docLen += tf;
      }
      float sum2 = 0;
      for (Entry<String> item : enDoc.entrySet()) {
        String term = item.getKey();
        int tf = item.getValue();
        int df = dfTable.get(term);
        mModel.setDF(df);
        float score = mModel.computeDocumentWeight(tf, docLen);
        if (score > 0) {
          v.put(term, score);
          sum2 += score * score;
        }
      }
      // normalize
      sum2 = (float) Math.sqrt(sum2);
      for (edu.umd.cloud9.util.map.MapKF.Entry<String> e : v.entrySet()) {
        float score = v.get(e.getKey());
        v.put(e.getKey(), score / sum2);
      }

      docVectors.add(v);
    }

    return docVectors;
  }

  private List<String> readAlignments(String alignmentFileName) {
    List<String> alignments = new ArrayList<String>();
    try {
      BufferedReader dis = new BufferedReader(new InputStreamReader(new FileInputStream(
          alignmentFileName), "UTF-8"));
      String line;
      while ((line = dis.readLine()) != null) {
        alignments.add(line);
      }
      dis.close();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return alignments;
  }

  private void prepareTrainTestData(List<String> fSents, List<String> eSents, List<HMapSIW> fTfs, List<HMapSIW> eTfs, List<HMapSFW> transVectors,
      List<HMapSFW> eVectors, int featureSet, float prob, List<String> alignments) {
    NumberFormat nf = NumberFormat.getNumberInstance();
    nf.setGroupingUsed(false);
    nf.setMaximumFractionDigits(2);
    int cnt = 0;
    String label;
    long time = System.currentTimeMillis();

    for (int i = 0; i < transVectors.size(); i++) {
      HMapSFW transVector = transVectors.get(i);
      HMapSIW fTfMap = fTfs.get(i);
      String fSent = fSents.get(i);
      for (int j = 0; j < eVectors.size(); j++) {
        HMapSFW eVector = eVectors.get(j);
        HMapSIW eTfMap = eTfs.get(j);
        String eSent = eSents.get(j);
        if (i == j) {
          label = "parallel";
        } else {
          label = "non_parallel";
        }

        String[] featVector = null;
        if (featureSet == 1) {
          featVector = CLIRUtils.computeFeaturesF1(eVector, transVector, enSentLengths.get(j),
              deSentLengths.get(i));
        } else if (featureSet == 2) {
          featVector = CLIRUtils.computeFeaturesF2(eTfMap, eVector, fTfMap, transVector, enSentLengths
              .get(j), deSentLengths.get(i), eVocabSrc, eVocabTrg, fVocabSrc,
              fVocabTrg, e2f_Probs, f2e_Probs, prob);
        } else if (featureSet == 3) {
          featVector = CLIRUtils.computeFeaturesF3(eSent, eTfMap, eVector, fSent, fTfMap, transVector, enSentLengths
              .get(j), deSentLengths.get(i), eVocabSrc, eVocabTrg, fVocabSrc,
              fVocabTrg, e2f_Probs, f2e_Probs, prob);
        }

        if (featVector != null) {
          String s = concat(featVector);
          System.out.println(s + " " + label);
        }
        cnt++;
      }
    }
    System.out.println("Computed " + cnt + " F" + featureSet + " instances in " + (System.currentTimeMillis() - time));
  }

  /**
   * @param fName
   *            source language text in 'one sentence per line' format
   * @param featureSet
   *            integer value indicating which set of features to generate in
   *            the test/train data
   * @param alignmentFileName
   *            optional. if word-alignments are available for corpus, they
   *            can be used to generate additional features
   */
  public void runPrepareSentenceExtractionData(String fLang, String eLang, String fName,
      String eName, String fVocabSrcFile, String eVocabTrgFile, String eVocabSrcFile,
      String fVocabTrgFile, String probTablef2eFile, String probTablee2fFile,
      String fTokenFile, String eTokenFile, int featureSet, float prob, String alignmentFileName) {
    FileSystem localFs = null;
    try {
      localFs = FileSystem.getLocal(new Configuration());
      List<String> alignments = null;
      if (alignmentFileName != null) {
        alignments = readAlignments(alignmentFileName);
      }
      eVocabSrc = HadoopAlign.loadVocab(new Path(eVocabSrcFile), localFs);
      eVocabTrg = HadoopAlign.loadVocab(new Path(eVocabTrgFile), localFs);
      fVocabSrc = HadoopAlign.loadVocab(new Path(fVocabSrcFile), localFs);
      fVocabTrg = HadoopAlign.loadVocab(new Path(fVocabTrgFile), localFs);
      f2e_Probs = new TTable_monolithic_IFAs(localFs, new Path(probTablef2eFile), true);
      e2f_Probs = new TTable_monolithic_IFAs(localFs, new Path(probTablee2fFile), true);

      readSentences(eName, fName, eLang, fLang, eVocabTrg, fVocabSrc, fTokenFile, eTokenFile);

      System.out.println(fSentTfs.size()+"="+eSentTfs.size());

      List<HMapSFW> eSentVectors = buildDocVectors(eSentTfs, avgEnDocLeng, dfE);
      List<HMapSFW> fSentVectors = translateDocVectors(fSentTfs, dfE);

      System.out.println(fSentVectors.size()+"="+eSentVectors.size());

      prepareTrainTestData(fSents, eSents, fSentTfs, eSentTfs, fSentVectors, eSentVectors, featureSet, prob, alignments);
    } catch (Exception e) {
      System.err.println(eVocabSrcFile);
      System.err.println(eVocabTrgFile);
      System.err.println(fVocabSrcFile);
      System.err.println(fVocabTrgFile);
      System.err.println(probTablef2eFile);
      System.err.println(probTablee2fFile);
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmdline = parseArgs(args);
    if ( cmdline == null ) {
      printUsage();
      return;
    }
    BitextClassifierUtils dt = new BitextClassifierUtils();
    numSentencesPerDocE = new HMapSIW();
    numSentencesPerDocF = new HMapSIW();
    dt.runPrepareSentenceExtractionData(cmdline.getOptionValue(FLANG_OPTION), cmdline.getOptionValue(ELANG_OPTION),
        cmdline.getOptionValue(FBITEXT_OPTION), cmdline.getOptionValue(EBITEXT_OPTION), cmdline.getOptionValue(FSRC_OPTION),
        cmdline.getOptionValue(ETRG_OPTION), cmdline.getOptionValue(ESRC_OPTION), cmdline.getOptionValue(FTRG_OPTION),
        cmdline.getOptionValue(F2E_OPTION), cmdline.getOptionValue(E2F_OPTION), cmdline.getOptionValue(FTOK_OPTION),
        cmdline.getOptionValue(ETOK_OPTION), Integer.parseInt(cmdline.getOptionValue(FEAT_OPTION)), (cmdline.hasOption(PROB_OPTION) ? Float.parseFloat(cmdline.getOptionValue(PROB_OPTION)) : 0), null);
    System.out.println("Done.");

  }

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "BitextClassifierUtils", options );
    System.exit(-1);    
  }

  // output a cleaned, stemmed version of the sentence
  public String concat(String[] tokens) {
    String line = "";
    for (String token : tokens) {
      line += token + " ";
    }
    return line;
  }

  private static final String FLANG_OPTION = "f_lang";
  private static final String ELANG_OPTION = "e_lang";
  private static final String FBITEXT_OPTION = "f_bitext";
  private static final String EBITEXT_OPTION = "e_bitext";
  private static final String FSRC_OPTION = "f_srcvocab";
  private static final String ESRC_OPTION = "e_srcvocab";
  private static final String FTRG_OPTION = "f_trgvocab";
  private static final String ETRG_OPTION = "e_trgvocab";
  private static final String F2E_OPTION = "f2e_ttable";
  private static final String E2F_OPTION = "e2f_ttable";
  private static final String ETOK_OPTION = "e_tokenizer";
  private static final String FTOK_OPTION = "f_tokenizer";
  private static final String FEAT_OPTION = "feature";
  private static final String PROB_OPTION = "prob";
  private static final String LIBJARS_OPTION = "libjars";

  @SuppressWarnings("static-access")
  private static CommandLine parseArgs(String[] args) {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("two-letter code for f-language").withArgName("en|de|tr|cs|zh|ar|es").hasArg().isRequired().create(FLANG_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter code for e-language").withArgName("en|de|tr|cs|zh|ar|es").hasArg().isRequired().create(ELANG_OPTION));
    options.addOption(OptionBuilder.withDescription("source-side of training bitext").withArgName("path").hasArg().isRequired().create(FBITEXT_OPTION));
    options.addOption(OptionBuilder.withDescription("target-side of training bitext").withArgName("path").hasArg().isRequired().create(EBITEXT_OPTION));
    options.addOption(OptionBuilder.withDescription("source vocabulary (f-side) of P(e|f)").withArgName("path to Vocab object").hasArg().isRequired().create(FSRC_OPTION));
    options.addOption(OptionBuilder.withDescription("source vocabulary (e-side) of P(f|e)").withArgName("path to Vocab object").hasArg().isRequired().create(ESRC_OPTION));
    options.addOption(OptionBuilder.withDescription("target vocabulary (f-side) of P(f|e)").withArgName("path to Vocab object").hasArg().isRequired().create(FTRG_OPTION));
    options.addOption(OptionBuilder.withDescription("target vocabulary (e-side) of P(e|f)").withArgName("path to Vocab object").hasArg().isRequired().create(ETRG_OPTION));
    options.addOption(OptionBuilder.withDescription("translation table P(e|f)").withArgName("path to TTable object").hasArg().isRequired().create(F2E_OPTION));
    options.addOption(OptionBuilder.withDescription("translation table P(f|e)").withArgName("path to TTable object").hasArg().isRequired().create(E2F_OPTION));
    options.addOption(OptionBuilder.withDescription("tokenizer model for f-language").withArgName("path to Tokenizer object").hasArg().isRequired().create(FTOK_OPTION));
    options.addOption(OptionBuilder.withDescription("tokenizer model for e-language").withArgName("path to Tokenizer object").hasArg().isRequired().create(ETOK_OPTION));
    options.addOption(OptionBuilder.withDescription("id of feature set to be used").withArgName("1|2|3").hasArg().isRequired().create(FEAT_OPTION));
    options.addOption(OptionBuilder.withDescription("lower threshold for token translation probability").withArgName("0-1").hasArg().create(PROB_OPTION));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars").withArgName("jar packages").hasArg().create(LIBJARS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return null;
    }
    return cmdline;
  }
}
