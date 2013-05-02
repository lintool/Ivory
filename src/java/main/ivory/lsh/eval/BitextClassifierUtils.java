package ivory.lsh.eval;

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
import edu.umd.cloud9.io.map.HMapIIW;
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
  static List<HMapSIW> fDocTfs = new ArrayList<HMapSIW>();
  static List<String> fSents = new ArrayList<String>();
  static List<HMapSIW> eDocs = new ArrayList<HMapSIW>();
  static List<HMapSIW> eDocTfs = new ArrayList<HMapSIW>();
  static List<String> eSents = new ArrayList<String>();

  static ArrayListOfInts enSentLengths = new ArrayListOfInts();
  static ArrayListOfInts deSentLengths = new ArrayListOfInts();

  static HMapSIW numSentencesPerDocE;
  static HMapSIW numSentencesPerDocF;

  static List<HMapSIW> gDocs = new ArrayList<HMapSIW>();
  static HMapSIW dfE = new HMapSIW();
  static HMapSIW dfD = new HMapSIW();
  static HMapSIW dfG = new HMapSIW();

  static HMapSIW fTitle2SentCnt = new HMapSIW();
  static HMapSIW eTitle2SentCnt = new HMapSIW();
  static HMapIIW parallelPairs = new HMapIIW();

  static float avgFDocLeng;
  static float avgEDocLeng;
  static float avgGDocLeng;
  static Vocab eVocabSrc, eVocabTrg;
  static Vocab fVocabSrc, fVocabTrg;
  static TTable_monolithic_IFAs f2e_Probs, e2f_Probs;
  private static Options options;

  private List<HMapSFW> translateDocVectors(String eLang, 
      String eTokenizerModelFile, String eStopwordsFile, List<HMapSIW> docs, float avgLen, HMapSIW transDfTable) {
    Bm25 mModel = new Bm25();
    // set number of docs
    mModel.setDocCount(docs.size());

    // set average doc length
    mModel.setAvgDocLength(avgLen);

    List<HMapSFW> transDocs = new ArrayList<HMapSFW>();
    Tokenizer tokenizer = TokenizerFactory.createTokenizer(eLang, 
        eTokenizerModelFile, true, eStopwordsFile, eStopwordsFile + ".stemmed", null);

    // translate doc texts here
    for (HMapSIW deDoc : docs) {
      HMapIFW tfS = new HMapIFW();
      int docLen = 0;
      try {
        docLen = CLIRUtils.translateTFs(deDoc, tfS, eVocabSrc, eVocabTrg, fVocabSrc,
            fVocabTrg, e2f_Probs, f2e_Probs, tokenizer , null);   // tokenizer just for stopword list
      } catch (IOException e) {
        e.printStackTrace();
      }
      HMapSFW v = CLIRUtils.createTermDocVector(docLen, tfS, eVocabTrg, mModel, dfE, true, null);
      // System.out.println("f"+(n++)+" : " + v);

      transDocs.add(v);
    }

    return transDocs;
  }

  // read from special wiki format created by Smith et al as part of their 2010 paper 
  private void readWikiSentences(String eReadFile, String fReadFile, String pairsFile, String eLang, String fLang,
      Vocab eVocab, Vocab fVocab, String fToken, String eToken, String fStopwordsFile, String eStopwordsFile) {
    Tokenizer eTokenizer = TokenizerFactory.createTokenizer(eLang, eToken, true, eStopwordsFile, eStopwordsFile + ".stemmed", null);
    Tokenizer fTokenizer = TokenizerFactory.createTokenizer(fLang, fToken, true, fStopwordsFile, fStopwordsFile + ".stemmed", null);

    try {
      BufferedReader dis1 = new BufferedReader(new InputStreamReader(new FileInputStream(new File(eReadFile)), "UTF-8"));
      BufferedReader dis2 = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fReadFile)), "UTF-8"));

      avgEDocLeng = readLines(dis1, eTokenizer, eTitle2SentCnt, enSentLengths, eDocTfs, eSents, dfE);
      avgFDocLeng = readLines(dis2, fTokenizer, fTitle2SentCnt, deSentLengths, fDocTfs, fSents, dfD);

      dis1 = new BufferedReader(new InputStreamReader(new FileInputStream(new File(pairsFile)), "UTF-8"));
      String line = null;
      while ((line = dis1.readLine()) != null) {
        String[] arr = line.split("\t");
        String fTitle = arr[0];
        String eTitle = arr[1];
        int fSentNo = Integer.parseInt(arr[2]); 
        int eSentNo = Integer.parseInt(arr[3]);
        parallelPairs.put(fTitle2SentCnt.get(fTitle) + fSentNo, eTitle2SentCnt.get(eTitle) + eSentNo);
      }
    }catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private float readLines(BufferedReader reader, Tokenizer tokenizer, HMapSIW title2SentCnt, ArrayListOfInts sentLengths,
      List<HMapSIW> sentTfs, List<String> sents, HMapSIW dfTable) throws IOException {
    String line = null;
    boolean isNewDoc = true;
    int cnt = 0;
    float sumLengths = 0;
    HMapSIW sent = new HMapSIW();

    while ((line = reader.readLine()) != null) {
      line = line.trim();

      if (isNewDoc) {
        title2SentCnt.put(line, cnt);
        isNewDoc = false;
      } else if (line.equals("")){
        isNewDoc = true;       
      }else {
        String[] tokens = tokenizer.processContent(line);
        sentLengths.add(tokens.length);
        sumLengths += tokens.length;

        for (String token : tokens) {
          if (!sent.containsKey(token)) {
            dfTable.increment(token);
          }
          sent.increment(token);
        }
        sentTfs.add(sent);
        sents.add(line);
        cnt++;
        sent.clear();
      }
    } 
    reader.close();

    return (sumLengths / cnt);    
  }

  // regular 1 sentence per line, 1 sentence per doc format
  private void readSentences(int sentsPerDoc, String eReadFile, String fReadFile, String eLang, String fLang,
      String fToken, String eToken, String fStopwordsFile, String eStopwordsFile) throws IOException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    Tokenizer eTokenizer = TokenizerFactory.createTokenizer(eLang, eToken, true, eStopwordsFile, eStopwordsFile + ".stemmed", null);
    Tokenizer fTokenizer = TokenizerFactory.createTokenizer(fLang, fToken, true, fStopwordsFile, fStopwordsFile + ".stemmed", null);

    float sumFLengs = 0, sumELengs = 0;

    try {
      BufferedReader dis1 = new BufferedReader(new InputStreamReader(new FileInputStream(new File(eReadFile)), "UTF-8"));
      BufferedReader dis2 = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fReadFile)), "UTF-8"));
      HMapSIW fDoc = new HMapSIW();
      HMapSIW eDoc = new HMapSIW();
      String eLine = null, fLine = null;
      int cntEDocs = 0, cntFDocs = 0, lastDocLenE = 0, lastDocLenF = 0, numSents = 0;

      while ((eLine = dis1.readLine()) != null) {
        fLine = dis2.readLine().trim();
        eLine = eLine.trim();

        String[] tokens = fTokenizer.processContent(fLine);      
        lastDocLenF += tokens.length;

        for (String token : tokens) {
          if (!fDoc.containsKey(token)) { // if this is first time we saw token in this sentence
            dfD.increment(token);
          }
          fDoc.increment(token);
        }

        tokens = eTokenizer.processContent(eLine);
        lastDocLenE += tokens.length;

        for (String token : tokens) {
          if (!eDoc.containsKey(token)) {
            dfE.increment(token);
          }
          eDoc.increment(token);
        }
        
        numSents++;
        
        if (numSents == sentsPerDoc) {
          sumFLengs += lastDocLenF;
          sumELengs += lastDocLenE;

          enSentLengths.add(lastDocLenE);
          deSentLengths.add(lastDocLenF);

          eDocTfs.add(eDoc);
          fDocTfs.add(fDoc);
          cntEDocs++;
          cntFDocs++;
          
          // reset variables 
          fDoc = new HMapSIW();
          eDoc = new HMapSIW();
          numSents = 0;
          lastDocLenE = 0;
          lastDocLenF = 0;
        }
        eSents.add(eLine);
        fSents.add(fLine);

      }

      // dispose all the resources after using them.
      dis1.close();
      dis2.close();

      avgFDocLeng = sumFLengs / cntFDocs;
      avgEDocLeng = sumELengs / cntEDocs;
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

    // tf-idf computation
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

  private void prepareTrainTestData(List<String> fSents, List<String> eSents, 
      Tokenizer fTokenizer, Tokenizer eTokenizer,
      List<HMapSIW> fTfs, List<HMapSIW> eTfs, HMapIIW parallelPairs, List<HMapSFW> transVectors,
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
        if (parallelPairs.get(i) == j) {
          label = "parallel";
        } else {
          label = "non_parallel";
        }
        String[] featVector = null;
        if (featureSet == 1) {
          featVector = CLIRUtils.computeFeaturesF1(eVector, transVector, 
              enSentLengths.get(j), deSentLengths.get(i));
        } else if (featureSet == 2) {
          featVector = CLIRUtils.computeFeaturesF2(eTfMap, eVector, fTfMap, transVector, 
              enSentLengths.get(j), deSentLengths.get(i), 
              eVocabSrc, eVocabTrg, fVocabSrc, fVocabTrg, e2f_Probs, f2e_Probs, prob);
        } else if (featureSet == 3) {
          featVector = CLIRUtils.computeFeaturesF3(fSent, eSent, fTokenizer, eTokenizer,
              eTfMap, eVector, fTfMap, transVector, enSentLengths.get(j), deSentLengths.get(i), 
              eVocabSrc, eVocabTrg, fVocabSrc, fVocabTrg, e2f_Probs, f2e_Probs, prob);
        }

        if (featVector != null) {
          String s = concat(featVector);
          if (label.equals("non_parallel") && s.contains("uppercaseratio=1")) {
            System.out.println("DEBUG:"+fSent+"\nDEBUG:"+eSent);            
          }
          System.out.println(s + " " + label);
        }
        cnt++;
      }
    }
    System.out.println("Computed " + cnt + " F" + featureSet + " instances in " + (System.currentTimeMillis() - time));
  }

  /**
   * @param fFile
   *            source language text in 'one sentence per line' format
   * @param featureSet
   *            integer value indicating which set of features to generate in
   *            the test/train data
   * @param alignmentFileName
   *            optional. if word-alignments are available for corpus, they
   *            can be used to generate additional features
   */
  public void runPrepareSentenceExtractionData(String fLang, String eLang, String fFile,
      String eFile, String pairsFile, String fStopwordsFile, String eStopwordsFile, String fVocabSrcFile, 
      String eVocabTrgFile, String eVocabSrcFile,
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
      Tokenizer fTokenizer = TokenizerFactory.createTokenizer(localFs, fLang, fTokenFile, false);
      Tokenizer eTokenizer = TokenizerFactory.createTokenizer(localFs, eLang, eTokenFile, false);
      long startTime = System.currentTimeMillis(); 

      if (pairsFile == null) {
        readSentences(1, eFile, fFile, eLang, fLang,
            fTokenFile, eTokenFile, fStopwordsFile, eStopwordsFile);
        for (int i = 0; i < fDocTfs.size(); i++) {
          parallelPairs.put(i, i);
        }
      } else {
        readWikiSentences(eFile, fFile, pairsFile, eLang, fLang, eVocabTrg, fVocabSrc, 
            fTokenFile, eTokenFile, fStopwordsFile, eStopwordsFile);        
      }

      long sentTime = System.currentTimeMillis();
      System.out.println("Sentences read in " + (sentTime - startTime) + 
          " ms. Number of sentences: " + fDocTfs.size() + " = " + eDocTfs.size());

      List<HMapSFW> eSentVectors = buildDocVectors(eDocTfs, avgEDocLeng, dfE);

      long evectTime = System.currentTimeMillis();
      System.out.println("E vectors created in " + (evectTime - sentTime) + " ms");

      List<HMapSFW> fSentVectors = translateDocVectors(eLang, eTokenFile, eStopwordsFile, fDocTfs, avgFDocLeng, dfE);

      long fvectTime = System.currentTimeMillis();
      System.out.println("F vectors created in " + (fvectTime - evectTime) + 
          " ms. Number of vectors: " + fSentVectors.size() + " = " + eSentVectors.size());

      prepareTrainTestData(fSents, eSents, fTokenizer, eTokenizer, fDocTfs, eDocTfs, parallelPairs,  
          fSentVectors, eSentVectors, featureSet, prob, alignments);

      long endTime = System.currentTimeMillis();
      System.out.println("Features computed in " + (endTime - fvectTime) + " ms");

    } catch (Exception e) {
      System.err.println(eFile);
      System.err.println(fFile);
      System.err.println(pairsFile);
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
    
    long startTime = System.currentTimeMillis();

//    runCLIRComparison();
    BitextClassifierUtils dt = new BitextClassifierUtils();
    dt.runPrepareSentenceExtractionData( 
        cmdline.getOptionValue(FLANG_OPTION), 
        cmdline.getOptionValue(ELANG_OPTION),
        cmdline.getOptionValue(FBITEXT_OPTION), 
        cmdline.getOptionValue(EBITEXT_OPTION), 
        cmdline.getOptionValue(PAIRSFILE_OPTION), 
        cmdline.getOptionValue(FSTOP_OPTION),
        cmdline.getOptionValue(ESTOP_OPTION), 
        cmdline.getOptionValue(FSRC_OPTION),
        cmdline.getOptionValue(ETRG_OPTION), 
        cmdline.getOptionValue(ESRC_OPTION), 
        cmdline.getOptionValue(FTRG_OPTION),
        cmdline.getOptionValue(F2E_OPTION), 
        cmdline.getOptionValue(E2F_OPTION), 
        cmdline.getOptionValue(FTOK_OPTION),
        cmdline.getOptionValue(ETOK_OPTION), 
        Integer.parseInt(cmdline.getOptionValue(FEAT_OPTION)), 
        (cmdline.hasOption(PROB_OPTION) ? Float.parseFloat(cmdline.getOptionValue(PROB_OPTION)) : 0), null);

    System.out.println("Done in " + (System.currentTimeMillis() - startTime) + " ms");

  }

  private static void runCLIRComparison() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    String VOCABDIR = "/fs/clip-qa/ferhan/end2end-experiments/Ivory/data/vocab";    // /Users/ferhanture/Documents/workspace/ivory-github/Ivory/data/vocab
    String TOKENDIR = "/fs/clip-qa/ferhan/end2end-experiments/Ivory/data/tokenizer";   // "/Users/ferhanture/Documents/workspace/ivory-github/Ivory/data/tokenizer
    String DATADIR = "/fs/clip-qa/ferhan/cl-pwsim/pwsim-experiments-2013";    // /Users/ferhanture/edu/research_archive/data/de-en/eu-nc-wmt08
    
    BitextClassifierUtils dt = new BitextClassifierUtils();
    numSentencesPerDocE = new HMapSIW();
    numSentencesPerDocF = new HMapSIW();
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    eVocabSrc = HadoopAlign.loadVocab(new Path(VOCABDIR+"/vocab.en-de.en"), localFs);
    eVocabTrg = HadoopAlign.loadVocab(new Path(VOCABDIR+"/vocab.de-en.en"), localFs);
    fVocabSrc = HadoopAlign.loadVocab(new Path(VOCABDIR+"/vocab.de-en.de"), localFs);
    fVocabTrg = HadoopAlign.loadVocab(new Path(VOCABDIR+"/vocab.en-de.de"), localFs);
    f2e_Probs = new TTable_monolithic_IFAs(localFs, new Path(VOCABDIR+"/ttable.de-en"), true);
    e2f_Probs = new TTable_monolithic_IFAs(localFs, new Path(VOCABDIR+"/ttable.en-de"), true);
    dt.readSentences(10, 
        DATADIR+"/europarl-v6.sample.de", 
        DATADIR+"/europarl-v6.sample.de", "en", "de", 
        TOKENDIR+"/de-token.bin", 
        TOKENDIR+"/en-token.bin", 
        TOKENDIR+"/de.stop", 
        TOKENDIR+"/en.stop");
    List<HMapSFW> fDocVectors = dt.translateDocVectors("en", 
        TOKENDIR+"/en-token.bin", 
        TOKENDIR+"/en.stop", 
        fDocTfs, avgFDocLeng, dfE);
    eDocTfs.clear();
    dfE.clear();
    
    dt.readSentences(1, 
        DATADIR+"/europarl-v6.sample.googletrans.en", 
        DATADIR+"/europarl-v6.sample.googletrans.en", "en", "de", 
        TOKENDIR+"/de-token.bin", 
        TOKENDIR+"/en-token.bin", 
        TOKENDIR+"/de.stop", 
        TOKENDIR+"/en.stop");
    List<HMapSFW> googletransDocVectors = dt.buildDocVectors(eDocTfs, avgEDocLeng, dfE);
    eDocTfs.clear();
    dfE.clear();
    
    dt.readSentences(10, 
        DATADIR+"/europarl-v6.sample.cdectrans.en", 
        DATADIR+"/europarl-v6.sample.cdectrans.en", "en", "de", 
        TOKENDIR+"/de-token.bin", 
        TOKENDIR+"/en-token.bin", 
        TOKENDIR+"/de.stop", 
        TOKENDIR+"/en.stop");
    List<HMapSFW> cdectransDocVectors = dt.buildDocVectors(eDocTfs, avgEDocLeng, dfE);
    eDocTfs.clear();
    dfE.clear();
    
    dt.readSentences(10, 
        DATADIR+"/europarl-v6.sample.en", 
        DATADIR+"/europarl-v6.sample.en", "en", "de", 
        TOKENDIR+"/de-token.bin", 
        TOKENDIR+"/en-token.bin", 
        TOKENDIR+"/de.stop", 
        TOKENDIR+"/en.stop");
    List<HMapSFW> eDocVectors = dt.buildDocVectors(eDocTfs, avgEDocLeng, dfE);
    for (int i=0; i<100; i++) {
//      System.out.println(CLIRUtils.cosine(fDocVectors.get(i), eDocVectors.get(i)));
      System.out.println("cdec\t+\t" + CLIRUtils.cosine(cdectransDocVectors.get(i), eDocVectors.get(i)));
      System.out.println("google\t+\t" + CLIRUtils.cosine(googletransDocVectors.get(i), eDocVectors.get(i)));
      System.out.println("clir\t+\t" + CLIRUtils.cosine(fDocVectors.get(i), eDocVectors.get(i)));
      int rand = (int) (Math.random()*100);
      while (rand == i) {
        rand = (int) (Math.random()*100);
      }
      System.out.println("cdec\t-\t" + CLIRUtils.cosine(cdectransDocVectors.get(i), eDocVectors.get(rand)));
      System.out.println("google\t-\t" + CLIRUtils.cosine(googletransDocVectors.get(i), eDocVectors.get(rand)));
      System.out.println("clir\t-\t" + CLIRUtils.cosine(fDocVectors.get(i), eDocVectors.get(rand)));
    }    
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
  private static final String ESTOP_OPTION = "e_stopwords";
  private static final String FSTOP_OPTION = "f_stopwords";
  private static final String FEAT_OPTION = "feature";
  private static final String PAIRSFILE_OPTION = "pairs";
  private static final String PROB_OPTION = "prob";
  private static final String LIBJARS_OPTION = "libjars";

  @SuppressWarnings("static-access")
  private static CommandLine parseArgs(String[] args) {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("two-letter code for f-language")
        .withArgName("en|de|tr|cs|zh|ar|es").hasArg().create(FLANG_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter code for e-language")
        .withArgName("en|de|tr|cs|zh|ar|es").hasArg().create(ELANG_OPTION));
    options.addOption(OptionBuilder.withDescription("source-side of training bitext")
        .withArgName("path").hasArg().create(FBITEXT_OPTION));
    options.addOption(OptionBuilder.withDescription("target-side of training bitext")
        .withArgName("path").hasArg().create(EBITEXT_OPTION));
    options.addOption(OptionBuilder.withDescription("source vocabulary (f-side) of P(e|f)")
        .withArgName("path to Vocab object").hasArg().create(FSRC_OPTION));
    options.addOption(OptionBuilder.withDescription("source vocabulary (e-side) of P(f|e)")
        .withArgName("path to Vocab object").hasArg().create(ESRC_OPTION));
    options.addOption(OptionBuilder.withDescription("target vocabulary (f-side) of P(f|e)")
        .withArgName("path to Vocab object").hasArg().create(FTRG_OPTION));
    options.addOption(OptionBuilder.withDescription("target vocabulary (e-side) of P(e|f)")
        .withArgName("path to Vocab object").hasArg().create(ETRG_OPTION));
    options.addOption(OptionBuilder.withDescription("translation table P(e|f)")
        .withArgName("path to TTable object").hasArg().create(F2E_OPTION));
    options.addOption(OptionBuilder.withDescription("translation table P(f|e)")
        .withArgName("path to TTable object").hasArg().create(E2F_OPTION));
    options.addOption(OptionBuilder.withDescription("tokenizer model for f-language")
        .withArgName("path to Tokenizer object").hasArg().create(FTOK_OPTION));
    options.addOption(OptionBuilder.withDescription("tokenizer model for e-language")
        .withArgName("path to Tokenizer object").hasArg().create(ETOK_OPTION));
    options.addOption(OptionBuilder.withDescription("stopwords for f-language")
        .withArgName("path to stopword list").hasArg().create(FSTOP_OPTION));
    options.addOption(OptionBuilder.withDescription("stopwords for e-language")
        .withArgName("path to stopword list").hasArg().create(ESTOP_OPTION));
    options.addOption(OptionBuilder.withDescription("id of feature set to be used")
        .withArgName("1|2|3").hasArg().create(FEAT_OPTION));
    options.addOption(OptionBuilder.withDescription("lower threshold for token translation probability")
        .withArgName("0-1").hasArg().create(PROB_OPTION));
    options.addOption(OptionBuilder.withDescription("parallel sentence id pairs (for Wikipedia format)")
        .withArgName("path").hasArg().create(PAIRSFILE_OPTION));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars")
        .withArgName("jar packages").hasArg().create(LIBJARS_OPTION));

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
