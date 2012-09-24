package ivory.lsh.bitext;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.dictionary.DefaultFrequencySortedDictionary;
import ivory.core.data.stat.DfTableArray;
import ivory.core.data.stat.PrefixEncodedGlobalStats;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.core.util.CLIRUtils;
import ivory.pwsim.score.Bm25;
import ivory.pwsim.score.ScoringModel;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import opennlp.model.MaxentModel;
import ivory.lsh.bitext.MoreGenericModelReader;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.util.map.MapKI.Entry;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

@SuppressWarnings("deprecation")
public class PreprocessHelper {
  private String eLang, fLang, eDir;
  private int MinVectorTerms, MinSentenceLength;
  private SentenceDetectorME fModel, eModel;
  private Tokenizer fTok, eTok;
  private VocabularyWritable eVocabSrc, eVocabTrg, fVocabTrg, fVocabSrc;
  private TTable_monolithic_IFAs f2e_Probs;
  private TTable_monolithic_IFAs e2f_Probs;
  private ScoringModel fScoreFn, eScoreFn;
  //  private HMapIFW transDfTable;
  private MaxentModel classifier;
  //  private PrefixEncodedGlobalStats globalStatsMap;  // for backward compatibility
  private DfTableArray dfTable;
  private DefaultFrequencySortedDictionary dict;
  private final Logger sLogger = Logger.getLogger(PreprocessHelper.class);
  private static final HMapSIW lang2AvgSentLen = new HMapSIW();
  static {
    lang2AvgSentLen.put("en",20);       // took average # of tokens per sentence in de-en.wmt12 train data
    lang2AvgSentLen.put("de",17);       // took average # of tokens per sentence in de-en.wmt12 train data
    lang2AvgSentLen.put("zh",27);       // took average # of tokens per sentence in zh-en.fbis train data
    lang2AvgSentLen.put("fr",18);       // took average # of tokens per sentence in fr-en.wmt12 train data
    lang2AvgSentLen.put("tr",21);       // took average # of tokens per sentence in tr-en.oflazer train data
    lang2AvgSentLen.put("ar",17);       // took average # of tokens per sentence in ar-en.galep5oct train data
  };

  /**
   * Implemented for HDFS cluster mode, files read from local cache
   */
  public PreprocessHelper(int minVectorTerms, int minSentenceLength, JobConf conf) throws Exception {
    super();
    sLogger.setLevel(Level.INFO);
    fLang = conf.get("fLang");
    eLang = conf.get("eLang");
    eDir = conf.get("eDir");

    MinVectorTerms = minVectorTerms;
    MinSentenceLength = minSentenceLength;
    loadModels(conf);
  }

  /**
   * Implemented for non-cluster mode, files read directly from local FS
   */
  public PreprocessHelper(int minVectorTerms, int minSentenceLength, Configuration conf) throws Exception {
    super();
//    sLogger.setLevel(Level.DEBUG);
    fLang = conf.get("fLang");
    eLang = conf.get("eLang");
    eDir = conf.get("eDir");

    MinVectorTerms = minVectorTerms;
    MinSentenceLength = minSentenceLength;
    loadFModels(conf);
    loadEModels(conf);
  }

  public void loadModels(JobConf job) throws Exception{
    loadFModels(job);
    loadEModels(job);
  }

  private void loadFModels(JobConf conf) throws Exception {
    sLogger.info("Loading models for " + fLang + " ...");

    FileSystem fs = FileSystem.get(conf);
    FileSystem localFs = FileSystem.getLocal(conf);
    Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

    String sentDetectorFile = getSentDetectorFile(fLang); //localFiles[6].toString();
    String eVocabSrcFile = getSrcVocab(eLang, fLang);           //localFiles[3].toString();
    String eVocabTrgFile = getTrgVocab(fLang, eLang);           //localFiles[4].toString();
    String fVocabSrcFile = getSrcVocab(fLang, eLang);           //localFiles[7].toString();
    String fVocabTrgFile = getTrgVocab(eLang, fLang);           //localFiles[8].toString();
    String f2e_ttableFile = getTTable(fLang, eLang);          //localFiles[9].toString();
    String e2f_ttableFile = getTTable(eLang, fLang);          //localFiles[10].toString();
    String modelFileName = getClassifierFile();           //localFiles[12].toString();

    Map<String, Path> pathMapping = Maps.newHashMap();
    for (Path p : localFiles) {
      sLogger.info("In DistributedCache: " + p);
      if (p.toString().contains(sentDetectorFile)) {
        pathMapping.put(sentDetectorFile, p);
        sLogger.info("--> sentdector");
      } else if (p.toString().contains(eVocabSrcFile)) {
        pathMapping.put(eVocabSrcFile, p);
        sLogger.info("--> eVocabSrcFile");
      } else if (p.toString().contains(eVocabTrgFile)) {
        pathMapping.put(eVocabTrgFile, p);
        sLogger.info("--> eVocabTrgFile");
      } else if (p.toString().contains(fVocabSrcFile)) {
        pathMapping.put(fVocabSrcFile, p);
        sLogger.info("--> fVocabSrcFile");
      } else if (p.toString().contains(fVocabTrgFile)) {
        pathMapping.put(fVocabTrgFile, p);
        sLogger.info("--> fVocabTrgFile");
      } else if (p.toString().contains(f2e_ttableFile)) {
        pathMapping.put(f2e_ttableFile, p);
        sLogger.info("--> f2e_ttableFile");
      } else if (p.toString().contains(e2f_ttableFile)) {
        pathMapping.put(e2f_ttableFile, p);
        sLogger.info("--> e2f_ttableFile");
      } else if (p.toString().contains(modelFileName)) {
        pathMapping.put(modelFileName, p);
        sLogger.info("--> classifier model");
      }
    }

    InputStream modelIn = localFs.open(pathMapping.get(sentDetectorFile));
    SentenceModel model = new SentenceModel(modelIn);
    fModel = new SentenceDetectorME(model);
    sLogger.info("Sentence model created successfully.");

    eVocabSrc = (VocabularyWritable) HadoopAlign.loadVocab(pathMapping.get(eVocabSrcFile), localFs);
    eVocabTrg = (VocabularyWritable) HadoopAlign.loadVocab(pathMapping.get(eVocabTrgFile), localFs);
    fVocabSrc = (VocabularyWritable) HadoopAlign.loadVocab(pathMapping.get(fVocabSrcFile), localFs);
    fVocabTrg = (VocabularyWritable) HadoopAlign.loadVocab(pathMapping.get(fVocabTrgFile), localFs);         
    f2e_Probs = new TTable_monolithic_IFAs(localFs, pathMapping.get(f2e_ttableFile), true);
    e2f_Probs = new TTable_monolithic_IFAs(localFs, pathMapping.get(e2f_ttableFile), true);

    // tokenizer file not read from cache, since it might be a directory (e.g. Chinese segmenter)
    String tokenizerFile = conf.get("fTokenizer");
    fTok = TokenizerFactory.createTokenizer(fs, fLang, tokenizerFile, fVocabSrc);
    sLogger.info("Tokenizer and vocabs created successfully.");

    // average sentence length = just a heuristic derived from sample text
    fScoreFn = (ScoringModel) new Bm25();
    fScoreFn.setAvgDocLength(lang2AvgSentLen.get(fLang));         

    // we use df table of English side, so we should read collection doc count from English dir
    RetrievalEnvironment eEnv = new RetrievalEnvironment(eDir, fs);
    fScoreFn.setDocCount(eEnv.readCollectionDocumentCount());   

    classifier = new MoreGenericModelReader(pathMapping.get(modelFileName), localFs).constructModel();
  }

  private void loadEModels(JobConf conf) throws Exception {
    sLogger.info("Loading models for " + eLang + " ...");

    String sentDetectorFile = getSentDetectorFile(eLang); //localFiles[1].toString();

    Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
    Map<String, Path> pathMapping = Maps.newHashMap();
    for (Path p : localFiles) {
      sLogger.info("In DistributedCache: " + p);
      if (p.toString().contains(sentDetectorFile)) {
        pathMapping.put(sentDetectorFile, p);
        sLogger.info("--> sentdector");
      } 
    }

    FileSystem localFs = FileSystem.getLocal(conf);
    InputStream modelIn = localFs.open(pathMapping.get(sentDetectorFile));
    SentenceModel model = new SentenceModel(modelIn);
    eModel = new SentenceDetectorME(model);
    sLogger.info("Sentence model created successfully.");

    FileSystem fs = FileSystem.get(conf);   
    RetrievalEnvironment env = new RetrievalEnvironment(eDir, fs);
    sLogger.info("Environment created successfully.");

    String tokenizerFile = conf.get("eTokenizer");
    eTok = TokenizerFactory.createTokenizer(fs, eLang, tokenizerFile, eVocabTrg);
    sLogger.info("Tokenizer and vocabs created successfully.");

    eScoreFn = (ScoringModel) new Bm25();
    eScoreFn.setAvgDocLength(lang2AvgSentLen.get(eLang));        //average sentence length = heuristic based on De-En data
    eScoreFn.setDocCount(env.readCollectionDocumentCount());

    dict = new DefaultFrequencySortedDictionary(new Path(env.getIndexTermsData()), new Path(env.getIndexTermIdsData()), new Path(env.getIndexTermIdMappingData()), fs);
    dfTable = new DfTableArray(new Path(env.getDfByTermData()), fs);

    //for backward compatibility
    //    String indexTermsFile = localFiles[12].toString();
    //    String dfTableFile = localFiles[0].toString();

    //for backward compatibility
    //    globalStatsMap = new PrefixEncodedGlobalStats(new Path(indexTermsFile), localFs);
    //    globalStatsMap.loadDFStats(new Path(dfTableFile));
  }

  public HMapSFW createFDocVector(String sentence) {
    return createFDocVector(sentence, new HMapSIW());
  }

  public HMapSFW createFDocVector(String sentence, HMapSIW term2Tf) {
    String[] terms = fTok.processContent(sentence);
    
    // don't count numbers for the min #terms constraint since Wikipedia has "sentences" full of numbers that doesn't make any sense
    int numNonNumbers = 0;
    for(String term : terms){
      term2Tf.increment(term);
      if (!term.matches("\\d+")) {
        numNonNumbers++;
      }
    }
    if(numNonNumbers < MinVectorTerms){
      return null;
    }

    //translated tf values
    HMapIFW transTermTf = new HMapIFW();
    for(Entry<String> entry : term2Tf.entrySet()){
      String fTerm = entry.getKey();
      int tf = entry.getValue();
      transTermTf = CLIRUtils.updateTFsByTerm(fTerm, tf, transTermTf, eVocabSrc, eVocabTrg, fVocabSrc, fVocabTrg, e2f_Probs, f2e_Probs, eTok, sLogger);
    }
    HMapSFW weightedVector = CLIRUtils.createTermDocVector(terms.length, transTermTf, eVocabTrg, fScoreFn, dict, dfTable, true, sLogger);

    return weightedVector;
  }

  public HMapSFW createEDocVector(String sentence) {
    HMapIFW term2Tf = new HMapIFW();
    HMapSFW weightedVector = new HMapSFW();
    String[] terms = eTok.processContent(sentence);

    // don't count numbers for the min #terms constraint since Wikipedia has "sentences" full of numbers that doesn't make any sense
    int numNonNumbers = 0;
    for(String term : terms){
      int termId = eVocabTrg.get(term);
      term2Tf.increment(termId, 1);
      if (!term.matches("\\d+")) {
        numNonNumbers++;
      }
    }
    if(numNonNumbers < MinVectorTerms){
      return null;
    }

    weightedVector = CLIRUtils.createTermDocVector(terms.length, term2Tf, eVocabTrg, eScoreFn, dict, dfTable, true, sLogger);

    //for backward compatibility
    //    weightedVector = CLIRUtils.createTermDocVector(terms.length, term2Tf, eVocabSrc, eScoreFn, globalStatsMap, true, sLogger);  

    return weightedVector;
  }

  public ArrayListWritable<Text> getESentences(String text, ArrayListWritable<HMapSFW> vectors, ArrayListOfIntsWritable sentLengths) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    ArrayListWritable<Text> sentences = new ArrayListWritable<Text>();
    String[] lines = text.split("\n");

    for(String line : lines){
      if(!line.matches("\\s+") && !line.isEmpty()){
        String[] sents = eModel.sentDetect(line);

        for(String sent : sents){
          if(sent.contains("date:")||sent.contains("jpg")||sent.contains("png")||sent.contains("gif")||sent.contains("fontsize:")||sent.contains("category:")){
            continue;
          }
          int length = eTok.getNumberTokens(sent);
          if(length >= MinSentenceLength){
            HMapSFW vector = createEDocVector(sent.toString());
            if(vector != null){
              vectors.add(vector);
              sentences.add(new Text(sent));
              if (sentLengths != null) sentLengths.add(length);
            }
          }
        }
      }
    }
    return sentences;
  }

  public ArrayListWritable<Text> getFSentences(String text, ArrayListWritable<HMapSFW> vectors, ArrayListOfIntsWritable sentLengths) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    //    sLogger.setLevel(Level.DEBUG);
    sLogger.debug("text length="+text.length());

    ArrayListWritable<Text> sentences = new ArrayListWritable<Text>();
    String[] lines = text.split("\n");
    sLogger.debug("num lines="+lines.length);

    for(String line : lines){
      // convert '。' to standard period character '.' for sentence detector to work
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < line.length(); i++) {
        char c = line.charAt(i);
        String unicode = String.format("%04x", (int) c);
        if (unicode.equals("3002")) {
          sb.append(".");
        }else {
          sb.append(c);
        }
      }
      line = sb.toString();
      sLogger.debug("line="+line);
      if (!line.matches("\\s+") && !line.isEmpty()) {
        String[] sents = fModel.sentDetect(line);

        for (String sent : sents) {
          sLogger.debug("sent="+sent);
          // discard some of the non-text content in Wikipedia
          if (sent.contains("datei:") || sent.contains("jpg") || sent.contains("png") || sent.contains("fontsize:") || sent.contains("kategorie:")) {
            continue;
          }
          int length = fTok.getNumberTokens(sent);
          if (length >= MinSentenceLength) {
            HMapSFW vector = createFDocVector(sent);
            if (vector != null) {
              vectors.add(vector);
              sentences.add(new Text(sent));
              sLogger.debug("added="+vector);

              if (sentLengths != null) sentLengths.add(length);
            }
          }
        }
      }
    }

    sLogger.setLevel(Level.INFO);

    return sentences;
  }

  private String getSentDetectorFile(String lang) {
    return lang+"-sent.bin";
  }

  private String getClassifierFile() {
    return "classifier-";
  }

  private String getTTable(String srcLang, String trgLang) {
    return "ttable." + srcLang + "-" + trgLang;
  }

  private String getTrgVocab(String srcLang, String trgLang) {
    return "vocab." + srcLang + "-" + trgLang + "." + trgLang;
  }

  private String getSrcVocab(String srcLang, String trgLang) {
    return "vocab." + srcLang + "-" + trgLang + "." + srcLang;
  }

  public MaxentModel getClassifier() {
    return classifier;
  }

  public Tokenizer getETokenizer() {
    return eTok;
  }

  public Tokenizer getFTokenizer() {
    return fTok;
  }


  public TTable_monolithic_IFAs getE2F() {
    return e2f_Probs;
  }

  public TTable_monolithic_IFAs getF2E() {
    return f2e_Probs;
  }

  public Vocab getFSrc() {
    return fVocabSrc;
  }

  public Vocab getETrg() {
    return eVocabTrg;
  }

  public Vocab getESrc() {
    return eVocabSrc;
  }

  public Vocab getFTrg() {
    return fVocabTrg;
  }

  private void loadFModels(Configuration conf) throws Exception {
    sLogger.info("Loading models for " + fLang + " ...");
    //    FileSystem fs = FileSystem.get(conf);
    FileSystem localFs = FileSystem.getLocal(conf);

    InputStream modelIn = localFs.open(new Path(conf.get("eSentDetectorFile")));
    SentenceModel model = new SentenceModel(modelIn);
    fModel = new SentenceDetectorME(model);
    sLogger.info("Sentence model created successfully.");

    eVocabSrc = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get("eVocabSrcFile")), localFs);
    eVocabTrg = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get("eVocabTrgFile")), localFs);
    fVocabSrc = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get("fVocabSrcFile")), localFs);
    fVocabTrg = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get("fVocabTrgFile")), localFs);         
    f2e_Probs = new TTable_monolithic_IFAs(localFs, new Path(conf.get("f2e_ttableFile")), true);
    e2f_Probs = new TTable_monolithic_IFAs(localFs, new Path(conf.get("e2f_ttableFile")), true);

    // tokenizer file not read from cache, since it might be a directory (e.g. Chinese segmenter)
    String tokenizerFile = conf.get("fTokenizer");
    fTok = TokenizerFactory.createTokenizer(localFs, fLang, tokenizerFile, fVocabSrc);
    sLogger.info("Tokenizer and vocabs created successfully.");

    // average sentence length = just a heuristic derived from sample text
    fScoreFn = (ScoringModel) new Bm25();
    fScoreFn.setAvgDocLength(lang2AvgSentLen.get(fLang));         

    // we use df table of English side, so we should read collection doc count from English dir
    RetrievalEnvironment eEnv = new RetrievalEnvironment(eDir, localFs);
    fScoreFn.setDocCount(eEnv.readCollectionDocumentCount());   

    classifier = new MoreGenericModelReader(new Path(conf.get("modelFileName")), localFs).constructModel();
  }

  private void loadEModels(Configuration conf) throws Exception {
    sLogger.info("Loading models for " + eLang + " ...");

    FileSystem localFs = FileSystem.getLocal(conf);
    InputStream modelIn = localFs.open(new Path(conf.get("fSentDetectorFile")));
    SentenceModel model = new SentenceModel(modelIn);
    eModel = new SentenceDetectorME(model);
    sLogger.info("Sentence model created successfully.");

    //    FileSystem fs = FileSystem.get(conf);   
    RetrievalEnvironment env = new RetrievalEnvironment(eDir, localFs);
    sLogger.info("Environment created successfully.");

    String tokenizerFile = conf.get("eTokenizer");
    eTok = TokenizerFactory.createTokenizer(localFs, eLang, tokenizerFile, eVocabTrg);
    sLogger.info("Tokenizer and vocabs created successfully.");

    eScoreFn = (ScoringModel) new Bm25();
    eScoreFn.setAvgDocLength(lang2AvgSentLen.get(eLang));        //average sentence length = heuristic based on De-En data
    eScoreFn.setDocCount(env.readCollectionDocumentCount());

    dict = new DefaultFrequencySortedDictionary(new Path(env.getIndexTermsData()), new Path(env.getIndexTermIdsData()), new Path(env.getIndexTermIdMappingData()), localFs);
    dfTable = new DfTableArray(new Path(env.getDfByTermData()), localFs);
  }

  public float getFInVocabRate(String fSent) {
    VocabularyWritable v = fTok.getVocab();
    boolean isStopwordRemoval = fTok.getStopwordRemoval();
    fTok.setVocab(null);
    
    
//    sLogger.setLevel(Level.INFO);
//    if (fSent.contains("Canto rodado (escuela de arte)") || fSent.contains("İsim Yaş Tür Bulunuş tarihi Ülke Keşfeden kişiler")) {
//      sLogger.setLevel(Level.DEBUG);
//    }
    
    
    String[] fTokens = fTok.processContent(fSent);
    float oov = 0, total = fTokens.length;
    for (String fToken : fTokens) {
      if (fVocabSrc.get(fToken) <= 0 && fVocabTrg.get(fToken) <= 0) {
        oov++;
        sLogger.debug(fToken+" --> OOV");
      }else {
        sLogger.debug(fToken+" --> in vocab");
      }
    }
    fTok.setVocab(v);
    fTok.setStopwordRemoval(isStopwordRemoval);
    return (total-oov) / total;
  }

  public float getEInVocabRate(String eSent) {
    VocabularyWritable v = eTok.getVocab();
    boolean isStopwordRemoval = eTok.getStopwordRemoval();
    eTok.setVocab(null);
    eTok.setStopwordRemoval(false);
    
//    sLogger.setLevel(Level.INFO);
//    if (eSent.contains("Name Age Species Date discovered Country Discovered by") || eSent.contains("Harri Rovanper")) {
//      sLogger.setLevel(Level.DEBUG);
//    }
    
    
    String[] eTokens = eTok.processContent(eSent);
    float oov = 0, total = eTokens.length;
    for (String eToken : eTokens) {
      if (eVocabSrc.get(eToken) <= 0 && eVocabTrg.get(eToken) <= 0) {
        oov++;
        sLogger.debug(eToken+" --> OOV");
      }else {
        sLogger.debug(eToken+" --> in vocab");
      }
    }
    eTok.setVocab(v);
    eTok.setStopwordRemoval(isStopwordRemoval);
    sLogger.debug(oov / total);
    return (total-oov) / total;
  }
}
