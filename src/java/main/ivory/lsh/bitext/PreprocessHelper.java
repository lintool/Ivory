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
import opennlp.model.MaxentModel;
import ivory.lsh.bitext.MoreGenericModelReader;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.util.map.MapKI.Entry;
import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

@SuppressWarnings("deprecation")
public class PreprocessHelper {
  private int MinVectorTerms, MinSentenceLength;
  private SentenceDetectorME fModel, eModel;
  private Tokenizer fTok, eTok;
  private VocabularyWritable eVocabSrc, eVocabTrg, fVocabTrg, fVocabSrc;
  private TTable_monolithic_IFAs f2e_Probs;
  private TTable_monolithic_IFAs e2f_Probs;
  private ScoringModel fScoreFn, eScoreFn;
  private HMapIFW transDfTable;
  private MaxentModel classifier;
//  private PrefixEncodedGlobalStats globalStatsMap;  // for backward compatibility
  private DfTableArray dfTable;
  private DefaultFrequencySortedDictionary dict;
  private final Logger sLogger = Logger.getLogger(PreprocessHelper.class);
  private static final HMapSIW lang2AvgSentLen = new HMapSIW();
  static {
    lang2AvgSentLen.put("en",29);       // took average # of tokens per sentence in de-en.wmt12 train data
    lang2AvgSentLen.put("de",27);       // took average # of tokens per sentence in de-en.wmt12 train data
    lang2AvgSentLen.put("zh",25);       // took average # of tokens per sentence in zh-en.fbis train data
    lang2AvgSentLen.put("fr",32);       // took average # of tokens per sentence in fr-en.wmt12 train data
    lang2AvgSentLen.put("tr",17);       // took average # of tokens per sentence in tr-en.oflazer train data
    lang2AvgSentLen.put("ar",21);       // took average # of tokens per sentence in ar-en.galep5oct train data
  };
  
  public PreprocessHelper(int minVectorTerms, int minSentenceLength, JobConf job) throws Exception {
    super();
    sLogger.setLevel(Level.INFO);
    MinVectorTerms = minVectorTerms;
    MinSentenceLength = minSentenceLength;
    loadModels(job);
  }

  public void loadModels(JobConf job) throws Exception{
    loadFModels(job);
    loadEModels(job);
  }

  private void loadFModels(JobConf job) throws Exception {
    String fLang = job.get("fLang");
    sLogger.info("Loading models for " + fLang + " ...");

    FileSystem fs = FileSystem.get(job);
    FileSystem localFs = FileSystem.getLocal(job);
    Path[] localFiles = null;
    localFiles = DistributedCache.getLocalCacheFiles(job);

    String dir = job.get("fDir");
    String sentDetectorFile = localFiles[6].toString();
    String tokenizerFile = job.get("fTokenizer");//localFiles[7].toString();
    String eVocabSrcFile = localFiles[3].toString();
    String eVocabTrgFile = localFiles[4].toString();
    String fVocabSrcFile = localFiles[8].toString();
    String fVocabTrgFile = localFiles[9].toString();
    String f2e_ttableFile = localFiles[10].toString();
    String e2f_ttableFile = localFiles[11].toString();
    String dfTableFile = localFiles[5].toString();

    RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
    sLogger.info("Environment created successfully.");

    InputStream modelIn = localFs.open(new Path(sentDetectorFile));

    SentenceModel model = new SentenceModel(modelIn);
    fModel = new SentenceDetectorME(model);

    sLogger.info("Sentence model created successfully.");

    fTok = TokenizerFactory.createTokenizer(fs, fLang, tokenizerFile, null);
    
    eVocabSrc = (VocabularyWritable) HadoopAlign.loadVocab(new Path(eVocabSrcFile), localFs);
    eVocabTrg = (VocabularyWritable) HadoopAlign.loadVocab(new Path(eVocabTrgFile), localFs);
    fVocabSrc = (VocabularyWritable) HadoopAlign.loadVocab(new Path(fVocabSrcFile), localFs);
    fVocabTrg = (VocabularyWritable) HadoopAlign.loadVocab(new Path(fVocabTrgFile), localFs);         
    f2e_Probs = new TTable_monolithic_IFAs(localFs, new Path(f2e_ttableFile), true);
    e2f_Probs = new TTable_monolithic_IFAs(localFs, new Path(e2f_ttableFile), true);

    sLogger.info("Tokenizer and vocabs created successfully.");

    fScoreFn = (ScoringModel) new Bm25();
    fScoreFn.setAvgDocLength(lang2AvgSentLen.get(fLang));      //average sentence length = just a heuristic
    fScoreFn.setDocCount(env.readCollectionDocumentCount());
    transDfTable = CLIRUtils.readTransDfTable(new Path(dfTableFile), localFs);

    String modelFileName = localFiles[13].toString();
    classifier = new MoreGenericModelReader(modelFileName, FileSystem.getLocal(job)).constructModel();
  }
  
  private void loadEModels(JobConf job) throws Exception {
    sLogger.info("Loading models...");

    FileSystem fs = FileSystem.get(job);
    FileSystem localFs = FileSystem.getLocal(job);
    Path[] localFiles = null;
    localFiles = DistributedCache.getLocalCacheFiles(job);

    String dir = job.get("eDir");
    String sentDetectorFile = localFiles[1].toString();
    String tokenizerFile = localFiles[2].toString();
    //for backward compatibility
//    String indexTermsFile = localFiles[12].toString();
//    String dfTableFile = localFiles[0].toString();

    RetrievalEnvironment env = new RetrievalEnvironment(dir, fs);
    sLogger.info("Environment created successfully.");

    eTok = TokenizerFactory.createTokenizer(localFs, "en", tokenizerFile, null);

    sLogger.info("Tokenizer and vocabs created successfully.");

    eScoreFn = (ScoringModel) new Bm25();
    eScoreFn.setAvgDocLength(16.0f);        //average sentence length = heuristic based on De-En data
    eScoreFn.setDocCount(env.readCollectionDocumentCount());

    InputStream modelIn = localFs.open(new Path(sentDetectorFile));
    SentenceModel model = new SentenceModel(modelIn);
    eModel = new SentenceDetectorME(model);

    sLogger.info("Sentence model created successfully.");

    dict = new DefaultFrequencySortedDictionary(new Path(env.getIndexTermsData()), new Path(env.getIndexTermIdsData()), new Path(env.getIndexTermIdMappingData()), fs);
    dfTable = new DfTableArray(new Path(env.getDfByTermData()), fs);
    
    //for backward compatibility
//    globalStatsMap = new PrefixEncodedGlobalStats(new Path(indexTermsFile), localFs);
//    globalStatsMap.loadDFStats(new Path(dfTableFile));
  }

  public HMapSFW createFDocVector(String sentence) {
    return createFDocVector(sentence, new HMapSIW());
  }
  
  public HMapSFW createFDocVector(String sentence, HMapSIW term2Tf) {
    String[] terms = fTok.processContent(sentence);
    for(String term : terms){
      term2Tf.increment(term);
    }
    
    if(term2Tf.size() < MinVectorTerms){
      return null;
    }
    
    //translated tf values
    HMapIFW transTermTf = new HMapIFW();

    for(Entry<String> entry : term2Tf.entrySet()){
      String fTerm = entry.getKey();
      int tf = entry.getValue();
      transTermTf = CLIRUtils.updateTFsByTerm(fTerm, tf, transTermTf, eVocabSrc, eVocabTrg, fVocabSrc, fVocabTrg, e2f_Probs, f2e_Probs, sLogger);
    }
    HMapSFW weightedVector = CLIRUtils.createTermDocVector(terms.length, transTermTf, eVocabSrc, fScoreFn, transDfTable, true, sLogger);
    
    return weightedVector;
  }

  public HMapSFW createEDocVector(String sentence) {
    HMapSIW term2Tf = new HMapSIW();
    HMapSFW weightedVector = new HMapSFW();
    String[] terms = eTok.processContent(sentence);

    for(String term : terms){
      term2Tf.increment(term);
    }
    
    if(term2Tf.size() < MinVectorTerms){
      return null;
    }

    weightedVector = CLIRUtils.createTermDocVector(terms.length, term2Tf, eVocabSrc, eScoreFn, dict, dfTable, true, sLogger);

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
    ArrayListWritable<Text> sentences = new ArrayListWritable<Text>();
    String[] lines = text.split("\n");
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
      
      if (!line.matches("\\s+") && !line.isEmpty()) {
        String[] sents = fModel.sentDetect(line);

        for (String sent : sents) {
          // discard some of the non-text content in Wikipedia
          if (sent.contains("datei:") || sent.contains("jpg") || sent.contains("png") || sent.contains("fontsize:") || sent.contains("kategorie:")) {
            continue;
          }
           sLogger.info("Sent="+sent);
          int length = fTok.getNumberTokens(sent);
          if (length >= MinSentenceLength) {
            HMapSFW vector = createFDocVector(sent);
            if (vector != null) {
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


  public VocabularyWritable getESrc() {
    return eVocabSrc;
  }

  public VocabularyWritable getETrg() {
    return eVocabTrg;
  }

  public VocabularyWritable getFTrg() {
    return fVocabTrg;
  }

  public VocabularyWritable getFSrc() {
    return fVocabSrc;
  }

  public TTable_monolithic_IFAs getF2E() {
    return f2e_Probs;
  }

  public TTable_monolithic_IFAs getE2F() {
    return e2f_Probs;
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
}
