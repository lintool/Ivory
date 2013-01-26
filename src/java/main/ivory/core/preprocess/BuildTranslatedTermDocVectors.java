package ivory.core.preprocess;

import ivory.core.Constants;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.dictionary.DefaultFrequencySortedDictionary;
import ivory.core.data.document.TermDocVector;
import ivory.core.data.stat.DfTableArray;
import ivory.core.data.stat.DocLengthTable;
import ivory.core.data.stat.DocLengthTable4B;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.core.util.CLIRUtils;
import ivory.pwsim.score.ScoringModel;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.google.common.collect.Maps;
import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.MapIF;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

/**
 * Translates term doc vectors in foreign language (e.g. German) into target language (e.g. English) using the CLIR technique discussed in Probabilistic Structured
Query Methods, SIGIR'03, Kareem Darwish and Douglas W. Oard.

 * @author ferhanture
 *
 */
@SuppressWarnings( "deprecation")
public class BuildTranslatedTermDocVectors extends PowerTool {
  private static final Logger LOG = Logger.getLogger(BuildTranslatedTermDocVectors.class);
  private static int SAMPLING = 1;

  protected static enum Docs { DBG, ZERO, SHORT, SHORTAfterTranslation, Total };
  protected static enum DF { TransDf, NoDf }

  private static class MyMapperTrans extends MapReduceBase implements
  Mapper<IntWritable, TermDocVector, IntWritable, HMapSFW> {

    private ScoringModel model;
    // eVocabSrc is the English vocabulary for probability table e2f_Probs.
    // engVocabTrgis the English vocabulary for probability table f2e_Probs.
    // fVocabSrc is the German vocabulary for probability table f2e_Probs.
    // fVocabTrg is the German vocabulary for probability table e2f_Probs.	
    static Vocab eVocabSrc, fVocabSrc, fVocabTrg, eVocabTrg;
    static TTable_monolithic_IFAs f2e_Probs, e2f_Probs;
    private Tokenizer tokenizer;
    static float avgDocLen;
    static int numDocs;
    static boolean isNormalize;
    private String language;
    int MIN_SIZE = 0;	// minimum document size, to avoid noise in Wikipedia due to stubs/very short articles etc. this is set via Conf object
    DefaultFrequencySortedDictionary dict;
    DfTableArray dfTable; 

    private String getFilename(String s) {
      return s.substring(s.lastIndexOf("/") + 1);
    }

    public void configure(JobConf conf) {
      String termsFile, termidsFile, idToTermFile, dfFile;
      numDocs = conf.getInt("Ivory.CollectionDocumentCount", -1);
      avgDocLen = conf.getFloat("Ivory.AvgDocLen", -1);
      isNormalize = conf.getBoolean("Ivory.Normalize", false);
      language = conf.get("Ivory.Lang");
      LOG.debug(numDocs + " " + avgDocLen);
      MIN_SIZE = conf.getInt("Ivory.MinNumTerms", 0);

      try {
        if (conf.get ("mapred.job.tracker").equals ("local")) {
          // Explicitly not support local mode.
          throw new RuntimeException("Local mode not supported!");
        }

        FileSystem remoteFS = FileSystem.get(conf);
        RetrievalEnvironment targetEnv = new RetrievalEnvironment(conf.get(Constants.TargetIndexPath), remoteFS);

        termsFile = getFilename(targetEnv.getIndexTermsData());
        termidsFile = getFilename(targetEnv.getIndexTermIdsData());
        idToTermFile = getFilename(targetEnv.getIndexTermIdMappingData());
        dfFile = getFilename(targetEnv.getDfByIntData());

        FileSystem fs = FileSystem.getLocal(conf);
        Map<String, Path> pathMapping = Maps.newHashMap();

        // We need to figure out which file in the DistributeCache is which...
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
        for (Path p : localFiles) {
          LOG.info("In DistributedCache: " + p);
          if (p.toString().contains(termsFile)) {
            pathMapping.put(termsFile, p);
          } else if (p.toString().contains(termidsFile)) {
            pathMapping.put(termidsFile, p);
          } else if (p.toString().contains(idToTermFile)) {
            pathMapping.put(idToTermFile, p);
          } else if (p.toString().contains(dfFile)) {
            pathMapping.put(dfFile, p);
          } else if (p.toString().contains(getFilename(conf.get("Ivory.E_Vocab_F2E")))) {
            pathMapping.put("Ivory.E_Vocab_F2E", p);
            LOG.info("Ivory.E_Vocab_F2E -> " + p);
          } else if (p.toString().contains(getFilename(conf.get("Ivory.F_Vocab_F2E")))) {
            pathMapping.put("Ivory.F_Vocab_F2E", p);
            LOG.info("Ivory.F_Vocab_F2E -> " + p);
          }  else if (p.toString().contains(getFilename(conf.get("Ivory.TTable_F2E")))) {
            pathMapping.put("Ivory.TTable_F2E", p);
            LOG.info("Ivory.TTable_F2E -> " + p);
          } else if (p.toString().contains(getFilename(conf.get("Ivory.E_Vocab_E2F")))) {
            pathMapping.put("Ivory.E_Vocab_E2F", p);
            LOG.info("Ivory.E_Vocab_E2F -> " + p);
          } else if (p.toString().contains(getFilename(conf.get("Ivory.F_Vocab_E2F")))) {
            pathMapping.put("Ivory.F_Vocab_E2F", p);
            LOG.info("Ivory.F_Vocab_E2Ff -> " + p);
          } else if (p.toString().contains(getFilename(conf.get("Ivory.TTable_E2F")))) {
            pathMapping.put("Ivory.TTable_E2F", p);
            LOG.info("Ivory.TTable_E2F -> " + p);
          } else if (p.toString().contains(getFilename(conf.get(Constants.TargetStemmedStopwordList)))) {
            pathMapping.put(Constants.TargetStemmedStopwordList, p);
            LOG.info(Constants.TargetStemmedStopwordList + " -> " + p);
          } else if (p.toString().contains(getFilename(conf.get(Constants.TargetStopwordList)))) {
            pathMapping.put(Constants.TargetStopwordList, p);
            LOG.info(Constants.TargetStopwordList + " -> " + p);
          } else if (p.toString().contains(getFilename(conf.get(Constants.TargetTokenizer)))) {
            pathMapping.put(Constants.TargetTokenizer, p);
            LOG.info(Constants.TargetTokenizer + " -> " + p);
          }
        }

        LOG.info(" - terms: " + pathMapping.get(termsFile));
        LOG.info(" - id: " + pathMapping.get(termidsFile));
        LOG.info(" - idToTerms: " + pathMapping.get(idToTermFile));
        LOG.info(" - df data: " + pathMapping.get(dfFile));

        try{
          dict = new DefaultFrequencySortedDictionary(pathMapping.get(termsFile),
              pathMapping.get(termidsFile), pathMapping.get(idToTermFile), FileSystem.getLocal(conf));
          dfTable = new DfTableArray(pathMapping.get(dfFile), FileSystem.getLocal(conf));
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException("Error loading Terms File for dictionary from "+localFiles[0]);
        }     

        eVocabTrg = HadoopAlign.loadVocab(pathMapping.get("Ivory.E_Vocab_F2E"), fs);
        fVocabSrc = HadoopAlign.loadVocab(pathMapping.get("Ivory.F_Vocab_F2E"), fs);
        f2e_Probs = new TTable_monolithic_IFAs(fs, pathMapping.get("Ivory.TTable_F2E"), true);

        eVocabSrc = HadoopAlign.loadVocab(pathMapping.get("Ivory.E_Vocab_E2F"), fs);
        fVocabTrg = HadoopAlign.loadVocab(pathMapping.get("Ivory.F_Vocab_E2F"), fs);
        e2f_Probs = new TTable_monolithic_IFAs(fs, pathMapping.get("Ivory.TTable_E2F"), true);

        String tokenizerModel = pathMapping.get(Constants.TargetTokenizer).toString();
        String stopwordsFile = pathMapping.get(Constants.TargetStopwordList).toString();
        String stemmedStopwordsFile = pathMapping.get(Constants.TargetStemmedStopwordList).toString();       
        tokenizer = TokenizerFactory.createTokenizer(fs, conf.get(Constants.TargetLanguage), 
            tokenizerModel, true, 
            stopwordsFile, 
            stemmedStopwordsFile, null);   // just for stopword removal in translateTFs
      } catch (IOException e) {
        throw new RuntimeException ("Local cache files not read properly.");
      }

      try {
        model = (ScoringModel) Class.forName(conf.get("Ivory.ScoringModel")).newInstance();
      } catch (Exception e) {
        throw new RuntimeException("Error initializing Ivory.ScoringModel!");
      }

      // this only needs to be set once for the entire collection
      model.setDocCount(numDocs);
      model.setAvgDocLength(avgDocLen);

      try {
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing tokenizer!");
      }

      if (conf.get("debug") != null) {
        LOG.setLevel(Level.DEBUG);
      }
      LOG.info("# docs in collection = "+numDocs);
      LOG.info("avg doc len = "+avgDocLen);
      LOG.info("---------");
    }

    public void map(IntWritable docno, TermDocVector doc,
        OutputCollector<IntWritable, HMapSFW> output, Reporter reporter) throws IOException {
      if (docno.get() % SAMPLING != 0) {
        return; // for generating sample document vectors. no sampling if SAMPLING=1
      }

      if (!language.equals("english") && !language.equals("en")) {
        docno.set(docno.get() + 1000000000);
        // To distinguish between the two collections in the PWSim sliding window algorithm.
      }

      // Translate doc vector.
      TermDocVector.Reader reader = doc.getReader();
      int numTerms = reader.getNumberOfTerms(); 
      if (numTerms == 0) {
        reporter.incrCounter(Docs.ZERO, 1);
        return;
      }else if (numTerms < MIN_SIZE) {
        reporter.incrCounter(Docs.SHORT, 1);
        return;
      }

      HMapIFW tfS = new HMapIFW();
      // We simply use the source-language doc length since the ratio of doc length to average doc
      // length is unlikely to change significantly (not worth complicating the pipeline)
      int docLen = CLIRUtils.translateTFs(doc, tfS, eVocabSrc, eVocabTrg, fVocabSrc, fVocabTrg,
          e2f_Probs, f2e_Probs, tokenizer, LOG);

      HMapSFW v = CLIRUtils.createTermDocVector(docLen, tfS, eVocabTrg, model, dict, dfTable,
          isNormalize, LOG);

      // If no translation of any word is in the target vocab, remove document i.e., our model
      // wasn't capable of translating it.
      if (v.size() < MIN_SIZE) {
        reporter.incrCounter(Docs.SHORTAfterTranslation, 1);
        return;
      } else {
        reporter.incrCounter(Docs.Total, 1);
        output.collect(docno, v);
      }
    }
  }

  public BuildTranslatedTermDocVectors(Configuration conf) {
    super(conf);
  }

  public static final String[] RequiredParameters = { Constants.IndexPath, Constants.TargetIndexPath, "Ivory.ScoringModel" };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  @Override
  public int runTool() throws Exception {
    String indexPath = getConf().get(Constants.IndexPath);
    String scoringModel = getConf().get("Ivory.ScoringModel");

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, FileSystem.get(getConf()));    
    String outputPath = env.getWeightedTermDocVectorsDirectory();
    String fVocab_f2e= getConf().get("Ivory.F_Vocab_F2E");   // fVocab from P(e|f)
    String eVocab_f2e = getConf().get("Ivory.E_Vocab_F2E");  // eVocab from P(e|f)
    String ttable_f2e = getConf().get("Ivory.TTable_F2E");   // P(e|f)
    String eVocab_e2f = getConf().get("Ivory.E_Vocab_E2F");  // eVocab from P(f|e)
    String fVocab_e2f = getConf().get("Ivory.F_Vocab_E2F");  // fVocab from P(f|e)
    String ttable_e2f = getConf().get("Ivory.TTable_E2F");   // P(f|e)

    String eStopwords = getConf().get(Constants.TargetStopwordList); 
    String eStemmedStopwords = getConf().get(Constants.TargetStemmedStopwordList); 
    String eTokenizerModel = getConf().get(Constants.TargetTokenizer); 

    String targetIndexPath = getConf().get(Constants.TargetIndexPath);
    RetrievalEnvironment targetEnv = new RetrievalEnvironment(targetIndexPath, FileSystem.get(getConf()));
    String termsFilePath = targetEnv.getIndexTermsData();
    String termsIdsFilePath = targetEnv.getIndexTermIdsData();
    String termIdMappingFilePath = targetEnv.getIndexTermIdMappingData();
    String dfByIntFilePath = targetEnv.getDfByIntData();

    JobConf conf = new JobConf(getConf(), BuildTranslatedTermDocVectors.class);
    conf.setJobName("BuildTranslatedTermDocVectors");
    FileSystem fs = FileSystem.get(conf);

    if (fs.exists(new Path(outputPath))) {
      LOG.info(outputPath + ": Translated term doc vectors already exist! Nothing to do for this job...");
      return 0;
    }

    String collectionName = getConf().get("Ivory.CollectionName");
    String inputPath = env.getTermDocVectorsDirectory();

    LOG.info("Preparing to build document vectors using " + scoringModel);
    LOG.info("Document vectors to be stored in " + outputPath);
    LOG.info("CollectionName: " + collectionName);
    LOG.info("Input path: " + inputPath);
    LOG.info("Target-language stopwords: " + eStopwords);
    LOG.info("Target-language stemmed stopwords: " + eStemmedStopwords);
    LOG.info("Target-language tokenizer model: " + eTokenizerModel);

    DocLengthTable mDLTable;
    try {
      mDLTable = new DocLengthTable4B(env.getDoclengthsData(), fs);
    } catch (IOException e1) {
      throw new RuntimeException("Error initializing Doclengths file");
    }
    LOG.info(mDLTable.getAvgDocLength()+" is average source-language document length.");
    LOG.info(targetEnv.readCollectionDocumentCount()+" is number of target-language docs. We use the target-side DF table so we set #docs to this value in our scoring model.");

    /////// Configuration setup

    conf.set(Constants.IndexPath, indexPath);
    conf.set("Ivory.ScoringModel", scoringModel);
    conf.setFloat("Ivory.AvgDocLen", mDLTable.getAvgDocLength());
    conf.setInt(Constants.CollectionDocumentCount, targetEnv.readCollectionDocumentCount());
    conf.set(Constants.Language, getConf().get("Ivory.Lang"));
    conf.set("Ivory.Normalize", getConf().get("Ivory.Normalize"));
    conf.set("Ivory.MinNumTerms", getConf().get("Ivory.MinNumTerms"));

    conf.setNumMapTasks(300);			
    conf.setNumReduceTasks(0);
    conf.set("mapred.child.java.opts", "-Xmx2048m");
    conf.setInt("mapred.map.max.attempts", 10);
    conf.setInt("mapred.reduce.max.attempts", 10);
    conf.setInt("mapred.task.timeout", 6000000);

    //////// Cache files

    DistributedCache.addCacheFile(new URI(termsFilePath), conf);
    DistributedCache.addCacheFile(new URI(termsIdsFilePath), conf);
    DistributedCache.addCacheFile(new URI(termIdMappingFilePath), conf);
    DistributedCache.addCacheFile(new URI(dfByIntFilePath), conf);
    DistributedCache.addCacheFile(new URI(eVocab_f2e), conf);
    DistributedCache.addCacheFile(new URI(fVocab_f2e), conf);
    DistributedCache.addCacheFile(new URI(ttable_f2e), conf);
    DistributedCache.addCacheFile(new URI(eVocab_e2f), conf);
    DistributedCache.addCacheFile(new URI(fVocab_e2f), conf);
    DistributedCache.addCacheFile(new URI(ttable_e2f), conf);
    DistributedCache.addCacheFile(new URI(ttable_e2f), conf);
    DistributedCache.addCacheFile(new URI(eStopwords), conf);
    DistributedCache.addCacheFile(new URI(eStemmedStopwords), conf);
    DistributedCache.addCacheFile(new URI(eTokenizerModel), conf);

    FileInputFormat.setInputPaths(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(HMapSFW.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(HMapSFW.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);

    conf.setMapperClass(MyMapperTrans.class);

    long startTime = System.currentTimeMillis();
    JobClient.runJob(conf);
    LOG.info("Job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  private void createTranslatedDFFile(String transDfFile) {
    try {
      JobConf conf2 = new JobConf(getConf(), BuildTranslatedTermDocVectors.class);
      conf2.setJobName("BuildTranslatedDfTable");
      FileSystem fs2 = FileSystem.get(conf2);

      if (fs2.exists(new Path(transDfFile))) {
        LOG.info("Translated Df file already exists! Nothing to do for this job...");
      } else {
        LOG.info("Creating translated Df file ...");
        conf2.set("mapred.child.java.opts", "-Xmx2048m");
        conf2.setInt("mapred.map.max.attempts", 10);
        conf2.setInt("mapred.reduce.max.attempts", 10);
        conf2.setInt("mapred.task.timeout", 6000000);
        conf2.set("TransDfFile", transDfFile);
        conf2.setSpeculativeExecution(false);
        conf2.setNumMapTasks(1);
        conf2.setNumReduceTasks(0);
        conf2.setInputFormat(NullInputFormat.class);
        conf2.setOutputFormat(NullOutputFormat.class);
        conf2.setMapperClass(DataWriterMapper.class);
        JobClient.runJob(conf2);
        LOG.info("Translating DF table done.");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static class DataWriterMapper extends NullMapper {
    public void run(JobConf conf, Reporter reporter) throws IOException {
      Logger sLogger = Logger.getLogger(DataWriterMapper.class);
      sLogger.setLevel(Level.DEBUG);

      String indexPath = conf.get(Constants.IndexPath);
      FileSystem fs2  = FileSystem.get(conf);

      RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs2);

      String transDfFile = conf.get("TransDfFile");
      String eFile = conf.get("Ivory.E_Vocab_E2F");
      String fFile = conf.get("Ivory.F_Vocab_E2F");

      String e2fttableFile = conf.get("Ivory.TTable_E2F");
      String termsFile = env.getIndexTermsData();
      String dfByIntFile = env.getDfByIntData();

      if(!fs2.exists(new Path(fFile)) || !fs2.exists(new Path(eFile)) || !fs2.exists(new Path(e2fttableFile)) || !fs2.exists(new Path(termsFile)) || !fs2.exists(new Path(dfByIntFile))){
        throw new RuntimeException("Error: Translation files do not exist!");
      }

      Vocab eVocab_e2f = null, fVocab_e2f = null;
      TTable_monolithic_IFAs en2DeProbs = null;
      try {
        eVocab_e2f = HadoopAlign.loadVocab(new Path(eFile), conf);
        fVocab_e2f = HadoopAlign.loadVocab(new Path(fFile), conf);

        en2DeProbs = new TTable_monolithic_IFAs(fs2, new Path(e2fttableFile), true);
      } catch (IOException e) {
        e.printStackTrace();
      }	

      DefaultFrequencySortedDictionary dict = new DefaultFrequencySortedDictionary(new Path(env.getIndexTermsData()), new Path(env.getIndexTermIdsData()), new Path(env.getIndexTermIdMappingData()), fs2);
      DfTableArray dfTable = new DfTableArray(new Path(dfByIntFile), fs2);

      HMapIFW transDfTable = CLIRUtils.translateDFTable(eVocab_e2f, fVocab_e2f, en2DeProbs, dict, dfTable);

      SequenceFile.Writer writer = SequenceFile.createWriter(fs2, conf, new Path(transDfFile), IntWritable.class, FloatWritable.class);
      for(MapIF.Entry term : transDfTable.entrySet()){
        reporter.incrCounter(DF.TransDf, 1);
        writer.append(new IntWritable(term.getKey()), new FloatWritable(term.getValue()));
      }
      writer.close();
    }
  }
}
