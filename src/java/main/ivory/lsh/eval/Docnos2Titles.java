package ivory.lsh.eval;

import ivory.core.util.CLIRUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.map.HMapIIW;
import edu.umd.cloud9.io.pair.PairOfIntString;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.map.HMapIV;

/**

 * @author ferhanture
 * 
 */
public class Docnos2Titles extends Configured implements Tool {

  private static final Logger sLogger = Logger.getLogger(Docnos2Titles.class);

  private static Options options;

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "Docnos2Titles", options );
    System.exit(-1);    
  }

  static enum Pairs {
    COUNT, COUNT2, COUNT3, COUNTE, COUNTF, COUNT4, COUNT3x;
  }  
  /**
   * Candidate generation
   * 
   * Map: (edocno, eWikiPage) --> (<fdocno, edocno>, <E, eTitle>)
   * Map: (fdocno, fWikiPage) --> (<fdocno, edocno>, <F, fTitle>)
   * Input is the union of source and target collections
   * @author ferhanture
   */
  private static class MyMapper extends MapReduceBase implements
  Mapper<IntWritable, WikipediaPage, PairOfInts, PairOfIntString> {

    private HMapIV<ArrayListOfIntsWritable> pwsimMapping;   // mapping for pwsim pairs
    private JobConf mJob;
    private ArrayListOfIntsWritable similarDocnos;
    private String srcLang;
    private PairOfIntString valOut;
    private PairOfInts keyOut;
    private HMapIIW samplesMap = null;

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);
      srcLang = job.get("fLang");      
      mJob = job;
      pwsimMapping = new HMapIV<ArrayListOfIntsWritable>();
      valOut = new PairOfIntString();
      keyOut = new PairOfInts();

      // read doc ids of sample into vectors
      String samplesFile = job.get("Ivory.SampleFile"); 
      if (samplesFile != null) {
        try {
          samplesMap = readSamplesFromCache(getFilename(samplesFile), job);
        } catch (NumberFormatException e) {
          e.printStackTrace();
          throw new RuntimeException("Incorrect format in " + samplesFile);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException("I/O error in " + samplesFile);
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException("Error reading sample file: " + samplesFile);
        }
      }
    }

    private static String getFilename(String s) {
      return s.substring(s.lastIndexOf("/") + 1);
    }

    private static void loadPairs(HMapIV<ArrayListOfIntsWritable> pwsimMapping, int langID, JobConf job, Reporter reporter){
      try {
        Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
        String pwsimFile = job.get("PwsimPairs");
        for (Path localFile : localFiles) {
          if (localFile.toString().contains(getFilename(pwsimFile))) {
            SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(job), localFile, job);

            PairOfInts key = (PairOfInts) reader.getKeyClass().newInstance();
            IntWritable value = (IntWritable) reader.getValueClass().newInstance();
            int cnt = 0;
            while (reader.next(key, value)) {
              int fDocno = key.getRightElement();
              int eDocno = key.getLeftElement();
              if ((eDocno == 6127 && fDocno == 1000000074) || (eDocno == 6127 && fDocno == 1000000071)) {
                sLogger.info(key);
              }
              if(langID == CLIRUtils.E){
                if(!pwsimMapping.containsKey(eDocno)){
                  pwsimMapping.put(eDocno, new ArrayListOfIntsWritable());
                }
                pwsimMapping.get(eDocno).add(fDocno);   // we add 1000000000 to foreign docnos to distinguish them during pwsim algo
              }else{
                if(!pwsimMapping.containsKey(fDocno)){
                  pwsimMapping.put(fDocno, new ArrayListOfIntsWritable());
                }
                pwsimMapping.get(fDocno).add(eDocno);   // we add 1000000000 to foreign docnos to distinguish them during pwsim algo
              }
              cnt++;
              key = (PairOfInts) reader.getKeyClass().newInstance();
              value = (IntWritable) reader.getValueClass().newInstance();
            }
            reader.close();
            sLogger.info(pwsimMapping.size() + "," + cnt + " pairs loaded from " + localFile);

          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private HMapIIW readSamplesFromCache(String samplesFile, JobConf conf) throws IOException {
      Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
      HMapIIW samplesMap = null;
      for (Path localFile : localFiles) {
        if (localFile.toString().contains(samplesFile)) {
          samplesMap = new HMapIIW();
          LineReader reader = new LineReader(FileSystem.getLocal(conf).open(localFile));
          Text t = new Text();
          while (reader.readLine(t) != 0) {
            int docno = Integer.parseInt(t.toString());
            sLogger.info(docno + " --> sample");
            samplesMap.put(docno, 1);
          }
          reader.close();
          sLogger.info(samplesMap.size() + " sampled");
        }
      }
      if (samplesMap == null) throw new RuntimeException("Not found in local cache: " + samplesFile);
      return samplesMap;
    }

    public void map(IntWritable docnoKey, WikipediaPage p, OutputCollector<PairOfInts, PairOfIntString> output, Reporter reporter) throws IOException {
      int docno = docnoKey.get();
      String title = p.getTitle();
      String lang = p.getLanguage();
      int langID = lang.equals(srcLang) ? CLIRUtils.F : CLIRUtils.E;
     
      if (langID == CLIRUtils.F ) {
        docno += 1000000000;
        if (samplesMap != null && !samplesMap.containsKey(docno)) {
          return;
        }
      }
      
      // we only load the mapping once, during the first map() call of a mapper. 
      // this works b/c all input kv pairs of a given mapper will have same lang id (reason explained above)
      if (pwsimMapping.isEmpty()) {
        loadPairs(pwsimMapping, langID, mJob, reporter);
        sLogger.info("Mapping loaded: " + pwsimMapping.size());
      }

      // if no similar docs for docno, return
      if (pwsimMapping.containsKey(docno)) {
        similarDocnos = pwsimMapping.get(docno);  
      }else{
        return;
      }

      for (int similarDocno : similarDocnos) {
        if (langID == CLIRUtils.E) {
          if (samplesMap != null && !samplesMap.containsKey(similarDocno)) {
            continue;
          }
          keyOut.set(similarDocno, docno);
        }else {
          keyOut.set(docno, similarDocno);          
        }
        valOut.set(langID, title);
        output.collect(keyOut, valOut);
      }   
    }
  }

  private static class MyReducer extends MapReduceBase implements
  Reducer<PairOfInts, PairOfIntString, Text, Text>{
    private Text fTitle, eTitle;

    public void configure(JobConf job) {
      fTitle = new Text();
      eTitle = new Text();
    }

    @Override
    public void reduce(PairOfInts docnoPair, Iterator<PairOfIntString> titles,
        OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      eTitle.clear();
      fTitle.clear();
      sLogger.info(docnoPair);

      int cnt = 0;
      while (titles.hasNext()) {
        PairOfIntString title = titles.next();
        sLogger.info(title);
        if (title.getLeftElement() == CLIRUtils.E) {
          eTitle.set(title.getRightElement());
          cnt++;
        } else if (title.getLeftElement() == CLIRUtils.F) {
          fTitle.set(title.getRightElement());
          cnt++;
        } else {
          throw new RuntimeException("Unknown language ID: " + title.getLeftElement());
        }
      }

      if (cnt == 2) {
        output.collect(fTitle, eTitle);
      }else {
        sLogger.info("Incomplete data for " + docnoPair + ":" + fTitle + "," + eTitle);
      }
    }

  }


  /**
   * Runs this tool.
   */
  @SuppressWarnings("deprecation")
  public int run(String[] args) throws Exception {
    JobConf job = new JobConf(getConf(), Docnos2Titles.class);

    // Read commandline arguments
    CommandLine cmdline = parseArgs(args);
    if (cmdline == null) {
      printUsage();
    }
    String eCollectionPath = cmdline.getOptionValue(ECOLLECTION_OPTION);
    String fCollectionPath = cmdline.getOptionValue(FCOLLECTION_OPTION);
    String pwsimOutputPath = cmdline.getOptionValue(PWSIM_OPTION);
    String titlePairsPath = cmdline.getOptionValue(OUTPUT_PATH_OPTION);
    String eLang = cmdline.getOptionValue(ELANG_OPTION);
    String fLang = cmdline.getOptionValue(FLANG_OPTION);
    String samplesFile = cmdline.getOptionValue(SAMPLEDOCNOS_OPTION); 
    job.setJobName("Docnos2Titles_" + fLang + "-" + eLang);  

    FileInputFormat.addInputPaths(job, eCollectionPath);
    FileInputFormat.addInputPaths(job, fCollectionPath);
    FileOutputFormat.setOutputPath(job, new Path(titlePairsPath));
    DistributedCache.addCacheFile(new URI(pwsimOutputPath), job);
    DistributedCache.addCacheFile(new URI(samplesFile), job);
    job.set("eLang", eLang);
    job.set("fLang", fLang);
    job.set("PwsimPairs", pwsimOutputPath);
    job.set("Ivory.SampleFile", samplesFile);

    job.setInt("mapred.task.timeout", 60000000);
    job.set("mapreduce.map.memory.mb", "3000");
    job.set("mapreduce.map.java.opts", "-Xmx3000m");
    job.setBoolean("mapred.map.tasks.speculative.execution", false);
    job.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    job.setNumMapTasks(100);
    job.setNumReduceTasks(1);
    job.setInt("mapred.min.split.size", 2000000000);
    job.setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(PairOfIntString.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    sLogger.info("Running job " + job.getJobName() + "...");
    sLogger.info("E-collection path: " + eCollectionPath);
    sLogger.info("F-collection path: " + fCollectionPath);
    sLogger.info("Pwsim output path: " + pwsimOutputPath);
    sLogger.info("Output path: " + titlePairsPath);
    sLogger.info("Sample file?: " + ((samplesFile != null) ? samplesFile : "none"));

    long startTime = System.currentTimeMillis();
    JobClient.runJob(job);
    System.out.println("Job finished in " 
        + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  private static final String FCOLLECTION_OPTION = "f_collection";
  private static final String ECOLLECTION_OPTION = "e_collection";
  private static final String FLANG_OPTION = "f_lang";
  private static final String ELANG_OPTION = "e_lang";
  private static final String PWSIM_OPTION = "pwsim_output";
  private static final String OUTPUT_PATH_OPTION = "output";
  private static final String SAMPLEDOCNOS_OPTION = "docnos";
  private static final String LIBJARS_OPTION = "libjars";

  @SuppressWarnings("static-access")
  private CommandLine parseArgs(String[] args) throws Exception {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("path to output of pwsim algorithm").withArgName("path").hasArg().isRequired().create(PWSIM_OPTION));
    options.addOption(OptionBuilder.withDescription("path to output").withArgName("path").hasArg().isRequired().create(OUTPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("source-side raw collection path").withArgName("path").hasArg().isRequired().create(FCOLLECTION_OPTION));
    options.addOption(OptionBuilder.withDescription("target-side raw collection path").withArgName("path").hasArg().isRequired().create(ECOLLECTION_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter code for f-language").withArgName("en|de|tr|cs|zh|ar|es").hasArg().isRequired().create(FLANG_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter code for e-language").withArgName("en|de|tr|cs|zh|ar|es").hasArg().isRequired().create(ELANG_OPTION));
    options.addOption(OptionBuilder.withDescription("only keep pairs that match these docnos").withArgName("path to sample docnos file").hasArg().create(SAMPLEDOCNOS_OPTION));
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

  /**
   * Dispatches command-line arguments to the tool via the
   * <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Docnos2Titles(), args);
    System.exit(res);
  }

}