package ivory.lsh.eval;

import ivory.core.RetrievalEnvironment;

import java.io.IOException;
import java.util.HashSet;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMapping;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.io.FSLineReader;
import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.map.HMapIIW;

/**
 * A class to extract interwiki language links from a Wikipedia collection .
 * 
 * @author ferhanture
 * 
 * 
 */
@SuppressWarnings("deprecation")
public class ExtractWikipedia extends Configured implements Tool {

  public static final String[] RequiredParameters = {};
  private static final Logger sLogger = Logger.getLogger(ExtractWikipedia.class);

  protected static enum Maps {
    FOUND, NOTFOUND
  };

  static enum Count {
    INTER, DOCS, SKIPPED;
  };

  private static int printUsage() {
    System.out
        .println("usage: [en-collection-path] [en-index-path] [intermediate-output-path] [de-collection-path] [de-index-path] [output-path] [sample-docnos-path]");
    return -1;
  }

  // Given input English wikipedia pages, extract (de-title, en-docno) pairs
  static class MyMapperTitle2Docno extends MapReduceBase implements
      Mapper<IntWritable, WikipediaPage, Text, IntWritable> {
    Text valText;
    IntWritable keyInt;
    private DocnoMapping mDocMapping;
    String sampleDocnosFile;
    HMapIIW samplesMap = null;

    public void configure(JobConf job) {
      // sLogger.setLevel(Level.DEBUG);
      keyInt = new IntWritable();
      valText = new Text();

      sampleDocnosFile = job.get("SampleDocnosFile");
      if (sampleDocnosFile != null) {
        samplesMap = new HMapIIW();
        try {
          FSLineReader reader = new FSLineReader(sampleDocnosFile);
          Text t = new Text();
          while (reader.readLine(t) != 0) {
            int docno = Integer.parseInt(t.toString());
            samplesMap.put(docno, 1);
          }
          reader.close();
        } catch (IOException e1) {
        }
      }

      mDocMapping = new WikipediaDocnoMapping();
      FileSystem fs;
      try {
        fs = FileSystem.get(job);
        String indexPath = job.get("Ivory.IndexPath");
        RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

        Path mappingFile = env.getDocnoMappingData();
        mDocMapping.loadMapping(mappingFile, fs);
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    public void map(IntWritable key, WikipediaPage doc, OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
      int docno = mDocMapping.getDocno(doc.getDocid());
      if (docno < 0) {
        reporter.incrCounter(Count.SKIPPED, 1);
        return;
      }
      // docno+=1000000000;

      // English sample filtering here
      if (samplesMap != null && !samplesMap.containsKey(docno)) {
        return;
      }

      reporter.incrCounter(Count.DOCS, 1);
      String rawxml = doc.getRawXML();
      Pattern p = Pattern.compile("\\[\\[de:(.+)\\]\\]");
      Matcher m = p.matcher(rawxml);
      if (m.find()) {
        sLogger.debug(doc.getTitle());
        sLogger.debug(m.group(1));
        keyInt.set(docno);
        String titleText = m.group(1);
        valText.set(titleText.split("#")[0]);
        output.collect(valText, keyInt);
        reporter.incrCounter(Count.INTER, 1);
        sLogger.debug(rawxml);
        sLogger.debug("---------");
      }
    }
  }

  // Given input German wikipedia pages and mapping from de-title to en-docno, extract en-docno,
  // de-docno pairs.
  static class MyMapperEn2DeDocno extends MapReduceBase implements
      Mapper<IntWritable, WikipediaPage, IntWritable, IntWritable> {
    @SuppressWarnings("unchecked")
    SortedMap<WritableComparable, Writable> title2Docno;
    private DocnoMapping mDocMapping;
    String sampleDocnosFile;
    HMapIIW samplesMap = null;

    public void configure(JobConf job) {
      sLogger.setLevel(Level.DEBUG);
      title2Docno = SequenceFileUtils.readFileIntoMap(new Path(job.get("TitleDocnoFile")));
      sampleDocnosFile = job.get("SampleDocnosFile");
      if (sampleDocnosFile != null) {
        samplesMap = new HMapIIW();
        try {
          FSLineReader reader = new FSLineReader(sampleDocnosFile);
          Text t = new Text();
          while (reader.readLine(t) != 0) {
            int docno = Integer.parseInt(t.toString());
            samplesMap.put(docno, 1);
          }
          reader.close();
        } catch (IOException e1) {
        }
        sLogger.info("Loaded " + samplesMap.size() + " samples");
      } else {
        sLogger.info("No sample file read.");
      }

      mDocMapping = new WikipediaDocnoMapping();
      FileSystem fs;
      try {
        fs = FileSystem.get(job);
        String indexPath = job.get("Ivory.IndexPath");
        RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

        Path mappingFile = env.getDocnoMappingData();
        mDocMapping.loadMapping(mappingFile, fs);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public void map(IntWritable key, WikipediaPage doc,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      int docno = mDocMapping.getDocno(doc.getDocid());
      if (docno < 0) {
        reporter.incrCounter(Count.SKIPPED, 1);
        return;
      }
      docno += 1000000000;

      // German sample filtering here
      if (samplesMap != null && !samplesMap.containsKey(docno)) {
        sLogger.info(docno + " not found!");
        sLogger.info(samplesMap);
        return;
      }

      Text title = new Text(doc.getTitle());
      // if(docno%100==0){
      // sLogger.debug(docno+","+title+"\n"+doc.getContent());
      // reporter.incrCounter(Maps.ALL, 1);
      // }
      IntWritable w = (IntWritable) title2Docno.get(title);
      if (w != null) {
        reporter.incrCounter(Maps.FOUND, 1);
        // sLogger.debug(docno+","+title+"\n"+doc.getContent());
        sLogger.debug("English docno: " + w.get());
        sLogger.debug("German title: " + title);

        // output.collect(new IntWritable(docno), w);
        output.collect(w, new IntWritable(docno));

      } else {
        reporter.incrCounter(Maps.NOTFOUND, 1);
        sLogger.debug(title + " does not have language link");
      }
    }
  }

  // A Mapper that goes over the Wikipedia collection. Helpful for exploration of the collection.
  static class MyMapperGetDocs extends MapReduceBase implements
      Mapper<LongWritable, WikipediaPage, Text, Text> {
    Text valText;
    Text keyText;
    private DocnoMapping mDocMapping;
    String sampleDocnosFile;
    HMapIIW samplesMap = null;
    HashSet<Integer> germandocnos = new HashSet<Integer>();
    HashSet<Integer> englishdocnos = new HashSet<Integer>();

    public void configure(JobConf job) {
      sLogger.setLevel(Level.DEBUG);
      keyText = new Text();
      valText = new Text();

      sampleDocnosFile = job.get("SampleDocnosFile");
      if (sampleDocnosFile != null) {
        samplesMap = new HMapIIW();
        try {
          FSLineReader reader = new FSLineReader(sampleDocnosFile);
          Text t = new Text();
          while (reader.readLine(t) != 0) {
            String[] docnos = t.toString().split("\t");
            germandocnos.add(Integer.parseInt(docnos[0]));
            englishdocnos.add(Integer.parseInt(docnos[1]));
          }
          reader.close();
        } catch (IOException e1) {
        }
      }

      mDocMapping = new WikipediaDocnoMapping();
      FileSystem fs;
      try {
        fs = FileSystem.get(job);
        String indexPath = job.get("Ivory.IndexPath");
        RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

        Path mappingFile = env.getDocnoMappingData();
        mDocMapping.loadMapping(mappingFile, fs);
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    public void map(LongWritable key, WikipediaPage doc, OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
      int docno = mDocMapping.getDocno(doc.getDocid());
      if (docno < 0) {
        reporter.incrCounter(Count.SKIPPED, 1);
        return;
      }
      docno += 1000000000;

      if (!germandocnos.contains(new Integer(docno))) {
        return;
      }
      reporter.incrCounter(Count.DOCS, 1);
      String content = "\n" + doc.getContent(); // separate key and value lines in output
      valText.set(content);
      keyText.set("DOCNO " + docno);
      output.collect(keyText, valText);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 7) {
      return printUsage();
    }

    String collectionPath = args[0];
    String indexPath = args[1];
    String outputPath = args[2];

    JobConf job = new JobConf(getConf(), ExtractWikipedia.class);
    job.set("Ivory.IndexPath", indexPath);
    job.setJobName("ExtractWikipedia-Step1");

    sLogger.info("Extracting information from En-Wikipedia...");
    sLogger.info("InputPath: " + collectionPath);
    sLogger.info("Output Path: " + outputPath);
    sLogger.info("Index Path: " + indexPath);

    FileSystem.get(job).delete(new Path(outputPath), true);
    FileInputFormat.setInputPaths(job, new Path(collectionPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    FileOutputFormat.setCompressOutput(job, false);

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.map.max.attempts", 10);
    job.setInt("mapred.reduce.max.attempts", 10);
    job.setInt("mapred.task.timeout", 6000000);

    job.setNumMapTasks(100);
    job.setNumReduceTasks(1);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(MyMapperTitle2Docno.class);
    job.setReducerClass(IdentityReducer.class);

    job.setOutputFormat(SequenceFileOutputFormat.class);
    JobClient.runJob(job);

    // ////////////////////////////////

    String prevOutputPath = outputPath;
    collectionPath = args[3];
    indexPath = args[4];
    outputPath = args[5];

    job = new JobConf(getConf(), ExtractWikipedia.class);
    job.set("Ivory.IndexPath", indexPath);
    job.set("TitleDocnoFile", prevOutputPath + "/part-00000");
    job.set("SampleDocnosFile", args[6]);

    job.setJobName("ExtractWikipedia-Step2");

    sLogger.info("Extracting information from De-Wikipedia...");
    sLogger.info("InputPath: " + collectionPath);
    sLogger.info("Output Path: " + outputPath);
    sLogger.info("Index Path: " + indexPath);
    sLogger.info("Sample file: " + args[6]);

    FileInputFormat.setInputPaths(job, new Path(collectionPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    FileOutputFormat.setCompressOutput(job, false);

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.map.max.attempts", 10);
    job.setInt("mapred.reduce.max.attempts", 10);
    job.setInt("mapred.task.timeout", 6000000);

    job.setNumMapTasks(100);
    job.setNumReduceTasks(1);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(MyMapperEn2DeDocno.class);
    job.setReducerClass(IdentityReducer.class);
    // job.setOutputFormat(SequenceFileOutputFormat.class);

    JobClient.runJob(job);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ExtractWikipedia(), args);
    System.exit(res);
  }

}
