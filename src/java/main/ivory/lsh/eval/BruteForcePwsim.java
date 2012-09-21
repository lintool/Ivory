package ivory.lsh.eval;

import ivory.core.data.document.WeightedIntDocVector;
import ivory.core.util.CLIRUtils;
import ivory.lsh.data.Signature;

import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritables;

/**
 * A class to extract the similarity list of each sample document, either by performing dot product
 * between the doc vectors or finding hamming distance between signatures.
 * 
 * @author ferhanture
 * 
 */
@SuppressWarnings("deprecation")
public class BruteForcePwsim extends Configured implements Tool {
  public static final String[] RequiredParameters = {};
  private static final Logger sLogger = Logger.getLogger(BruteForcePwsim.class);

  static enum Pairs {
    Total, Emitted, DEBUG, DEBUG2, Total2
  };

  static enum Sample {
    Size
  };

  private static int printUsage() {
    System.out.println("usage: [type = signature|termdocvector|intdocvector] [input-path] [output-path] [sample-path] [threshold] [num-results = -1 for all]");
    return -1;
  }

  public BruteForcePwsim() {
    super();
  }

  /**
   * For every document in the sample, find all other docs that have cosine similarity higher than
   * some given threshold.
   * 
   * @author ferhanture
   * 
   */
  public static class MyMapperDocVectors extends MapReduceBase implements
  Mapper<IntWritable, WeightedIntDocVector, IntWritable, PairOfFloatInt> {

    @SuppressWarnings("unchecked")
    static List<PairOfWritables<WritableComparable, Writable>> vectors;
    float threshold;

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);
      threshold = job.getFloat("Ivory.CosineThreshold", -1);
      sLogger.info("Threshold = " + threshold);

      // read doc ids of sample into vectors
      Path[] localFiles = null;
      try {
        localFiles = DistributedCache.getLocalCacheFiles(job);
        vectors = SequenceFileUtils.readFile(localFiles[0], FileSystem.getLocal(job));
      } catch (Exception e) {
        throw new RuntimeException("Error reading doc vectors from "
            + (localFiles != null ? localFiles[0] : "null"));
      }
      sLogger.info("Read " + vectors.size() + " sample doc vectors");
    }

    public void map(IntWritable docno, WeightedIntDocVector docvector,
        OutputCollector<IntWritable, PairOfFloatInt> output, Reporter reporter) throws IOException {
      for (int i = 0; i < vectors.size(); i++) {
        IntWritable sampleDocno = (IntWritable) vectors.get(i).getLeftElement();

        WeightedIntDocVector fromSample = (WeightedIntDocVector) vectors.get(i).getRightElement();
        float cs = CLIRUtils.cosine(docvector.getWeightedTerms(), fromSample.getWeightedTerms());

        if (cs >= threshold) {
          output.collect(new IntWritable(sampleDocno.get()), new PairOfFloatInt(cs, docno.get()));
        }
      }
    }
  }

  /**
   * For every document in the sample, find all other docs that have cosine similarity higher than
   * some given threshold.
   * 
   * @author ferhanture
   * 
   */
  public static class MyMapperTermDocVectors extends MapReduceBase implements
  Mapper<IntWritable, HMapSFW, IntWritable, PairOfFloatInt> {

    @SuppressWarnings("unchecked")
    static List<PairOfWritables<WritableComparable, Writable>> vectors;
    float threshold;

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);
      threshold = job.getFloat("Ivory.CosineThreshold", -1);
      sLogger.info("Threshold = " + threshold);

      // read doc ids of sample into vectors
      Path[] localFiles = null;
      try {
        localFiles = DistributedCache.getLocalCacheFiles(job);
        vectors = SequenceFileUtils.readFile(localFiles[0], FileSystem.getLocal(job));
      } catch (Exception e) {
        throw new RuntimeException("Error reading doc vectors from "
            + (localFiles != null ? localFiles[0] : "null"));
      }
      sLogger.info("Read " + vectors.size() + " sample doc vectors");
    }

    public void map(IntWritable docno, HMapSFW docvector,
        OutputCollector<IntWritable, PairOfFloatInt> output, Reporter reporter) throws IOException {
      for (int i = 0; i < vectors.size(); i++) {
        reporter.incrCounter(Pairs.Total, 1);
        IntWritable sampleDocno = (IntWritable) vectors.get(i).getLeftElement();
        HMapSFW fromSample = (HMapSFW) vectors.get(i).getRightElement();

        float cs = CLIRUtils.cosine(docvector, fromSample);       
        if (cs >= threshold) {
          sLogger.debug(sampleDocno + "," + fromSample + "\n" + fromSample.length());
          sLogger.debug(docno + "," + docvector + "\n" + docvector.length());
          sLogger.debug(cs);
          reporter.incrCounter(Pairs.Emitted, 1);
          output.collect(new IntWritable(sampleDocno.get()), new PairOfFloatInt(cs, docno.get()));
        }
      }
    }
  }

  /**
   * For every document in the sample, find all other docs that are closer than some given hamming
   * distance.
   * 
   * @author ferhanture
   * 
   */
  public static class MyMapperSignature extends MapReduceBase implements
  Mapper<IntWritable, Signature, IntWritable, PairOfFloatInt> {

    @SuppressWarnings("unchecked")
    static List<PairOfWritables<WritableComparable, Writable>> signatures;
    int maxDist;

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);
      maxDist = (int) job.getFloat("Ivory.MaxHammingDistance", -1);
      sLogger.info("Threshold = " + maxDist);

      // read doc ids of sample into vectors
      try {
        Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
        signatures = SequenceFileUtils.readFile(localFiles[0], FileSystem.getLocal(job));
      } catch (Exception e) {
        throw new RuntimeException("Error reading sample signatures!");
      }
      sLogger.info(signatures.size());
    }

    public void map(IntWritable docno, Signature signature,
        OutputCollector<IntWritable, PairOfFloatInt> output, Reporter reporter) throws IOException {
      for (int i = 0; i < signatures.size(); i++) {
        reporter.incrCounter(Pairs.Total, 1);
        IntWritable sampleDocno = (IntWritable) signatures.get(i).getLeftElement();
        Signature fromSample = (Signature) signatures.get(i).getRightElement();
        int dist = signature.hammingDistance(fromSample, maxDist);
        // if((sampleDocno.get()==1000009022 && docno.get()==189034) ||
        // (sampleDocno.get()==1000170828 && docno.get()==2898431)){
        // reporter.incrCounter(Pairs.DEBUG, 1);
        // sLogger.info(sampleDocno.get()+","+docno.get()+"="+dist);
        // sLogger.info(fromSample);
        // sLogger.info(signature);
        // }else{
        // continue;
        // }

        if (dist <= maxDist) {
          output
          .collect(new IntWritable(sampleDocno.get()), new PairOfFloatInt(-dist, docno.get()));
          reporter.incrCounter(Pairs.Emitted, 1);
        }
      }
    }
  }

  /**
   * This reducer reduces the number of pairs per sample document to a given number
   * (Ivory.NumResults).
   * 
   * @author ferhanture
   * 
   */
  public static class MyReducer extends MapReduceBase implements
  Reducer<IntWritable, PairOfFloatInt, PairOfInts, Text> {
    int numResults;
    TreeSet<PairOfFloatInt> list = new TreeSet<PairOfFloatInt>();
    PairOfInts keyOut = new PairOfInts();
    Text valOut = new Text();
    NumberFormat nf;

    public void configure(JobConf conf) {
      sLogger.setLevel(Level.INFO);
      numResults = conf.getInt("Ivory.NumResults", Integer.MAX_VALUE);
      nf = NumberFormat.getInstance();
      nf.setMaximumFractionDigits(3);
      nf.setMinimumFractionDigits(3);
    }

    public void reduce(IntWritable key, Iterator<PairOfFloatInt> values,
        OutputCollector<PairOfInts, Text> output, Reporter reporter) throws IOException {
      list.clear();
      while (values.hasNext()) {
        PairOfFloatInt p = values.next();
        if (!list.add(new PairOfFloatInt(p.getLeftElement(), p.getRightElement()))) {
          sLogger.debug("Not added: " + p);
        } else {
          sLogger.debug("Added: " + p);
        }
        reporter.incrCounter(Pairs.Total, 1);
      }
      sLogger.debug(list.size());
      int cntr = 0;
      while (!list.isEmpty() && cntr < numResults) {
        PairOfFloatInt pair = list.pollLast();
        sLogger.debug("output " + cntr + "=" + pair);

        keyOut.set(pair.getRightElement(), key.get()); // first english docno, then foreign language
        // docno
        valOut.set(nf.format(pair.getLeftElement()));
        output.collect(keyOut, valOut);
        cntr++;
      }
    }

  }

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public int run(String[] args) throws Exception {
    if (args.length != 6) {
      return printUsage();
    }

    float threshold = -1;
    threshold = Float.parseFloat(args[4]);
    int numResults = Integer.parseInt(args[5]);
    JobConf job = new JobConf(getConf(), BruteForcePwsim.class);

    FileSystem fs = FileSystem.get(job);

    Path inputPath = new Path(args[1]); // PwsimEnvironment.getFileNameWithPars(dir, "IntDocs");
    Path outputPath = new Path(args[2]); // dir + "/real-cosine-pairs_"+THRESHOLD+"_all";

    fs.delete(outputPath, true);

    int numMappers = 100;
    int numReducers = 1;

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    FileOutputFormat.setCompressOutput(job, false);

    DistributedCache.addCacheFile(new URI(args[3]), job); // sample doc vectors or signatures in
    // file

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.map.max.attempts", 10);
    job.setInt("mapred.reduce.max.attempts", 10);
    job.setInt("mapred.task.timeout", 6000000);

    job.setNumMapTasks(numMappers);
    job.setNumReduceTasks(numReducers);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PairOfFloatInt.class);
    job.setOutputKeyClass(PairOfInts.class);
    job.setOutputValueClass(FloatWritable.class);

    int numBits = (getConf().getInt("Ivory.NumOfBits", -1) == -1 ? 0 : getConf().getInt(
        "Ivory.NumOfBits", -1));
    if (args[0].contains("signature")) {
      job.setJobName("BruteForcePwsim_signature_D=" + numBits + "_" + threshold + "_"
          + (numResults > 0 ? numResults : "all"));
      job.setMapperClass(MyMapperSignature.class);
      job.setFloat("Ivory.MaxHammingDistance", threshold);
    } else if (args[0].contains("vector")) {
      if (args[0].contains("term")) {
        job.setMapperClass(MyMapperTermDocVectors.class);
        job.setJobName("BruteForcePwsim_termdocvector_D=" + numBits + "_" + threshold + "_"
            + (numResults > 0 ? numResults : "all"));
      } else {
        job.setMapperClass(MyMapperDocVectors.class);
        job.setJobName("BruteForcePwsim_intdocvector_D=" + numBits + "_" + threshold + "_"
            + (numResults > 0 ? numResults : "all"));
      }
      job.setFloat("Ivory.CosineThreshold", threshold);

    }
    if (numResults > 0) {
      job.setInt("Ivory.NumResults", numResults);
    }
    job.setReducerClass(MyReducer.class);

    sLogger.info("Running job " + job.getJobName());

    JobClient.runJob(job);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new BruteForcePwsim(), args);
    return;
  }

}
