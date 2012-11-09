package edu.stanford.nlp.ie.crf;

import edu.stanford.nlp.optimization.CmdEvaluator;
import edu.stanford.nlp.sequences.DocumentReaderAndWriter;
import edu.stanford.nlp.stats.MultiClassChunkEvalStats;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.Pair;

import java.io.*;
import java.util.Collection;
import java.util.List;

/**
 * Evaluates CRFClassifier on a set of data
 * - called by QNMinimizer periodically 
 * - If evalCmd is set, runs command line specified by evalCmd
 *                      otherwise does evaluation internally
 *   NOTE: when running conlleval with exec on Linux, linux will first
 *          fork process by duplicating memory of current process.  So if
 *          JVM has lots of memory, it will all be duplicated when
 *          child process is initially forked.
 * @author Angel Chang
 */
public class CRFClassifierEvaluator<IN extends CoreMap> extends CmdEvaluator {
  private CRFClassifier<IN> classifier;
  private CRFLogConditionalObjectiveFunction func;
  // NOTE: Defalt uses -r, specify without -r if IOB
  private String cmdStr = "/u/nlp/bin/conlleval -r";
  private String[] cmd;

  // TODO: Use data structure to hold data + features
  // Cache already featurized documents
  // Original object bank
  Collection<List<IN>> data;
  // Featurized data
  List<Pair<int[][][], int[]>> featurizedData;
  
  public CRFClassifierEvaluator(String description,
                                CRFClassifier<IN> classifier,
                                CRFLogConditionalObjectiveFunction func,
                                Collection<List<IN>> data,
                                List<Pair<int[][][], int[]>> featurizedData)
  {
    this.description = description;
    this.classifier = classifier;
    this.func = func;
    this.data = data;
    this.featurizedData = featurizedData;
    cmd = getCmd(cmdStr);
  }

  public CRFClassifierEvaluator(String description,
                                CRFClassifier<IN> classifier)
  {
    this.description = description;
    this.classifier = classifier;
  }
  
  /**
   * Set helper function
   */
  public void setHelperFunction(CRFLogConditionalObjectiveFunction func)
  {
    this.func = func;
  }

  /**
   * Set the data to test on
   */
  public void setTestData(Collection<List<IN>> data, List<Pair<int[][][], int[]>> featurizedData)
  {
    this.data = data;
    this.featurizedData = featurizedData;
  }

  /**
   * Set the evaluation command (set to null to skip evaluation using command line)
   * @param evalCmd
   */
  public void setEvalCmd(String evalCmd)
  {
    this.cmdStr = evalCmd;
    if (cmdStr != null) {
      cmdStr = cmdStr.trim();
      if (cmdStr.length() == 0) { cmdStr = null; }
    }
    cmd = getCmd(cmdStr);
  }

  public void setValues(double[] x)
  {
    // TODO: Avoid this conversion of weights from 1D to 2D and usage of the
    //       CRFLogConditionalObjectiveFunction
    // (unnecessary and expensive if weights are large vectors - like say 100 million)
    classifier.weights = func.to2D(x);
  }

  public String[] getCmd()
  {
    return cmd;
  }

  public void outputToCmd(OutputStream outputStream)
  {
    try {
      classifier.classifyAndWriteAnswers(data, featurizedData, outputStream,
                                         classifier.makeReaderAndWriter());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public double evaluate(double[] x) {
    double score = 0;
    setValues(x);
    if (getCmd() != null) {
      evaluateCmd(getCmd());
    } else {
      try {
        // TODO: Classify in memory instead of writing to tmp file
        File f = File.createTempFile("CRFClassifierEvaluator","txt");
        f.deleteOnExit();
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(f));
        classifier.classifyAndWriteAnswers(data, featurizedData, outputStream,
                                           classifier.makeReaderAndWriter());
        outputStream.close();
        BufferedReader br = new BufferedReader(new FileReader(f));
        MultiClassChunkEvalStats stats = new MultiClassChunkEvalStats("O");
        score = stats.score(br, "\t");
        System.err.println(stats.getConllEvalString());
        f.delete();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return score;
  }

}
