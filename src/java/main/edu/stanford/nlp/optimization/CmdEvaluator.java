package edu.stanford.nlp.optimization;

//import edu.stanford.nlp.util.StreamGobbler;
import edu.stanford.nlp.util.SystemUtils;

import java.io.*;
import java.util.regex.Pattern;

/**
 * Runs a cmdline to evaluate a dataset (assumes cmd takes input from stdin)
 *
 * @author Angel Chang
 */
public abstract class CmdEvaluator implements Evaluator {
  private static final Pattern cmdSplitPattern = Pattern.compile("\\s+");
  private boolean saveOutput = false;
  private String outString;
  private String errString;
  protected String description;

  public abstract void setValues(double[] x);
  public abstract String[] getCmd();
  public abstract void outputToCmd(OutputStream outputStream);

  protected String[] getCmd(String cmdStr)
  {
    if (cmdStr == null) return null;
    return cmdSplitPattern.split(cmdStr);
  }

  public String getOutput() {
    return outString;
  }

  public String getError() {
    return errString;
  }

  public String toString() {
    return description;
  }

  public void evaluateCmd(String[] cmd) {
    try {
      SystemUtils.ProcessOutputStream outputStream;
      if (saveOutput) {
        StringWriter outSw = new StringWriter();
        StringWriter errSw = new StringWriter();
        outputStream = new SystemUtils.ProcessOutputStream(cmd, outSw, errSw);
        outString = outSw.toString();
        errString = errSw.toString();
      } else {
        outputStream = new SystemUtils.ProcessOutputStream(cmd, new PrintWriter(System.err));
      }
      outputToCmd(outputStream);
      outputStream.close();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  public double evaluate(double[] x) {
    setValues(x);
    evaluateCmd(getCmd());
    return 0;
  }
}