package ivory.sqe.retrieval;

import ivory.core.ConfigurationException;
import ivory.core.eval.Qrels;
import ivory.core.eval.RankedListEvaluator;
import ivory.smrf.retrieval.Accumulator;
import ivory.sqe.querygenerator.Utils;
import java.io.IOException;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import edu.umd.cloud9.collection.DocnoMapping;

public class RunQueryEngine {
  private static final Logger LOG = Logger.getLogger(RunQueryEngine.class);

  public static void main(String[] args) throws Exception {
    Configuration conf = parseArgs(args);
    if (conf == null) {
      System.exit(-1);
    }

    FileSystem fs = FileSystem.getLocal(conf);
    QueryEngine qe;

    try {
      long start = System.currentTimeMillis();
      qe = new QueryEngine(conf, fs);
      long end = System.currentTimeMillis();
      LOG.info("Initializing QueryEngine : " + ( end - start) + "ms");

      // MT-Bitext-SCFG components have no meaning when K=1, so default to non-gridsearch if K=1
      if (conf.getInt(Constants.KBest, 0) == 1 || !conf.getBoolean(Constants.GridSearch, false)) {
        LOG.info("Running the queries ...");
        start = System.currentTimeMillis();
        qe.runQueries(conf);
        end = System.currentTimeMillis();
        LOG.info("Completed in "+(end - start)+" ms");

        if (conf.get(Constants.QrelsPath) != null) {
          String setting = Utils.getSetting(conf);
          float MAP = eval(qe, conf, setting);
          LOG.info("Best = "+MAP+"\t"+1+"\t"+0);
        }

      } else {
        // do a grid search on (lambda1,lambda2)
        gridSearch(qe, conf);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void gridSearch(QueryEngine qe, Configuration conf) throws IOException {
    long start, end;
    float bestMAP=0, bestLambda1=0, bestLambda2=0;
    for (float lambda1 = 0f; lambda1 <= 1.01f; lambda1=lambda1+0.1f) {
      LOG.info("Lambda1 = "+lambda1);
      for (float lambda2 = 0f; lambda2 <= 1.01f-lambda1; lambda2=lambda2+0.1f) {
        LOG.info("Lambda2 = "+lambda2);
        conf.setFloat(Constants.MTWeight, lambda1);
        conf.setFloat(Constants.BitextWeight, lambda2);
        conf.setFloat(Constants.GrammarWeight, 1-lambda1-lambda2);

        qe.init(conf, FileSystem.get(conf));    // set three weights

        LOG.info("Running the queries ...");
        start = System.currentTimeMillis();
        qe.runQueries(conf);
        end = System.currentTimeMillis();
        LOG.info("Completed in "+(end - start)+" ms");

        String setting = Utils.getSetting(conf);

        float MAP = eval(qe, conf, setting);
        if (MAP > bestMAP) {
          bestMAP = MAP;
          bestLambda1 = lambda1;
          bestLambda2 = lambda2;
        }
      }
    }
    LOG.info("Best = "+bestMAP+"\t"+bestLambda1+"\t"+bestLambda2);    
  }

  public static Configuration parseArgs(String[] args) throws IOException {
    Configuration conf = new Configuration();
    return parseArgs(args, FileSystem.getLocal(conf), conf);
  }

  @SuppressWarnings("static-access")  
  public static Configuration parseArgs(String[] args, FileSystem fs, Configuration conf) {
    // option descriptions
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("config xml").create(Constants.ConfigXML));
    options.addOption(OptionBuilder.withArgName("mono|clir|mtN").hasArg().withDescription("query type").create(Constants.QueryType));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("index directory path").create(Constants.IndexPath));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("qrels file").create(Constants.QrelsPath));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("queries xml file").create(Constants.QueriesPath));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("tokenizer model directory path").create(Constants.QueryTokenizerData));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("tokenizer model directory path").create(Constants.DocTokenizerData));
    options.addOption(OptionBuilder.withArgName("en|zh|de").hasArg().withDescription("two-letter language code").create(Constants.DocLanguage));
    options.addOption(OptionBuilder.withArgName("en|zh|de").hasArg().withDescription("two-letter language code").create(Constants.QueryLanguage));
    options.addOption(OptionBuilder.withArgName("on|off").hasArg().withDescription("turn on/off bigram segmentation (default = off)").create(Constants.BigramSegment));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("doc-language vocabulary file").create(Constants.DocVocab));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("query-language vocabulary file").create(Constants.QueryVocab));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("query-lang -> doc-lang translation prob. table").create(Constants.f2eProbsPath));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("grammar file").create(Constants.GrammarPath));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output file").create(Constants.OutputPath));
    options.addOption(OptionBuilder.withArgName("0=unigram|1|2").hasArg().withDescription("min phrase size").create(Constants.MinWindow));
    options.addOption(OptionBuilder.withArgName("0=unigram|1|2").hasArg().withDescription("max phrase size").create(Constants.MaxWindow));
    options.addOption(OptionBuilder.withArgName("(0.0-1.0)").hasArg().withDescription("lexical probability threshold").create(Constants.LexicalProbThreshold));
    options.addOption(OptionBuilder.withArgName("(0.0-1.0)").hasArg().withDescription("cumulative probability limit").create(Constants.CumulativeProbThreshold));
    options.addOption(OptionBuilder.withArgName("1|5|10").hasArg().withDescription("number of best query translations").create(Constants.KBest));
    options.addOption(OptionBuilder.withArgName("").hasArg().withDescription("max number of translations per token to keep").create(Constants.NumTransPerToken));
    options.addOption(OptionBuilder.withArgName("(0.0-1.0)").hasArg().withDescription("weight of mt output when combining with other models").create(Constants.MTWeight));
    options.addOption(OptionBuilder.withArgName("(0.0-1.0)").hasArg().withDescription("weight of phrase translations when combining with other models").create(Constants.GrammarWeight));
    options.addOption(OptionBuilder.withArgName("(0.0-1.0)").hasArg().withDescription("weight of word translations when combining with other models").create(Constants.BitextWeight));
    options.addOption(OptionBuilder.withArgName("(0.0-1.0)").hasArg().withDescription("weight of token translations in query representation").create(Constants.TokenWeight));
    options.addOption(OptionBuilder.withArgName("(0.0-1.0)").hasArg().withDescription("weight of phrase translations in query representation").create(Constants.PhraseWeight)); 
    options.addOption(OptionBuilder.withArgName("off|on").withDescription("filter bilingual translation pairs that do not appear in grammar").create(Constants.Heuristic6));
    options.addOption(OptionBuilder.withArgName("0=one-to-none,1=one-to-one,2=one-to-many").hasArg().withDescription("three options for 1-to-many alignments").create(Constants.One2Many));
    options.addOption(OptionBuilder.withArgName("off|on").hasArg().withDescription("scale counts of source tokens that translate into multiple target tokens (i.e., fertility)").create(Constants.Scaling));  
    options.addOption(OptionBuilder.withArgName("0.0-1.0").hasArg().withDescription("paramater to discount the difference between likelihood of each k-best translation").create(Constants.Alpha));  
    options.addOption(OptionBuilder.withArgName("string").hasArg().withDescription("name of CLIR run").create(Constants.RunName));
    options.addOption(OptionBuilder.withDescription("run grid search on parameters").create(Constants.GridSearch));
    options.addOption(OptionBuilder.withArgName("ivory|indri").hasArg().withDescription("print translated query in specified format (no retrieval)").create(Constants.TranslateOnly));
    options.addOption(OptionBuilder.withDescription("do not print log info").create(Constants.Quiet));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("one stopword per line, query lang").create(Constants.StopwordListQ));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("one stemmed stopword per line, query lang").create(Constants.StemmedStopwordListQ));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("one stopword per line, doc lang").create(Constants.StopwordListD));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("one stemmed stopword per line, doc lang").create(Constants.StemmedStopwordListD));
    options.addOption(OptionBuilder.withDescription("stem query text").create(Constants.IsStemming));
    options.addOption(OptionBuilder.withDescription("use if documents were stemmed").create(Constants.IsDocStemmed));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("unknown words output by Moses -output-unknowns option").create(Constants.UNKFile));

    // read options from commandline or XML
    try {
      CommandLineParser parser = new GnuParser();
      CommandLine cmdline = parser.parse(options, args);
      if (cmdline.hasOption(Constants.ConfigXML)) {
        readXMLOptions(cmdline, fs, conf);
      } else{
        conf.set(Constants.QueryType, cmdline.getOptionValue(Constants.QueryType));
        conf.set(Constants.IndexPath, cmdline.getOptionValue(Constants.IndexPath));
        conf.set(Constants.QueriesPath, cmdline.getOptionValue(Constants.QueriesPath));
        conf.set(Constants.DocLanguage, cmdline.getOptionValue(Constants.DocLanguage));
        conf.set(Constants.QueryLanguage, cmdline.getOptionValue(Constants.QueryLanguage));
        conf.set(Constants.DocTokenizerData, cmdline.getOptionValue(Constants.DocTokenizerData));
        conf.set(Constants.QueryTokenizerData, cmdline.getOptionValue(Constants.QueryTokenizerData));
      }
      if (cmdline.hasOption(Constants.QrelsPath)) {
        conf.set(Constants.QrelsPath, cmdline.getOptionValue(Constants.QrelsPath));
      }
      if (cmdline.hasOption(Constants.BigramSegment)) {
        conf.set(Constants.BigramSegment, cmdline.getOptionValue(Constants.BigramSegment));
      } else {
        conf.set(Constants.BigramSegment, "off");   //default
      }       
      if (cmdline.hasOption(Constants.GrammarPath)) {
        conf.set(Constants.GrammarPath, cmdline.getOptionValue(Constants.GrammarPath));
      }  
      if (cmdline.hasOption(Constants.OutputPath)) {
        conf.set(Constants.OutputPath, cmdline.getOptionValue(Constants.OutputPath));
      }    
      if (cmdline.hasOption(Constants.f2eProbsPath) && cmdline.hasOption(Constants.QueryVocab) && cmdline.hasOption(Constants.DocVocab)) {
        conf.set(Constants.f2eProbsPath, cmdline.getOptionValue(Constants.f2eProbsPath));
        conf.set(Constants.QueryVocab, cmdline.getOptionValue(Constants.QueryVocab));
        conf.set(Constants.DocVocab, cmdline.getOptionValue(Constants.DocVocab));
      }     
      if (cmdline.hasOption(Constants.LexicalProbThreshold)) {
        conf.setFloat(Constants.LexicalProbThreshold, Float.parseFloat(cmdline.getOptionValue(Constants.LexicalProbThreshold)));
      }
      if (cmdline.hasOption(Constants.CumulativeProbThreshold)) {
        conf.setFloat(Constants.CumulativeProbThreshold, Float.parseFloat(cmdline.getOptionValue(Constants.CumulativeProbThreshold)));
      }
      if (cmdline.hasOption(Constants.TokenWeight)) {
        conf.setFloat(Constants.TokenWeight, Float.parseFloat(cmdline.getOptionValue(Constants.TokenWeight)));
      }
      if (cmdline.hasOption(Constants.PhraseWeight)) {
        conf.setFloat(Constants.PhraseWeight, Float.parseFloat(cmdline.getOptionValue(Constants.PhraseWeight)));
      }
      if (cmdline.hasOption(Constants.MTWeight)) {
        conf.setFloat(Constants.MTWeight, Float.parseFloat(cmdline.getOptionValue(Constants.MTWeight)));
      }
      if (cmdline.hasOption(Constants.BitextWeight)) {
        conf.setFloat(Constants.BitextWeight, Float.parseFloat(cmdline.getOptionValue(Constants.BitextWeight)));
      }
      if (cmdline.hasOption(Constants.GrammarWeight)) {
        conf.setFloat(Constants.GrammarWeight, Float.parseFloat(cmdline.getOptionValue(Constants.GrammarWeight)));
      }
      if (cmdline.hasOption(Constants.KBest)) {
        conf.setInt(Constants.KBest, Integer.parseInt(cmdline.getOptionValue(Constants.KBest)));
      }
      if (cmdline.hasOption(Constants.NumTransPerToken)) {
        conf.setInt(Constants.NumTransPerToken, Integer.parseInt(cmdline.getOptionValue(Constants.NumTransPerToken)));
      }
      if (cmdline.hasOption(Constants.MinWindow) && cmdline.hasOption(Constants.MaxWindow)) {
        conf.setInt(Constants.MinWindow, Integer.parseInt(cmdline.getOptionValue(Constants.MinWindow)));
        conf.setInt(Constants.MaxWindow, Integer.parseInt(cmdline.getOptionValue(Constants.MaxWindow)));
      }
      if (cmdline.hasOption(Constants.Heuristic6)) {
        conf.set(Constants.Heuristic6, cmdline.getOptionValue(Constants.Heuristic6));
      }
      if (cmdline.hasOption(Constants.One2Many)) {
        conf.setInt(Constants.One2Many, Integer.parseInt(cmdline.getOptionValue(Constants.One2Many)));
      }
      if (cmdline.hasOption(Constants.Scaling)) {
        conf.setBoolean(Constants.Scaling, true);
      }
      if (cmdline.hasOption(Constants.Alpha)) {
        conf.setFloat(Constants.Alpha , Float.parseFloat(cmdline.getOptionValue(Constants.Alpha)));
      }
      if (cmdline.hasOption(Constants.RunName)) {
        conf.set(Constants.RunName , cmdline.getOptionValue(Constants.RunName));
      }
      if (cmdline.hasOption(Constants.GridSearch)) {
        conf.setBoolean(Constants.GridSearch, true);
      }
      if (cmdline.hasOption(Constants.TranslateOnly)) {
        conf.set(Constants.TranslateOnly, cmdline.getOptionValue(Constants.TranslateOnly));
      }
      if (cmdline.hasOption(Constants.Quiet)) {
        conf.setBoolean(Constants.Quiet, true);
      }
      if (cmdline.hasOption(Constants.StopwordListD)) {
        conf.set(Constants.StopwordListD , cmdline.getOptionValue(Constants.StopwordListD));
      }
      if (cmdline.hasOption(Constants.StemmedStopwordListD)) {
        conf.set(Constants.StemmedStopwordListD , cmdline.getOptionValue(Constants.StemmedStopwordListD));
      }
      if (cmdline.hasOption(Constants.StopwordListQ)) {
        conf.set(Constants.StopwordListQ , cmdline.getOptionValue(Constants.StopwordListQ));
      }
      if (cmdline.hasOption(Constants.StemmedStopwordListQ)) {
        conf.set(Constants.StemmedStopwordListQ , cmdline.getOptionValue(Constants.StemmedStopwordListQ));
      }
      if (cmdline.hasOption(Constants.IsDocStemmed)) {
        conf.setBoolean(Constants.IsDocStemmed, true);
      }
      if (cmdline.hasOption(Constants.IsStemming)) {
        conf.setBoolean(Constants.IsStemming, true);
      }
      if (cmdline.hasOption(Constants.UNKFile)) {
        conf.set(Constants.UNKFile , cmdline.getOptionValue(Constants.UNKFile));
      }
    } catch (Exception e) {
      System.err.println("Error parsing command line: " + e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("RunQueryEngine", options);
      ToolRunner.printGenericCommandUsage(System.out);
      return null;
    }
    
    return conf;

  }

  private static void readXMLOptions(CommandLine cmdline, FileSystem fs, Configuration conf) throws ConfigurationException {
    String element = cmdline.getOptionValue(Constants.ConfigXML);

    Document d = null;
    try {
      d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(fs.open(new Path(element)));
    } catch (SAXException e) {
      throw new ConfigurationException(e.getMessage());
    } catch (IOException e) {
      throw new ConfigurationException(e.getMessage());
    } catch (ParserConfigurationException e) {
      throw new ConfigurationException(e.getMessage());
    }

    if (cmdline.hasOption(Constants.QueriesPath)) {
      conf.set(Constants.QueriesPath, cmdline.getOptionValue(Constants.QueriesPath));
    }

    NodeList list = d.getElementsByTagName(Constants.QueryType);
    if (list.getLength() > 0) {  conf.set(Constants.QueryType, list.item(0).getTextContent());  }

    list = d.getElementsByTagName(Constants.QrelsPath);
    if (list.getLength() > 0) {  conf.set(Constants.QrelsPath, list.item(0).getTextContent());  }

    list = d.getElementsByTagName(Constants.RunName);
    if (list.getLength() > 0) {  conf.set(Constants.RunName, list.item(0).getTextContent());  }

    list = d.getElementsByTagName(Constants.IndexPath);
    if (list.getLength() > 0) {  conf.set(Constants.IndexPath, list.item(0).getTextContent());  }

    list = d.getElementsByTagName(Constants.DocLanguage);
    if (list.getLength() > 0) {  conf.set(Constants.DocLanguage, list.item(0).getTextContent());  }

    list = d.getElementsByTagName(Constants.QueryLanguage);
    if (list.getLength() > 0) {  conf.set(Constants.QueryLanguage, list.item(0).getTextContent());  }

    list = d.getElementsByTagName(Constants.DocTokenizerData);
    if (list.getLength() > 0) {  conf.set(Constants.DocTokenizerData, list.item(0).getTextContent());  }

    list = d.getElementsByTagName(Constants.QueryTokenizerData);
    if (list.getLength() > 0) {  conf.set(Constants.QueryTokenizerData, list.item(0).getTextContent());  }  

    list = d.getElementsByTagName(Constants.GrammarPath);
    if (list.getLength() > 0) {  conf.set(Constants.GrammarPath, list.item(0).getTextContent());  }  

    list = d.getElementsByTagName(Constants.f2eProbsPath);
    if (list.getLength() > 0) {  conf.set(Constants.f2eProbsPath, list.item(0).getTextContent());  }  

    list = d.getElementsByTagName(Constants.QueryVocab);
    if (list.getLength() > 0) {  conf.set(Constants.QueryVocab, list.item(0).getTextContent());  }  

    list = d.getElementsByTagName(Constants.DocVocab);
    if (list.getLength() > 0) {  conf.set(Constants.DocVocab, list.item(0).getTextContent());  }  

    list = d.getElementsByTagName(Constants.KBest);
    if (list.getLength() > 0) {  conf.setInt(Constants.KBest, Integer.parseInt(list.item(0).getTextContent()));  }  

    list = d.getElementsByTagName(Constants.LexicalProbThreshold);
    if (list.getLength() > 0) {  conf.setFloat(Constants.LexicalProbThreshold, Float.parseFloat(list.item(0).getTextContent()));  }  

    list = d.getElementsByTagName(Constants.CumulativeProbThreshold);
    if (list.getLength() > 0) {  conf.setFloat(Constants.CumulativeProbThreshold, Float.parseFloat(list.item(0).getTextContent()));  }  

    list = d.getElementsByTagName(Constants.TokenWeight);
    if (list.getLength() > 0) {  conf.setFloat(Constants.TokenWeight, Float.parseFloat(list.item(0).getTextContent()));  }  

    list = d.getElementsByTagName(Constants.PhraseWeight);
    if (list.getLength() > 0) {  conf.setFloat(Constants.PhraseWeight, Float.parseFloat(list.item(0).getTextContent()));  }  

    list = d.getElementsByTagName(Constants.MTWeight);
    if (list.getLength() > 0) {  conf.setFloat(Constants.MTWeight, Float.parseFloat(list.item(0).getTextContent()));  }  

    list = d.getElementsByTagName(Constants.BitextWeight);
    if (list.getLength() > 0) {  conf.setFloat(Constants.BitextWeight, Float.parseFloat(list.item(0).getTextContent()));  }  

    list = d.getElementsByTagName(Constants.GrammarWeight);
    if (list.getLength() > 0) {  conf.setFloat(Constants.GrammarWeight, Float.parseFloat(list.item(0).getTextContent()));  }  

    list = d.getElementsByTagName(Constants.Quiet);
    if (list.getLength() > 0) {  conf.setBoolean(Constants.Quiet, Boolean.parseBoolean(list.item(0).getTextContent()));  }  

    list = d.getElementsByTagName(Constants.MinWindow);
    if (list.getLength() > 0) {  conf.setInt(Constants.MinWindow, Integer.parseInt(list.item(0).getTextContent()));  } 

    list = d.getElementsByTagName(Constants.MaxWindow);
    if (list.getLength() > 0) {  conf.setInt(Constants.MaxWindow, Integer.parseInt(list.item(0).getTextContent()));  }

    list = d.getElementsByTagName(Constants.NumTransPerToken);
    if (list.getLength() > 0) {  conf.setInt(Constants.NumTransPerToken, Integer.parseInt(list.item(0).getTextContent()));  }

    list = d.getElementsByTagName(Constants.StopwordListD);
    if (list.getLength() > 0) {  conf.set(Constants.StopwordListD, list.item(0).getTextContent());  }  

    list = d.getElementsByTagName(Constants.StemmedStopwordListD);
    if (list.getLength() > 0) {  conf.set(Constants.StemmedStopwordListD, list.item(0).getTextContent());  }  

    list = d.getElementsByTagName(Constants.StopwordListQ);
    if (list.getLength() > 0) {  conf.set(Constants.StopwordListQ, list.item(0).getTextContent());  }  

    list = d.getElementsByTagName(Constants.StemmedStopwordListQ);
    if (list.getLength() > 0) {  conf.set(Constants.StemmedStopwordListQ, list.item(0).getTextContent());  }  

    list = d.getElementsByTagName(Constants.IsDocStemmed);
    if (list.getLength() > 0) {  conf.setBoolean(Constants.IsDocStemmed, true);  }

    list = d.getElementsByTagName(Constants.IsStemming);
    if (list.getLength() > 0) {  conf.setBoolean(Constants.IsStemming, true);  }

    list = d.getElementsByTagName(Constants.OutputPath);
    if (list.getLength() > 0) {  conf.set(Constants.OutputPath, list.item(0).getTextContent());  }
 
    list = d.getElementsByTagName(Constants.UNKFile);
    if (list.getLength() > 0) {  conf.set(Constants.UNKFile, list.item(0).getTextContent());  }  
 
    list = d.getElementsByTagName(Constants.TranslateOnly);
    if (list.getLength() > 0) {  conf.set(Constants.TranslateOnly, list.item(0).getTextContent());  }  

  }

  static float eval(QueryEngine qe, Configuration conf, String setting){
    Qrels qrels = new Qrels(conf.get(Constants.QrelsPath));
    DocnoMapping mapping = qe.getDocnoMapping();
    float apSum = 0, p10Sum = 0;
    Map<String, Accumulator[]> results = qe.getResults();
    for (String qid : results.keySet()) {
      float ap = (float) RankedListEvaluator.computeAP(results.get(qid), mapping,
          qrels.getReldocsForQid(qid));

      float p10 = (float) RankedListEvaluator.computePN(10, results.get(qid), mapping,
          qrels.getReldocsForQid(qid));
      LOG.info("<AP>:::"+setting+":::"+qid+":::"+results.get(qid).length+":::"+qrels.getReldocsForQid(qid).size()+":::"+ap+":::"+p10);
      apSum += ap;
      p10Sum += p10;
    }
    conf.setFloat("AP", apSum);
    conf.setFloat("P10", p10Sum);
    float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / results.size());
    float P10Avg = (float) RankedListEvaluator.roundTo4SigFigs(p10Sum / results.size());
    LOG.info("<MAP>:::"+setting+":::"+MAP+":::"+P10Avg+"\nNumber of queries = "+results.size());

    return MAP;
  }
}
