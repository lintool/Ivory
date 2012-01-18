package ivory.core.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class ConvertRawModelIntoParametersFile {

  public static void convert(String index, String modelFilePath/* , String collectionName *//*
                                                                                             * ,
                                                                                             * String
                                                                                             * runTag
                                                                                             */)
      throws IOException {
    File f = new File(modelFilePath);
    BufferedReader reader = new BufferedReader(new FileReader(f));
    String line = reader.readLine();
    System.out.println(line);
    reader.close();
    modelFilePath = f.getName();
    // String line =
    // " Model: [ feat-bm25-term:0.7823712006704489 feat-lm-x-sameBinWt:0.11663811167148008 feat-bm25-x-orderedWt-5:0.10099068765807111 ]";
    line = line.substring(line.indexOf('[') + 1);
    line = line.substring(0, line.indexOf(']'));
    line = line.trim();
    String[] params = line.split(" ");
    String cliqueSet, potential, generator, scoreFunction, model, outputPath;
    potential = "potential=\"ivory.smrf.model.potential.QueryPotential\"";
    String collectionName = modelFilePath.substring(6);
    collectionName = collectionName.substring(0, collectionName.indexOf('.'));
    // model.wt10g.approx.fixed-list-64
    String runTag = modelFilePath.substring(modelFilePath.indexOf("approx.") + 7);
    runTag = runTag.substring(0, runTag.length() - 4);
    outputPath = "run." + collectionName + ".approx." + runTag + ".xml";
    FileWriter writer = new FileWriter(outputPath);
    model = "<model id=\"" + collectionName + "-sd-approx-" + runTag
        + "\" type=\"Feature\" output=\"ranking." + collectionName + "-sd-approx-" + runTag
        + ".txt\" hits=\"1000\">";

    // System.out.println("<parameters>");
    writer.write("<parameters>" + "\n");

    // System.out.println("\t<index>"+index+"</index>");
    writer.write("\t<index>" + index + "</index>" + "\n");

    // System.out.println("\t"+model);
    writer.write("\t" + model + "\n");

    for (String pair : params) {
      cliqueSet = "cliqueSet=\"ivory.smrf.model.builder.";
      generator = "generator=\"ivory.smrf.model.builder.";
      scoreFunction = "scoreFunction=\"ivory.smrf.model.score.";

      int p = pair.indexOf(':');
      String feature = pair.substring(5, p);
      String wt = pair.substring(p + 1);

      String fn = feature.substring(0, feature.indexOf('-'));
      feature = feature.substring(feature.indexOf('-') + 1);
      String width = "width=\"";
      // System.out.println("\t\t<!-- "+fn+"\t"+feature+"\t"+wt+" -->");
      writer.write("\t\t<!-- " + fn + "\t" + feature + "\t" + wt + " -->" + "\n");
      if (fn.equals("lm")) {
        scoreFunction += "DirichletScoringFunction\" ";
        if (feature.equals("term")) {
          generator += "TermExpressionGenerator\"";
          cliqueSet += "TermCliqueSet\"";
          scoreFunction += "mu=\"1000.0\" />";
        } else {// non-term
          cliqueSet += "OrderedCliqueSet\" dependence=\"sequential\"";
          scoreFunction += "mu=\"750.0\" />";
          if (feature.startsWith("x-sameBinWt")) {
            generator += "SameBinApproxExpressionGenerator\" width=\"1\"";
          } else {
            width += feature.substring(feature.lastIndexOf('-') + 1) + "\"";
            if (feature.startsWith("x-orderedWt")) {
              generator += "OrderedWindowApproxExpressionGenerator\" " + width;
            } else {// Unordered
              generator += "UnorderedWindowApproxExpressionGenerator\" " + width;
            }
          }
        }
      } else {
        scoreFunction += "BM25ScoringFunction\" ";
        if (feature.equals("term")) {
          generator += "TermExpressionGenerator\"";
          cliqueSet += "TermCliqueSet\"";
          scoreFunction += "k1=\"0.5\" b=\"0.3\" />";
        } else { // non-term
          cliqueSet += "OrderedCliqueSet\" dependence=\"sequential\"";
          scoreFunction += "k1=\"0.25\" b=\"0.0\" />";
          if (feature.startsWith("x-sameBinWt")) {
            generator += "SameBinApproxExpressionGenerator\" width=\"1\"";
          }
          // x-orderedAdjBinWt
          else if (feature.startsWith("x-orderedAdjBinWt")) {
            generator += "OrderedAdjacentBinApproxExpressionGenerator\" width=\"1\"";
          } else if (feature.startsWith("x-unorderedAdjBinWt")) {
            generator += "UnorderedAdjacentBinApproxExpressionGenerator\" width=\"1\"";
          } else {
            width += feature.substring(feature.lastIndexOf('-') + 1) + "\"";
            if (feature.startsWith("x-orderedWt")) {
              generator += "OrderedWindowApproxExpressionGenerator\" " + width;
            } else {// Unordered
              generator += "UnorderedWindowApproxExpressionGenerator\" " + width;
            }
          }
        }
      }
      // System.out.println("\t\t"+"<feature id=\""+fn+"-"+feature+"\" weight=\""+wt+"\"");
      writer.write("\t\t" + "<feature id=\"" + fn + "-" + feature + "\" weight=\"" + wt + "\""
          + "\n");
      // System.out.println("\t\t\t"+cliqueSet);
      writer.write("\t\t\t" + cliqueSet + "\n");
      // System.out.println("\t\t\t"+potential);
      writer.write("\t\t\t" + potential + "\n");
      // System.out.println("\t\t\t"+generator);
      writer.write("\t\t\t" + generator + "\n");
      // System.out.println("\t\t\t"+scoreFunction);
      writer.write("\t\t\t" + scoreFunction + "\n");
    }
    // System.out.println("\t</model>");
    writer.write("\t</model>" + "\n");
    // System.out.println("</parameters>");
    writer.write("</parameters>" + "\n");
    writer.close();
  }

  public static void main(String[] args) throws IOException {
    // convert(String index, String modelFilePath, String collectionName, String runTag)
    ConvertRawModelIntoParametersFile.convert(args[0], args[1]);
  }
}
