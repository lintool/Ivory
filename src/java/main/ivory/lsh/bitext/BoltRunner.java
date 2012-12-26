package ivory.lsh.bitext;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.core.util.CLIRUtils;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import opennlp.model.RealValueFileEventStream;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.InvalidFormatException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.util.array.ArrayListOfFloats;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class BoltRunner {


  /**
   * @param eQuestion
   *    english sentence or question, to be compared against foreign sentences in document
   * @param fDocFile
   *    path to document with sentences in foreign language
   * @param eSentModel 
   * @param fSentModel 
   * @param eVocabSrc 
   * @param fVocabSrc 
   * @param eVocabTrg 
   * @param fVocabTrg 
   * @param f2eTTable 
   * @param e2fTTable 
   * @param classifierModel 
   * @param expansionsFile 
   * @return
   *    an array of classifier score values, one for each foreign sentence in fDocFile
   * @throws Exception 
   */
  public static ArrayListOfFloats computeClassifierScores(String dataDir, String qaPairsFile, String eLang, String fLang, String eDir, String eTok, String fTok, String eStemmedStopword, String eStopword, String fStopword,
      String eSentModel, String fSentModel, String eVocabSrc, String fVocabSrc, String eVocabTrg, String fVocabTrg, String f2eTTable, String e2fTTable, String classifierModel, String expansionsFile) throws Exception {
    ArrayListOfFloats scores = new ArrayListOfFloats();
    Configuration conf = new Configuration();
    conf.set("eLang", eLang);
    conf.set("fLang", fLang);
    conf.set("eDir", eDir);
    conf.set("eTokenizer", eTok);
    conf.set("fTokenizer", fTok);
    conf.set("eStopword", eStopword);
    conf.set("fStopword", fStopword);
    conf.set("eSentDetectorFile", eSentModel);
    conf.set("fSentDetectorFile", fSentModel);
    conf.set("eVocabSrcFile", eVocabSrc);
    conf.set("fVocabSrcFile", fVocabSrc);
    conf.set("eVocabTrgFile", eVocabTrg);
    conf.set("fVocabTrgFile", fVocabTrg);
    conf.set("f2e_ttableFile", f2eTTable);
    conf.set("e2f_ttableFile", e2fTTable);
    conf.set("modelFileName", classifierModel);
    //      conf.set("IndexTermsData", indexTerms);
    //      conf.set("IndexTermIdsData", "en");
    //      conf.set("IndexTermIdMappingData", "en");

    PreprocessHelper helper = new PreprocessHelper(CLIRUtils.MinVectorTerms, CLIRUtils.MinSentenceLength, conf);
    Tokenizer eStemTokenizer = TokenizerFactory.createTokenizer(FileSystem.getLocal(conf), eLang, eTok, true, null, eStemmedStopword, null);

    Map<String, List<String>> expansionLists = readExpansionLists(expansionsFile);

    FileInputStream fis;
    fis = new FileInputStream(qaPairsFile);
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
    String line = null;
    Pattern regex = Pattern.compile("<post.+id=\"(.+)\"");

    while ((line = br.readLine()) != null) {       
      String[] arr = line.split("\\|\\|\\|");
      if (arr[0].trim().equals("NO")) { continue; }
      System.err.println("line="+line);
      String eQuestion = arr[2].trim();

      // if expansion list exists, translate terms and add to original question
      String expansionTranslation = "";
      if (expansionLists.containsKey(eQuestion)){
        expansionTranslation = expand(eQuestion, expansionLists.get(eQuestion), helper, eStemTokenizer);
      }
      
      HMapSIW eSrcTfs = new HMapSIW();
      // since these are stemmed already, add translaitons of expansion terms directly into term2TF map
      for (String expansionTranslatedTerm : expansionTranslation.split(" ")) {
        eSrcTfs.increment(expansionTranslatedTerm);
      }
      
      HMapSFW eVector = helper.createEDocVector(eQuestion, eSrcTfs);
      int eSentLength = eQuestion.length();
      System.err.println("question="+eQuestion+"\nexpansion="+expansionTranslation);
      //      System.err.println("evect="+eVector);
//      System.err.println("eleng="+eSentLength);

      String post = arr[5];
      System.err.println("post="+post);

      if(!arr[1].contains("arz")) { continue; }     // process only Arabic for now

      String path = dataDir + "/" + arr[1].split("-")[4] + "/" + arr[1] + ".xml"; 
      System.err.println("path="+path);

      FileInputStream fis2;
      try {
        fis2 = new FileInputStream(path);
      } catch (Exception e) {
        String path2 = dataDir + "/" + arr[1].split("-")[3] + "/" + arr[1] + ".xml"; 
        try {
          fis2 = new FileInputStream(path2);
        } catch (Exception e1) {
//          throw new RuntimeException("File not found in paths:\n" + path + " OR " + path2);
          System.err.println("Skipping file: " + path);
          continue;
        }
      }
      BufferedReader br2 = new BufferedReader(new InputStreamReader(fis2));
      String fLine = null;
      boolean isPost = false;
      int numCharsInPost = 0;
      int sentCnt = 0;
      while ((fLine = br2.readLine()) != null) {
        if (fLine.startsWith("</post")) {
          numCharsInPost = 0;
          isPost = false;
          sentCnt = 0;
        }
        if (isPost && !fLine.equals("")) {
          System.err.println(fLine);
          String[] fSents = helper.getFSentenceModel().sentDetect(fLine);

          for (String fSent : fSents) {
            sentCnt++;
            System.err.println("fsent="+fSent);
            HMapSIW term2tf = new HMapSIW();
            HMapSFW fVector = helper.createFDocVector(fSent, term2tf);
            int fSentLength = fSent.length();
//            System.err.println("fleng="+fSentLength);
            int charCnt = fSent.trim().toCharArray().length;

            if (fVector != null) {
              String[] instance = CLIRUtils.computeFeaturesF2(eSrcTfs, eVector, term2tf, fVector, 100, 100, helper.getESrc(), helper.getETrg(), helper.getFSrc(), helper.getFTrg(), helper.getE2F(), helper.getF2E());
              float[] values = RealValueFileEventStream.parseContexts(instance);
              double[] probs = helper.getClassifier().eval(instance, values);
              System.out.println(path+"|||"+eQuestion+"|||"+fSent+"|||"+sentCnt+"|||"+post+"|||"+(numCharsInPost+1)+"|||"+(numCharsInPost+charCnt)+"|||"+probs[1]);
            }else {
              //              System.out.println("score="+-1);
            }
            System.err.println(numCharsInPost+1);
            System.err.println("fvect="+fVector);
            numCharsInPost+=charCnt;
            System.err.println(numCharsInPost);
            sentCnt++;
            numCharsInPost++;   //end of line adjustment
          }
        }

        Matcher m = regex.matcher(fLine);
        if(m.find()) {
          if(m.group(1).equals(post)) {            
            isPost = true;
          }else {
            System.err.println(m.group(1));
          }
        }        

      }        // end each line in forum file
    }     // end each q-a pair

    return scores;
  }

  private static Map<String, List<String>> readExpansionLists(String expansionsFile) throws IOException {
    Map<String, List<String>> expMap = new HashMap<String, List<String>>(); 
    FileInputStream fis = new FileInputStream(expansionsFile);
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
    String line = null, query = null;
    while ((line = br.readLine()) != null) {
      if(line.contains("original")) {
        query = line.replaceAll("<original>", "").replaceAll("</original>", "").trim();
        System.err.println("Created entry for query: "+ query);
        expMap.put(query, new ArrayList<String>());
      }else {
        expMap.get(query).add(line.trim());
      }
    }
    return expMap;
  }

  private static String expand(String eQuestion, List<String> expansionTerms, PreprocessHelper helper, Tokenizer eStemTokenizer) {
    String expandedStr = "";
    Vocab fVocabSrc = helper.getFSrc();
    Vocab eVocabTrg = helper.getETrg();
    Tokenizer fTokenizer = helper.getFTokenizer();
    TTable_monolithic_IFAs f2e_Probs = helper.getF2E();
    for (String expansionTerm : expansionTerms){
      expansionTerm = fTokenizer.stem(expansionTerm);
      int f = fVocabSrc.get(expansionTerm);
      if (f > 0) {
        int[] eS = f2e_Probs.get(f).getTranslations(0.0f);

        for(int e : eS){
          String eTerm = eVocabTrg.get(e);
          if (!eStemTokenizer.isStemmedStopWord(eTerm)) {
            expandedStr += " " + eTerm;
            System.err.println("added expansion:"+ eTerm); 
          }else {
            System.err.println("e-stopword:"+ eTerm);             
          }
        }
      }else {
        System.err.println("OOV:"+ expansionTerm);
      }
    }

    return expandedStr;
  }

  public static void main(String[] args) {
    // String eLang, String fLang, String eDir, String eTok, String fTok, 
    // String eSentModel, String fSentModel, String eVocabSrc, String fVocabSrc, String eVocabTrg, 
    // String fVocabTrg, String f2eTTable, String e2fTTable, String classifierModel
    try {
      computeClassifierScores("/fs/clip-qa/ferhan/bolt.qa/data.arz", args[0], //"/fs/clip-qa/ferhan/bolt.qa/doc-q-a.pairs", 
          args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], 
          args[10], args[11], args[12], args[13], args[14], args[15], args[16], args[17], args[18]);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    System.out.println("v6");
  }
}
