package ivory.core.data.dictionary;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.sux4j.mph.MinimalPerfectHashFunction;
import ivory.util.RetrievalEnvironment;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

public class TestTermMapping {

  public static void main(String[] args) throws Exception {
    String indexPath = "/Users/jimmy/data/indexes/trec/";

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

    Path termsFilePath = new Path(env.getIndexTermsData());
    Path termIDsFilePath = new Path(env.getIndexTermIdsData());
    Path idToTermFilePath = new Path(env.getIndexTermIdMappingData());

    DefaultFrequencySortedDictionary termIDMap = new DefaultFrequencySortedDictionary(termsFilePath, termIDsFilePath, idToTermFilePath, fs);

    int nTerms = termIDMap.getVocabularySize();
    System.out.println("nTerms: " + nTerms);

    List<CharSequence> terms = Lists.newArrayList();
    for (int i=1; i<100; i++) {
      System.out.println(i + " " + termIDMap.getTerm(i));
      terms.add(termIDMap.getTerm(i));
    }
    MinimalPerfectHashFunction<CharSequence> dictionary =
      new MinimalPerfectHashFunction<CharSequence>(terms,
          TransformationStrategies.prefixFreeIso());

    System.out.println("termid = " + dictionary.getLong("page") + ", " + terms.get(0));
    
//    System.out.println(" \"term word\" to lookup termid; \"termid 234\" to lookup term");
//    String cmd = null;
//    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
//    System.out.print("lookup > ");
//    while ((cmd = stdin.readLine()) != null) {
//
//      String[] tokens = cmd.split("\\s+");
//
//      if (tokens.length != 2) {
//        System.out.println("Error: unrecognized command!");
//        System.out.print("lookup > ");
//
//        continue;
//      }
//
//      if (tokens[0].equals("termid")) {
//        int termid;
//        try {
//          termid = Integer.parseInt(tokens[1]);
//        } catch (Exception e) {
//          System.out.println("Error: invalid termid!");
//          System.out.print("lookup > ");
//
//          continue;
//        }
//
//        System.out.println("termid=" + termid + ", term=" + termIDMap.getTerm(termid));
//      } else if (tokens[0].equals("term")) {
//        String term = tokens[1];
//
//        System.out.println("term=" + term + ", termid=" + termIDMap.getID(term));
//        System.out.println("term=" + term + ", termid=" + dictionary.getLong(term));
//      } else {
//        System.out.println("Error: unrecognized command!");
//        System.out.print("lookup > ");
//        continue;
//      }
//
//      System.out.print("lookup > ");
//    }
  }

}
