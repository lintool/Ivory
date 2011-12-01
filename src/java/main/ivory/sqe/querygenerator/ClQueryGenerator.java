package ivory.sqe.querygenerator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class ClQueryGenerator {
	  public static void main(String args[]){
		    String queriesFile = args[0];
		    String grammarDir = args[1];
//		    String eVocabSrcPath = args[2];
//		    String fVocabTrgPath = args[3];
//		    String e2f_ttablePath = args[4];
		    String fVocabSrcPath = args[5];
		    String eVocabTrgPath = args[6];
		    String f2e_ttablePath = args[7];

		    List<String> queries = new ArrayList<String>();
		    Configuration conf = new Configuration();

		    try {
		      BufferedWriter w1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("titles-word.trans"), "UTF-8"));
		      BufferedWriter w2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("titles-phrase.trans"), "UTF-8"));
//		      Vocab eVocabSrc = HadoopAlign.loadVocab(new Path(eVocabSrcPath), conf);
//		      Vocab fVocabTrg = HadoopAlign.loadVocab(new Path(fVocabTrgPath), conf);
//		      TTable_monolithic_IFAs e2fProbs = new TTable_monolithic_IFAs(FileSystem.getLocal(conf), new Path(e2f_ttablePath), true);
		      Vocab fVocabSrc = HadoopAlign.loadVocab(new Path(fVocabSrcPath), conf);
		      Vocab eVocabTrg = HadoopAlign.loadVocab(new Path(eVocabTrgPath), conf);
		      TTable_monolithic_IFAs f2eProbs = new TTable_monolithic_IFAs(FileSystem.getLocal(conf), new Path(f2e_ttablePath), true);

		      BufferedReader r = new BufferedReader(new FileReader(queriesFile));

		      String  line = null;
		      while((line = r.readLine())!=null){
		        queries.add(line);
		      }

		      int i=0;
		      for(String query : queries){

		        // this where we determine what type of query -- if there are phrases, they should be identified here.
		    	JSONObject queryJson = new JSONObject();
		    	String[] tokens = query.split(" ");
		    	try {
					queryJson.put("#combine", new JSONArray(tokens));
				} catch (JSONException e) {
					e.printStackTrace();
				}
		    	
		        ////// phrases

		        //				Query q = new PhraseQuery(grammarDir+"/titles_en.grammar."+i+".0");
		        //        parseQuery(q, query, eVocabSrc, eVocabTrg, fVocabSrc, fVocabTrg, e2fProbs, f2eProbs);
		        //        QueryTranslation tq = q.translate();


		        //				p2pOutput = applyPhraseTable(terms, phraseTable, src_string2TFs, true, isCovered);

		  
		        System.out.println(queryJson.toString());
		        i++;
		      }
		      w1.close();
		      w2.close();
		    } catch (IOException e) {
		      e.printStackTrace();
		    }
		  }

}
